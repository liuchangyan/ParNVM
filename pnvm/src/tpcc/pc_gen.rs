


use super::workload::*;
use super::numeric::*;
use super::entry::*;
use super::table::*;

use std::{
    time,
    sync::{Arc},
    any::Any,
    str,
};

use rand::rngs::SmallRng;

use pnvm_lib::parnvm::nvm_txn::*;
use pnvm_lib::parnvm::piece::*;
use pnvm_lib::txn::*;

pub trait TPCCInput {
    fn get_input(&self) -> &Any;
}

pub struct NewOrderInput {
    w_id_ : i32,
    d_id_: i32,
    c_id_ : i32,
    ol_cnt_ : i32,
    qty_ : [i32;15],
    itemid_ : [i32;15],
    supware_ : [i32;15], 
    now_: i32, 
}

impl TPCCInput for NewOrderInput {
    fn get_input(&self) -> &Any {
        self
    }
}

pub fn pc_new_order_input(w_home: i32, rng: &mut SmallRng) 
    -> NewOrderInput
{
    let NUM_WAREHOUSES = num_warehouse_get();
    let NUM_DISTRICT = num_district_get();


    let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32;     
    //let w_id = urand(1, NUM_WAREHOUSES, rng);
    let w_id = w_home;
    let d_id = urand(1, NUM_DISTRICT, rng);
    let c_id = nurand(1023, 1, 3000, rng);
    let ol_cnt = urand(5, 15, rng);

    let mut supware = [0 as i32;15];
    let mut itemid = [0 as i32;15];
    let mut qty = [0 as i32;15];

    for i in 0..ol_cnt as usize {
        //supware[i] = if urand(1, 100, rng) > 1 {
        supware[i] = if true {
            w_id
        } else {
            urandexcept(1, NUM_WAREHOUSES, w_id, rng)
        };
        itemid[i] = nurand(8191, 1, 100000, rng);
        qty[i] = urand(1, 10, rng);
    }

    NewOrderInput {
        w_id_: w_id,
        d_id_: d_id, 
        c_id_ : c_id,
        ol_cnt_ : ol_cnt,
        supware_ : supware,
        itemid_ : itemid,
        qty_ : qty,
        now_ : now,
    }
    }

    pub fn pc_new_order_base(_tables: &Arc<Tables>) -> TransactionParBaseOCC
    { 
        let wh_num = num_warehouse_get();

        /* Read & Write District */
        let tables = _tables.clone();
        let new_order_1 = move |tx: &mut TransactionParOCC| {
            let (w_id, d_id) = {
                let input = tx.get_input::<NewOrderInput>();
                let w_id = input.w_id_;
                let d_id = input.d_id_;
                (w_id, d_id)
            };

            let district_ref = tables.district.retrieve(&(w_id, d_id), (w_id * wh_num + d_id) as usize).unwrap().into_table_ref(None, None);
            //println!("READ : DISTRICT : {:?}", district_ref.get_id());
            let mut district = tx.read::<District>(district_ref.box_clone()).clone();

            let o_id :i32 = district.d_next_o_id;
            let d_tax :Numeric = district.d_tax;
            district.d_next_o_id = o_id +1;
            tx.write(district_ref, district);

            tx.add_output(Box::new(o_id), 0);
            tx.add_output(Box::new(d_tax), 1);
        };

        /* Read Warehouse */
        let tables = _tables.clone();
        let new_order_2 = move |tx: &mut TransactionParOCC| {
            let (w_id, d_id) = {
                let input = tx.get_input::<NewOrderInput>();
                let w_id = input.w_id_;
                let d_id = input.d_id_;
                (w_id, d_id)
            };

            let warehouse_ref = tables.warehouse.retrieve(&w_id, w_id as usize)
                .unwrap().into_table_ref(None, None);
            //println!("READ : WAREHOUSE : {:?}", warehouse_ref.get_id());
            let w_tax :Numeric = tx.read::<Warehouse>(warehouse_ref).w_tax;

            tx.add_output(Box::new(w_tax), 2);
        };


        /* Insert NewOrder */
        let tables = _tables.clone();
        let new_order_3 = move |tx: &mut TransactionParOCC| {
            let (w_id, d_id) = {
                let input = tx.get_input::<NewOrderInput>();
                let w_id = input.w_id_;
                let d_id = input.d_id_;
                (w_id, d_id)
            };

            let o_id =  tx.get_output::<i32>(0).clone();
            tables.neworder.push_pc(tx,
                                    NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id },
                                    &tables);

        };


        /* Read Customer and Insert Order */
        let tables = _tables.clone();
        let new_order_4 = move |tx: &mut TransactionParOCC| {
            let (w_id, d_id, ol_cnt, now, c_id, src_whs) = {
                let input = tx.get_input::<NewOrderInput>();
                let w_id = input.w_id_;
                let d_id = input.d_id_;
                let ol_cnt = input.ol_cnt_;
                let now = input.now_;
                let c_id = input.c_id_;
                let src_whs = input.supware_.clone();
                (w_id, d_id, ol_cnt, now, c_id, src_whs)
            };


            let tid = tx.id().clone();
            let o_id =tx.get_output::<i32>(0).clone();

            let customer_ref = tables.customer.retrieve(&(w_id, d_id, c_id))
                .unwrap().into_table_ref(None, None);
            let c_discount = tx.read::<Customer>(customer_ref).c_discount;

            tx.add_output(Box::new(c_discount), 3);

            let mut all_local :i64 = 1;
            for i in 0..ol_cnt as usize {
                if w_id != src_whs[i] {
                    all_local = 0;
                }
            }

            info!("[{:?}][TXN-NEWORDER] Push ORDER {:?}, [w_id:{}, d_id:{}, o_id: {}, c_id: {}, cnt {}]", tid, o_id, w_id, d_id, o_id, c_id, ol_cnt);
            tables.order.push_pc(tx,
                                 Order {
                                     o_id: o_id, o_d_id: d_id, o_w_id: w_id, 
                                     o_c_id: c_id, o_entry_d: now,
                                     o_carrier_id: 0, 
                                     o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                                     o_all_local: Numeric::new(all_local, 1, 0)
                                 },
                                 &tables);
        };



        /* Item LOOP */
        let tables = _tables.clone();
        let new_order_5 = move |tx: &mut TransactionParOCC| {
            let (ol_cnt, item_ids) = {
                let input = tx.get_input::<NewOrderInput>();
                let ol_cnt = input.ol_cnt_;
                let item_ids = input.itemid_.clone();
                ( ol_cnt, item_ids)
            };


            let mut i_price_arr = Vec::with_capacity(ol_cnt as usize);
            for i in 0..ol_cnt as usize {
                let id = item_ids[i];
                let item_arc = tables.item.retrieve(&id, id as usize ).unwrap();
                let item_ref = item_arc.into_table_ref(None, None);
                //println!("READ : ITEM : {:?}", item_ref.get_id());
                let i_price = tx.read::<Item>(item_ref).i_price;
                i_price_arr.push(i_price);
            }
            tx.add_output(Box::new(i_price_arr), 4);
        };



        /* Stock LOOP  & OrderLine LOOP*/
        let tables = _tables.clone();
        let new_order_6= move |tx: &mut TransactionParOCC| {
            let ( ol_cnt, w_id, d_id, item_ids, src_whs, qty) = {
                let input = tx.get_input::<NewOrderInput>();
                let ol_cnt = input.ol_cnt_;
                let w_id = input.w_id_;
                let d_id = input.d_id_;
                let item_ids = input.itemid_.clone();
                let src_whs = input.supware_.clone();
                let qty = input.qty_.clone();
                ( ol_cnt, w_id, d_id, item_ids, src_whs, qty)
            };

            let tid = tx.id().clone();
            let w_tax =  tx.get_output::<Numeric>(2).clone();
            let d_tax = tx.get_output::<Numeric>(1).clone();
            let i_price_arr = tx.get_output::<Vec<Numeric>>(4).clone();
            let c_discount = tx.get_output::<Numeric>(3).clone();
            let o_id =tx.get_output::<i32>(0).clone();

            for i in 0..ol_cnt as usize {
                let stock_ref = tables.stock.retrieve(&(src_whs[i], item_ids[i]), (src_whs[i] as usize)).unwrap().into_table_ref(None, None);
                let mut stock = tx.read::<Stock>(stock_ref.box_clone()).clone();
                let s_quantity = stock.s_quantity;
                let s_remote_cnt = stock.s_remote_cnt;
                let s_order_cnt = stock.s_order_cnt;
                let s_dist = match d_id {
                    1 => stock.s_dist_01.clone(),
                    2 => stock.s_dist_02.clone(),
                    3 => stock.s_dist_03.clone(),
                    4 => stock.s_dist_04.clone(),
                    5 => stock.s_dist_05.clone(),
                    6 => stock.s_dist_06.clone(),
                    7 => stock.s_dist_07.clone(),
                    8 => stock.s_dist_08.clone(),
                    9 => stock.s_dist_09.clone(),
                    10=> stock.s_dist_10.clone(),
                    _ => panic!("invalid d_id: {}", d_id)
                };

                let qty = Numeric::new(qty[i] as i64, 4, 0);
                stock.s_quantity = if s_quantity > qty {
                    stock.s_quantity - qty
                } else {
                    stock.s_quantity + Numeric::new(91, 4, 0) - qty
                };

                if src_whs[i] != w_id {
                    stock.s_remote_cnt = stock.s_remote_cnt + s_remote_cnt;
                } else {
                    stock.s_order_cnt = s_order_cnt + Numeric::new(1, 4, 0);
                }
                info!("[{:?}][TXN-NEWORDER] Update STOCK \n\t {:?}", tid, stock);
                tx.write(stock_ref, stock);

                let ol_amount = qty * i_price_arr[i] * (Numeric::new(1, 1, 0) + w_tax + d_tax) *
                    (Numeric::new(1, 1, 0) - c_discount);

                //println!("{}", s_dist);
                info!("[{:?}][TXN-NEWORDER] PUSHING ORDERLINE  (w_id:{:?}, d_id:{}, o_id: {}, ol_cnt: {})", tid, w_id, d_id, o_id, i+1);
                tables.orderline.push_pc(tx, 
                                         OrderLine {
                                             ol_o_id: o_id, ol_d_id: d_id, ol_w_id: w_id, 
                                             ol_number: i as i32 + 1, ol_i_id: item_ids[i],
                                             ol_supply_w_id: src_whs[i], ol_delivery_d: 0,
                                             ol_quantity: qty, ol_amount: ol_amount,
                                             ol_dist_info: s_dist
                                         },
                                         &tables);
            }

        };

        let p1 = PieceOCC::new(
            Pid::new(0),
            String::from("neworder"),
            Arc::new(Box::new(new_order_1)),
            "neworder-0-cb",
            1);

        let p2 = PieceOCC::new(
            Pid::new(1),
            String::from("neworder"),
            Arc::new(Box::new(new_order_2)),
            "neworder-1-cb",
            1);

        let p3 = PieceOCC::new(
            Pid::new(2),
            String::from("neworder"),
            Arc::new(Box::new(new_order_3)),
            "neworder-2-cb",
            2);

        let p4 = PieceOCC::new(
            Pid::new(3),
            String::from("neworder"),
            Arc::new(Box::new(new_order_4)),
            "neworder-3-cb",
            3);


        let p5 = PieceOCC::new(
            Pid::new(4),
            String::from("neworder"),
            Arc::new(Box::new(new_order_5)),
            "neworder-4-cb",
            4);

        let p6 = PieceOCC::new(
            Pid::new(5),
            String::from("neworder"),
            Arc::new(Box::new(new_order_6)),
            "neworder-5-cb",
            5);

        let pieces = vec![p6, p5, p4, p3, p2, p1];

        TransactionParBaseOCC::new(pieces, String::from("neworder"))

    }


    /*   ********************************
     *   OrderStock 
     *   ********************************/

    pub struct OrderStockInput {
        w_id: i32,
        d_id: i32,
        c_last: Option<String>,
        c_id: Option<i32>,
    }

    pub fn pc_orderstock_input(w_home:i32, rng: &mut SmallRng)
        -> OrderStockInput
        {
            
            let d_id = urand(1, 10, rng);
            let w_id = w_home;

            let y = urand(1, 100, rng);
            let (c_last, c_id ) = if y <= 60 {
                (Some(rand_last_name(nurand(255, 0, 999, rng),rng)), None)
            } else {
                (None, Some(nurand(1023, 1, 3000, rng)))
            };

            OrderStockInput {
                d_id : d_id,
                w_id : w_id, 
                c_last: c_last,
                c_id : c_id, 
            }
        }

    pub fn pc_orderstatus_base(_tables: &Arc<Tables>) 
        -> TransactionParBaseOCC 
        {
           /* Read Cus and Read Order */ 
            let tables = _tables.clone();
            let orderstatus_cus_ord = move | tx: &mut TransactionParOCC |  {
                let (c_id, c_last, c_w_id, c_d_id, d_id, w_id) = {
                    let input = tx.get_input::<OrderStockInput>();
                    let c_id = input.c_id.as_ref().cloned();
                    let c_last = input.c_last.as_ref().cloned();
                    let c_w_id = input.w_id;
                    let c_d_id = input.d_id;
                    let d_id = input.d_id;
                    let w_id = input.w_id;

                    (c_id, c_last, c_w_id, c_d_id, d_id, w_id)
                };

                let tid = tx.id().clone();
                let c_row = match c_id {
                    Some(c_id) => {
                        tables.customer.retrieve(&(c_w_id, c_d_id, c_id))
                            .expect("customer by id empty")
                            .into_table_ref(None, None)
                    },

                    None => {
                        assert!(c_last.is_some());
                        info!("[{:?}][ORDER-STATUS] Getting by Name {:?}", 
                              tid, c_last);
                        let c_last = c_last.unwrap();
                        match tables.customer.find_by_name_id(&(c_last.clone(), c_w_id, c_d_id)) {
                            None=> {
                                warn!("[{:?}][ORDER-STATUS] No found Name {:?}", tid, c_last);
                                tx.should_abort();
                                return;
                            }
                            Some(arc) => arc.into_table_ref(None, None)
                        }

                    }
                };

                let c_id = tx.read::<Customer>(c_row).c_id;
                let o_row = match tables.order.retrieve_by_cid(&(c_w_id, c_d_id, c_id)) {
                    None => {
                        tx.should_abort();
                        warn!("retrieve_by_cid:: corrupted");
                        return;
                    },
                    Some(o_row) => {
                        o_row.into_table_ref(None, None)
                    }
                };

                let o_id = tx.read::<Order>(o_row).o_id;
                info!("[{:?}][ORDER-STATUS] GET ORDER FROM CUSTOMER [w_d: {}-{}, o_id: {}, c_id: {}]", 
                      tid, c_w_id, c_d_id,o_id, c_id);

                tx.add_output(Box::new(o_id), 0);
            };



            /* RW Orderline */
            let tables = _tables.clone();
            let orderstatus_ol = move | tx: &mut TransactionParOCC |  {
                let ( c_w_id, c_d_id) = {
                    let input = tx.get_input::<OrderStockInput>();
                    let c_w_id = input.w_id;
                    let c_d_id = input.d_id;
                    (c_w_id, c_d_id)
                };

                let o_id = *tx.get_output::<i32>(0);
                let ol_arcs = tables.orderline.find_by_oid(&(c_w_id, c_d_id, o_id));

                for ol_arc in ol_arcs {
                    let ol_row = ol_arc.into_table_ref(None, None);
                    let ol = tx.read::<OrderLine>(ol_row);
                }

            };



            let p1 = PieceOCC::new(
                Pid::new(0),
                String::from("orderstaus"),
                Arc::new(Box::new(orderstatus_cus_ord)),
                "orderstauts_cus_ord",
                3);

            let p2 = PieceOCC::new(
                Pid::new(1),
                String::from("orderstatus"),
                Arc::new(Box::new(orderstatus_ol)),
                "orderstatus_ol",
                5);



            let pieces = vec![p2, p1];

            TransactionParBaseOCC::new(pieces, String::from("orderstatus"))
        }



    /*   ********************************
     *   Payment 
     *   ********************************/


    pub struct PaymentInput {
        w_id: i32,
        d_id: i32,
        c_w_id : i32,
        c_d_id : i32,
        h_amount : Numeric,
        h_date: i32,
        c_last: Option<String>,
        c_id: Option<i32>,
    }


    pub fn pc_payment_input(w_home: i32, rng: &mut SmallRng) 
        -> PaymentInput
        {
            let NUM_WAREHOUSES = num_warehouse_get();
            let NUM_DISTRICT = num_district_get();

            let w_id = w_home;
            let d_id = urand(1, NUM_DISTRICT, rng);

            let x = urand(1, 100, rng);
            let y = urand(1, 100, rng);

            let c_w_id : i32;
            let c_d_id : i32;

            if NUM_WAREHOUSES == 1 || x <= 85 {
                //85% paying throuhg won house
                c_w_id = w_id;
                c_d_id = d_id;
            } else {
                //15% paying from remote  warehouse
                c_w_id =  urandexcept(1, NUM_WAREHOUSES, w_id, rng);
                assert!(c_w_id != w_id);
                c_d_id = urand(1, 10, rng);
            }

            let h_amount = rand_numeric(1.00, 5000.00, 10, 2, rng);
            let h_date = gen_now();     

            let (c_last, c_id ) = if y <= 60 {
                (Some(rand_last_name(nurand(255, 0, 999, rng),rng)), None)
            } else {
                (None, Some(nurand(1023, 1, 3000, rng)))
            };

            PaymentInput {
                w_id,
                d_id,
                c_w_id,
                c_d_id,
                h_amount,
                h_date,
                c_last,
                c_id,
            }
        }


    pub fn pc_payment_base(
        _tables: &Arc<Tables>
    ) 
        -> TransactionParBaseOCC 
        {

            let wh_num = num_warehouse_get();

            /* RW Warehouse */ 
            let tables = _tables.clone();
            let payment_wh = move | tx: &mut TransactionParOCC | 
            {
                let (w_id, h_amount) = {
                    let input = tx.get_input::<PaymentInput>();
                    let w_id = input.w_id;
                    let h_amount = input.h_amount;
                    (w_id, h_amount)
                };

                let tid = tx.id().clone();
                let warehouse_row = tables.warehouse.retrieve(&w_id, w_id as usize).expect("warehouse empty").into_table_ref(None, None);
                let mut warehouse = tx.read::<Warehouse>(warehouse_row.box_clone()).clone();
                let w_name = warehouse.w_name.clone();
                warehouse.w_ytd = warehouse.w_ytd +  h_amount;
                info!("[{:?}][TXN-PAYMENT] Update Warehouse::YTD {:?}", tid, warehouse.w_ytd);
                tx.write(warehouse_row, warehouse);
                
                tx.add_output(Box::new(w_name), 0);

            };


            /* RW District */
            let tables = _tables.clone();
            let payment_dis = move | tx: &mut TransactionParOCC | 
            {
                let (w_id, d_id, h_amount) = {
                    let input = tx.get_input::<PaymentInput>();
                    let w_id = input.w_id;
                    let h_amount = input.h_amount;
                    let d_id = input.d_id;
                    (w_id, d_id, h_amount)
                };

                let tid = tx.id().clone();


                let district_row = tables.district.retrieve(&(w_id, d_id),(w_id * wh_num + d_id) as usize ).expect("district empty").into_table_ref(None, None);
                let mut district = tx.read::<District>(district_row.box_clone()).clone();
                let d_name = district.d_name.clone();
                district.d_ytd = district.d_ytd + h_amount;
                info!("[{:?}][TXN-PAYMENT] Update District::YTD\t  {:?}", tid, district.d_ytd);
                tx.write(district_row,district);
                tx.add_output(Box::new(d_name), 1);
            };


            /* RW Customer */
            let tables = _tables.clone();
            let payment_cus = move | tx: &mut TransactionParOCC | 
            {
                let (c_id, c_last, c_w_id, c_d_id, h_amount, d_id, w_id) = {
                    let input = tx.get_input::<PaymentInput>();
                    let c_id = input.c_id.as_ref().cloned();
                    let c_last = input.c_last.as_ref().cloned();
                    let c_w_id = input.c_w_id;
                    let c_d_id = input.c_d_id;
                    let h_amount = input.h_amount;
                    let d_id = input.d_id;
                    let w_id = input.w_id;

                    (c_id, c_last, c_w_id, c_d_id, h_amount, d_id, w_id)
                };

                let tid = tx.id().clone();

                let c_row = match c_id {
                    Some(c_id) => {
                        /* Case 1 , by C_ID*/
                        info!("[{:?}][TXN-PAYMENT] Getting by id {:?}", 
                              tid, c_id);
                        tables.customer.retrieve(&(c_w_id, c_d_id, c_id)).expect("customer by id empty").into_table_ref(None, None)
                    }, 
                    None => {
                        assert!(c_last.is_some());
                        info!("[{:?}][TXN-PAYMENT] Getting by Name {:?}", 
                              tid, c_last);
                        let c_last = c_last.unwrap();
                        match tables.customer.find_by_name_id(&(c_last.clone(), c_w_id, c_d_id)) {
                            None=> {
                                warn!("[{:?}][TXN-PAYMENT] No found Name {:?}", tid, c_last);
                                tx.should_abort();
                                return;
                            }
                            Some(arc) => arc.into_table_ref(None, None)
                        }
                    }
                };

                let mut c = tx.read::<Customer>(c_row.box_clone()).clone();
                info!("[{:?}][TXN-PAYMENT] Read Customer\n\t  {:?}", tid, c);
                c.c_balance -= h_amount;
                c.c_ytd_payment += h_amount;
                c.c_payment_cnt += Numeric::new(1, 4, 0);
                let c_id = c.c_id;
                let c_credit = c.c_credit.clone(); 
                let c_credit = str::from_utf8(&c_credit).unwrap();
                match c_credit {
                    "BC" => {
                        let new_data_str =  format!("|{},{},{},{},{},{}|",
                                                    c.c_id, c.c_d_id, c.c_w_id, d_id, w_id, h_amount.as_string());
                        let len = new_data_str.len();
                        let new_data = new_data_str.as_bytes();
                        c.c_data.rotate_right(len);
                        c.c_data[0..len].copy_from_slice(new_data);
                    },
                    _ => {},
                }
                info!("[{:?}][TXN-PAYMENT] Updating Customer\n\t  {:?}", tid, c);
                tx.write(c_row, c);

                tx.add_output(Box::new(c_id), 2);
            };



            /* W History */
            let tables = _tables.clone();
            let payment_his = move | tx: &mut TransactionParOCC | 
            {
                let (c_w_id, c_d_id, h_amount, d_id, w_id, h_date) = {
                    let input = tx.get_input::<PaymentInput>();
                    let c_w_id = input.c_w_id;
                    let c_d_id = input.c_d_id;
                    let w_id = input.w_id;
                    let d_id = input.d_id;
                    let h_amount = input.h_amount;
                    let h_date = input.h_date;

                    (c_w_id, c_d_id, h_amount, d_id, w_id, h_date)
                };

                let tid = tx.id().clone();
                let w_name = tx.get_output::<[u8;10]>(0).clone();
                let d_name = tx.get_output::<[u8;10]>(1).clone();
                let c_id =  tx.get_output::<i32>(2).clone();

                let h_data = format!("{}    {}", str::from_utf8(&w_name).unwrap(), str::from_utf8(&d_name).unwrap());
                info!("[{:?}][TXN-PAYMENT] Inserting History::HDATA\t  {:?}", tid, h_data);
                tables.history.push_pc(tx,
                                    History::new(
                                        c_id,
                                        c_d_id,
                                        c_w_id,
                                        d_id,
                                        w_id,
                                        h_date,
                                        h_amount,
                                        h_data,
                                        ),
                                        &tables);


            };

            let p1 = PieceOCC::new(
                Pid::new(0),
                String::from("payment"),
                Arc::new(Box::new(payment_wh)),
                "payment_wh",
                1);

            let p2 = PieceOCC::new(
                Pid::new(1),
                String::from("payment"),
                Arc::new(Box::new(payment_dis)),
                "payment_dis",
                1);

            let p3 = PieceOCC::new(
                Pid::new(2),
                String::from("payment"),
                Arc::new(Box::new(payment_cus)),
                "payment_cus",
                3);

            let p4 = PieceOCC::new(
                Pid::new(3),
                String::from("payment"),
                Arc::new(Box::new(payment_his)),
                "payment_his",
                6);
            let pieces = vec![p4, p3, p2, p1];

            TransactionParBaseOCC::new(pieces, String::from("payment"))

        }

