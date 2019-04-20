//******************************
//Generating pieces for the TPCC workload
//
//Funcs:
//- input generators and transaction base generators
//******************************

use super::entry::*;
use super::numeric::*;
use super::table::*;
use super::tpcc_tables::*;
use super::workload_common::*;

use std::{any::Any, str, sync::Arc, time};

use rand::rngs::SmallRng;

use pnvm_lib::parnvm::nvm_txn_raw::*;
use pnvm_lib::parnvm::piece::*;
use pnvm_lib::txn::*;

pub trait TPCCInput {
    fn get_input(&self) -> &Any;
}

pub struct NewOrderInput {
    w_id_: i32,
    d_id_: i32,
    c_id_: i32,
    ol_cnt_: i32,
    qty_: [i32; 15],
    itemid_: [i32; 15],
    supware_: [i32; 15],
    now_: i32,
}

impl TPCCInput for NewOrderInput {
    fn get_input(&self) -> &Any {
        self
    }
}

pub fn pc_new_order_input(w_home: i32, rng: &mut SmallRng) -> NewOrderInput {
    let num_wh = num_warehouse_get();
    let num_dis = num_district_get();

    let now = time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i32;
    //let w_id = urand(1, num_wh, rng);
    let w_id = w_home;
    let d_id = urand(1, num_dis, rng);
    let c_id = nurand(1023, 1, 3000, rng);
    let ol_cnt = urand(5, 15, rng);

    let mut supware = [0 as i32; 15];
    let mut itemid = [0 as i32; 15];
    let mut qty = [0 as i32; 15];

    for i in 0..ol_cnt as usize {
        supware[i] = if urand(1, 100, rng) > 1 {
            w_id
        } else {
            urandexcept(1, num_wh, w_id, rng)
        };
        itemid[i] = nurand(8191, 1, 100000, rng);
        qty[i] = urand(1, 10, rng);

        #[cfg(feature = "noconflict")]
        {
            supware[i] = w_id;
        }
    }

    NewOrderInput {
        w_id_: w_id,
        d_id_: d_id,
        c_id_: c_id,
        ol_cnt_: ol_cnt,
        supware_: supware,
        itemid_: itemid,
        qty_: qty,
        now_: now,
    }
}

pub fn do_pc_new_order(
    tables: &Arc<Tables>,
    tx: &mut TransactionParOCCRaw,
    input: NewOrderInput,
) -> bool {
    /* Prelimary */
    tx.activate_txn();

    let dis_num = num_district_get();
    let w_id = input.w_id_;
    let d_id = input.d_id_;
    let ol_cnt = input.ol_cnt_;
    let now = input.now_;
    let c_id = input.c_id_;
    let src_whs = input.supware_.clone();
    let item_ids = input.itemid_.clone();
    let qty = input.qty_.clone();

    tx.wait_deps_start(1);
    tx.update_rank(1);
    /* Read & Write District */
    let mut o_id;
    let mut d_tax;
    while {
        let district_ref = tables
            .district
            .retrieve(&(w_id, d_id), (w_id * dis_num + d_id) as usize)
            .unwrap()
            .into_table_ref(None, None);
        //println!("READ : DISTRICT : {:?}", district_ref.get_id());
        let mut district = tx.read::<District>(district_ref.box_clone()).clone();

        o_id = district.d_next_o_id;
        d_tax = district.d_tax;
        district.d_next_o_id = o_id + 1;
        //tx.write(district_ref, district);
        tx.write_field(district_ref, district, vec![D_NEXT_O_ID]);

        let res = tx.try_commit_piece();
        !res
    } {}

    /* Read Warehouse */
    tx.wait_deps_start(2);
    tx.update_rank(2);

    let mut w_tax;
    while {
        let warehouse_ref = tables
            .warehouse
            .retrieve(&w_id, w_id as usize)
            .unwrap()
            .into_table_ref(None, None);
        //println!("READ : WAREHOUSE : {:?}", warehouse_ref.get_id());
        w_tax = tx.read::<Warehouse>(warehouse_ref).w_tax;

        let res = tx.try_commit_piece();
        !res
    } {}

    /* Insert NewOrder */
    tx.wait_deps_start(3);
    tx.update_rank(3);

    while {
        tables.neworder.push_pc_raw(
            tx,
            NewOrder {
                no_o_id: o_id,
                no_d_id: d_id,
                no_w_id: w_id,
            },
            &tables,
        );

        !tx.try_commit_piece()
    } {}

    /* Item LOOP */
    tx.wait_deps_start(4);
    tx.update_rank(4);

    let mut i_price_arr = Vec::with_capacity(ol_cnt as usize);
    while {
        for i in 0..ol_cnt as usize {
            let id = item_ids[i];
            let item_arc = tables.item.retrieve(&id, id as usize).unwrap();
            let item_ref = item_arc.into_table_ref(None, None);
            //println!("READ : ITEM : {:?}", item_ref.get_id());
            let i_price = tx.read::<Item>(item_ref).i_price;
            i_price_arr.push(i_price);
        }

        !tx.try_commit_piece()
    } {}

    /* Read Customer and Insert Order */
    tx.wait_deps_start(5);
    tx.update_rank(5);
    while {
        let tid = tx.id().clone();

        let customer_ref = tables
            .customer
            .retrieve(&(w_id, d_id, c_id))
            .unwrap()
            .into_table_ref(None, None);
        let c_discount = tx.read::<Customer>(customer_ref).c_discount;

        let mut all_local: i64 = 1;
        for i in 0..ol_cnt as usize {
            if w_id != src_whs[i] {
                all_local = 0;

                #[cfg(feature = "noconflict")]
                panic!("no conflict!");
            }
        }

        info!(
            "[{:?}][TXN-NEWORDER] Push ORDER {:?}, [w_id:{}, d_id:{}, o_id: {}, c_id: {}, cnt {}]",
            tid, o_id, w_id, d_id, o_id, c_id, ol_cnt
        );
        tables.order.push_pc_raw(
            tx,
            Order {
                o_id: o_id,
                o_d_id: d_id,
                o_w_id: w_id,
                o_c_id: c_id,
                o_entry_d: now,
                o_carrier_id: 0,
                o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                o_all_local: Numeric::new(all_local, 1, 0),
            },
            &tables,
        );

        for i in 0..ol_cnt as usize {
            let stock_ref = tables
                .stock
                .retrieve(&(src_whs[i], item_ids[i]), src_whs[i] as usize)
                .unwrap()
                .into_table_ref(None, None);
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
                10 => stock.s_dist_10.clone(),
                _ => panic!("invalid d_id: {}", d_id),
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
            //tx.write(stock_ref, stock);
            tx.write_field(
                stock_ref,
                stock,
                vec![S_QUANTITY, S_ORDER_CNT, S_REMOTE_CNT],
            );

            let ol_amount = qty
                * i_price_arr[i]
                * (Numeric::new(1, 1, 0) + w_tax + d_tax)
                * (Numeric::new(1, 1, 0) - c_discount);

            //println!("{}", s_dist);
            info!("[{:?}][TXN-NEWORDER] PUSHING ORDERLINE  (w_id:{:?}, d_id:{}, o_id: {}, ol_cnt: {})", tid, w_id, d_id, o_id, i+1);
            tables.orderline.push_pc_raw(
                tx,
                OrderLine {
                    ol_o_id: o_id,
                    ol_d_id: d_id,
                    ol_w_id: w_id,
                    ol_number: i as i32 + 1,
                    ol_i_id: item_ids[i],
                    ol_supply_w_id: src_whs[i],
                    ol_delivery_d: 0,
                    ol_quantity: qty,
                    ol_amount: ol_amount,
                    ol_dist_info: s_dist,
                },
                &tables,
            );
        }

        !tx.try_commit_piece()
    } {}

    true
}

/*   ********************************
 *   Stock Level
 *   ********************************/

pub fn do_pc_stocklevel(
    tables: &Arc<Tables>,
    w_id: i32,
    d_id: i32,
    thd: &Numeric,
    tx: &mut TransactionParOCCRaw,
) -> bool {
    /* Prelimary */
    tx.activate_txn();
    let dis_num = num_district_get();

    /* R District */
    tx.wait_deps_start(1);
    tx.update_rank(1);

    let mut d_next_o_id;
    while {
        let tid = tx.id().clone();
        let d_row = tables
            .district
            .retrieve(&(w_id, d_id), (w_id * dis_num + d_id) as usize)
            .unwrap()
            .into_table_ref(None, None);
        let d = tx.read::<District>(d_row).clone();
        d_next_o_id = d.d_next_o_id;
        info!(
            "[{:?}][STOCK-LEVEL] GETTING NEXT_O_ID [W_D: {}-{}, NEXT_O_ID: {}]",
            tid, w_id, d_id, d_next_o_id
        );

        !tx.try_commit_piece()
    } {}

    /* R Ol Stock */
    tx.wait_deps_start(5);
    tx.update_rank(5);
    while {
        let tid = tx.id().clone();

        let ol_arcs = tables
            .orderline
            .find_range(w_id, d_id, d_next_o_id - 20, d_next_o_id);

        let mut ol_i_ids = vec![];
        for ol_arc in ol_arcs {
            let ol_row = ol_arc.into_table_ref(None, None);
            let ol = tx.read::<OrderLine>(ol_row);
            ol_i_ids.push(ol.ol_i_id);
            info!(
                "[{:?}][STOCK-LEVEL] RECENT ORDER LINE [W_D: {}-{}, OL_I_ID: {}]",
                tid, w_id, d_id, ol.ol_i_id
            );
        }

        let mut low_stock = 0;
        for ol_i_id in ol_i_ids.into_iter() {
            let stock_row = tables
                .stock
                .retrieve(&(w_id, ol_i_id), w_id as usize)
                .expect("no stock")
                .into_table_ref(None, None);

            let stock = tx.read::<Stock>(stock_row);
            info!(
                "[{:?}][STOCK-LEVEL] STOCK LEVEL CHECK [W_ID:{}, ol_i_id: {}, stock_level: {:?}]",
                tid, w_id, ol_i_id, stock.s_quantity
            );
            if stock.s_quantity < thd.clone() {
                low_stock += 1;
            }
        }

        !tx.try_commit_piece()
    } {}

    true
}

/*   ********************************
 *   Delivery
 *   ********************************/

/* Not used */
pub struct DeliveryInput {
    delivery: i32,
}

pub fn pc_delivery_input(_w_id: i32, rng: &mut SmallRng) -> DeliveryInput {
    DeliveryInput { delivery: 0 }
}

pub fn do_pc_delivery(
    tables: &Arc<Tables>,
    w_id: i32,
    o_carrier_id: i32,
    tx: &mut TransactionParOCCRaw,
) -> bool {
    tx.activate_txn();
    let num_dis = num_district_get();

    /* NewOrder transaction */
    tx.wait_deps_start(3);
    tx.update_rank(3);

    let tid = tx.id().clone();
    let mut no_o_id_arr = Vec::with_capacity(num_dis as usize);
    while {
        for d_id in 1..=num_dis {
            let no_arc = tables.neworder.retrieve_min_oid(&(w_id, d_id));
            if no_arc.is_some() {
                let no_row = no_arc.unwrap().into_table_ref(None, None);
                let no_o_id = tx.read::<NewOrder>(no_row).no_o_id;
                //TODO:
                info!(
                    "[{:?}][DELIVERY] DELETING NEWORDER [W_ID: {}, D_ID: {}, O_ID: {}]",
                    tid, w_id, d_id, no_o_id
                );
                if !tables
                    .neworder
                    .delete_pc_raw(tx, &(w_id, d_id, no_o_id), &tables)
                {
                    return false;
                }

                no_o_id_arr.push(Some(no_o_id));
            } else {
                no_o_id_arr.push(None);
            }
        }

        !tx.try_commit_piece()
    } {}

    tx.wait_deps_start(5);
    tx.update_rank(5);

    while {
        for d_id in 1..=num_dis {
            let i: usize = d_id as usize - 1;
            match no_o_id_arr[i] {
                Some(no_o_id) => {
                    info!(
                        "[{:?}][DELIVERY] RETRIEVING ORDER  [W_ID: {}, D_ID: {}, O_ID: {}]",
                        tid, w_id, d_id, no_o_id
                    );
                    let o_row = tables
                        .order
                        .retrieve(&(w_id, d_id, no_o_id))
                        .expect("order empty")
                        .into_table_ref(None, None);
                    let mut o = tx.read::<Order>(o_row.box_clone()).clone();
                    let o_id = o.o_id;
                    let o_c_id = o.o_c_id;

                    o.o_carrier_id = o_carrier_id;
                    //tx.write(o_row, o);
                    tx.write_field(o_row, o, vec![O_CARRIER_ID]);

                    let ol_arcs = tables.orderline.find_by_oid(&(w_id, d_id, o_id));
                    let now = gen_now();
                    let mut ol_amount_sum = Numeric::new(0, 6, 2);
                    for ol_arc in ol_arcs {
                        let ol_row = ol_arc.into_table_ref(None, None);
                        let mut ol = tx.read::<OrderLine>(ol_row.box_clone()).clone();
                        ol_amount_sum += ol.ol_amount;

                        ol.ol_delivery_d = now;
                        info!(
                            "[{:?}][DELIVERY] UPDATEING ORDERLINE [OL_AMOUNT_SUM: {:?}]",
                            tid, ol_amount_sum
                        );
                        //tx.write(ol_row, ol);
                        tx.write_field(ol_row, ol, vec![OL_DELIVERY_D]);
                    }

                    let c_row = tables
                        .customer
                        .retrieve(&(w_id, d_id, o_c_id))
                        .expect("deliver::customer not empty")
                        .into_table_ref(None, None);
                    let mut c = tx.read::<Customer>(c_row.box_clone()).clone();
                    c.c_balance += ol_amount_sum;
                    c.c_delivery_cnt += Numeric::new(1, 4, 0);

                    info!(
                        "[{:?}][DELIVERY] UPDATEING CUSTOEMR [CID: {}, DELIVERY_CNT: {:?}]",
                        tid, o_c_id, c.c_delivery_cnt
                    );
                    //tx.write(c_row, c);
                    tx.write_field(c_row, c, vec![C_BALANCE, C_DELIVERY_CNT]);
                }
                None => {}
            }
        }

        !tx.try_commit_piece()
    } {}

    true
}

/*   ********************************
 *   OrderStatus
 *   ********************************/

pub struct OrderStatusInput {
    w_id: i32,
    d_id: i32,
    c_last: Option<String>,
    c_id: Option<i32>,
}

pub fn pc_orderstatus_input(w_home: i32, rng: &mut SmallRng) -> OrderStatusInput {
    let d_id = urand(1, 10, rng);
    let w_id = w_home;

    let y = urand(1, 100, rng);
    let (c_last, c_id) = if y <= 60 {
        (Some(rand_last_name(nurand(255, 0, 999, rng), rng)), None)
    } else {
        (None, Some(nurand(1023, 1, 3000, rng)))
    };

    OrderStatusInput {
        d_id: d_id,
        w_id: w_id,
        c_last: c_last,
        c_id: c_id,
    }
}

pub fn do_pc_orderstatus(
    tables: &Arc<Tables>,
    tx: &mut TransactionParOCCRaw,
    input: OrderStatusInput,
) -> bool {
    tx.activate_txn();
    let c_id = input.c_id.as_ref().cloned();
    let c_w_id = input.w_id;
    let c_d_id = input.d_id;
    let d_id = input.d_id;
    let w_id = input.w_id;

    tx.wait_deps_start(5);
    tx.update_rank(5);

    while {
        let c_last = input.c_last.as_ref().cloned();
        let tid = tx.id().clone();
        let c_row = match c_id {
            Some(c_id) => tables
                .customer
                .retrieve(&(c_w_id, c_d_id, c_id))
                .expect("customer by id empty")
                .into_table_ref(None, None),

            None => {
                assert!(c_last.is_some());
                info!("[{:?}][ORDER-STATUS] Getting by Name {:?}", tid, c_last);
                let c_last = c_last.unwrap();
                match tables
                    .customer
                    .find_by_name_id(&(c_last.clone(), c_w_id, c_d_id))
                {
                    None => {
                        warn!("[{:?}][ORDER-STATUS] No found Name {:?}", tid, c_last);
                        return false;
                    }
                    Some(arc) => arc.into_table_ref(None, None),
                }
            }
        };

        let c_id = tx.read::<Customer>(c_row).c_id;
        let o_row = match tables.order.retrieve_by_cid(&(c_w_id, c_d_id, c_id)) {
            None => {
                warn!("retrieve_by_cid:: corrupted");
                return false;
            }
            Some(o_row) => o_row.into_table_ref(None, None),
        };

        let o_id = tx.read::<Order>(o_row).o_id;
        info!(
            "[{:?}][ORDER-STATUS] GET ORDER FROM CUSTOMER [w_d: {}-{}, o_id: {}, c_id: {}]",
            tid, c_w_id, c_d_id, o_id, c_id
        );

        let ol_arcs = tables.orderline.find_by_oid(&(c_w_id, c_d_id, o_id));

        for ol_arc in ol_arcs {
            let ol_row = ol_arc.into_table_ref(None, None);
            let ol = tx.read::<OrderLine>(ol_row);
        }

        !tx.try_commit_piece()
    } {}

    true
}

/*   ********************************
 *   Payment
 *   ********************************/

pub struct PaymentInput {
    w_id: i32,
    d_id: i32,
    c_w_id: i32,
    c_d_id: i32,
    h_amount: Numeric,
    h_date: i32,
    c_last: Option<String>,
    c_id: Option<i32>,
}

pub fn pc_payment_input(w_home: i32, rng: &mut SmallRng) -> PaymentInput {
    let num_wh = num_warehouse_get();
    let num_dis = num_district_get();

    let w_id = w_home;
    let d_id = urand(1, num_dis, rng);

    let x = urand(1, 100, rng);
    let y = urand(1, 100, rng);

    let c_w_id: i32;
    let c_d_id: i32;

    #[cfg(feature = "noconflict")]
    {
        x = 1;
    }

    if num_wh == 1 || x <= 85 {
        //85% paying throuhg won house
        c_w_id = w_id;
        c_d_id = d_id;
    } else {
        //15% paying from remote  warehouse
        c_w_id = urandexcept(1, num_wh, w_id, rng);
        assert!(c_w_id != w_id);
        c_d_id = urand(1, 10, rng);
    }

    let h_amount = rand_numeric(1.00, 5000.00, 10, 2, rng);
    let h_date = gen_now();

    let (c_last, c_id) = if y <= 60 {
        (Some(rand_last_name(nurand(255, 0, 999, rng), rng)), None)
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

pub fn do_pc_payment(
    tables: &Arc<Tables>,
    input: PaymentInput,
    tx: &mut TransactionParOCCRaw,
) -> bool {
    tx.activate_txn();

    let dis_num = num_district_get();
    let w_id = input.w_id;
    let h_amount = input.h_amount;
    let d_id = input.d_id;
    let c_id = input.c_id.as_ref().cloned();
    let c_w_id = input.c_w_id;
    let c_d_id = input.c_d_id;
    let h_date = input.h_date;

    /* RW District */

    tx.wait_deps_start(1);
    tx.update_rank(1);

    let mut d_name;
    while {
        let tid = tx.id().clone();

        let district_row = tables
            .district
            .retrieve(&(w_id, d_id), (w_id * dis_num + d_id) as usize)
            .expect("district empty")
            .into_table_ref(None, None);
        let mut district = tx.read::<District>(district_row.box_clone()).clone();
        d_name = district.d_name.clone();
        district.d_ytd = district.d_ytd + h_amount;
        info!(
            "[{:?}][TXN-PAYMENT] Update District::YTD\t  {:?}",
            tid, district.d_ytd
        );
        tx.write_field(district_row, district, vec![D_YTD]);

        !tx.try_commit_piece()
    } {}

    /* RW Warehouse */

    tx.wait_deps_start(2);
    tx.update_rank(2);

    let mut w_name;
    while {
        let tid = tx.id().clone();
        let warehouse_row = tables
            .warehouse
            .retrieve(&w_id, w_id as usize)
            .expect("warehouse empty")
            .into_table_ref(None, None);
        let mut warehouse = tx.read::<Warehouse>(warehouse_row.box_clone()).clone();
        w_name = warehouse.w_name.clone();
        warehouse.w_ytd = warehouse.w_ytd + h_amount;
        info!(
            "[{:?}][TXN-PAYMENT] Update Warehouse::YTD {:?}",
            tid, warehouse.w_ytd
        );
        tx.write_field(warehouse_row, warehouse, vec![W_YTD]);

        !tx.try_commit_piece()
    } {}

    /* RW Customer */
    tx.wait_deps_start(5);
    tx.update_rank(5);
    let mut c_id_val;
    while {
        let tid = tx.id().clone();
        let c_last = input.c_last.as_ref().cloned();

        let c_row = match c_id {
            Some(c_id) => {
                /* Case 1 , by C_ID*/
                info!("[{:?}][TXN-PAYMENT] Getting by id {:?}", tid, c_id);
                tables
                    .customer
                    .retrieve(&(c_w_id, c_d_id, c_id))
                    .expect("customer by id empty")
                    .into_table_ref(None, None)
            }
            None => {
                assert!(c_last.is_some());
                info!("[{:?}][TXN-PAYMENT] Getting by Name {:?}", tid, c_last);
                let c_last = c_last.unwrap();
                match tables
                    .customer
                    .find_by_name_id(&(c_last.clone(), c_w_id, c_d_id))
                {
                    None => {
                        warn!("[{:?}][TXN-PAYMENT] No found Name {:?}", tid, c_last);
                        return false;
                    }
                    Some(arc) => arc.into_table_ref(None, None),
                }
            }
        };

        let mut c = tx.read::<Customer>(c_row.box_clone()).clone();
        info!("[{:?}][TXN-PAYMENT] Read Customer\n\t  {:?}", tid, c);
        let mut c_fields = vec![C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT];
        c.c_balance -= h_amount;
        c.c_ytd_payment += h_amount;
        c.c_payment_cnt += Numeric::new(1, 4, 0);
        c_id_val = c.c_id;
        let c_credit = c.c_credit.clone();
        let c_credit = str::from_utf8(&c_credit).unwrap();
        match c_credit {
            "BC" => {
                let new_data_str = format!(
                    "|{},{},{},{},{},{}|",
                    c.c_id,
                    c.c_d_id,
                    c.c_w_id,
                    d_id,
                    w_id,
                    h_amount.as_string()
                );
                let len = new_data_str.len();
                let new_data = new_data_str.as_bytes();
                c.c_data.rotate_right(len);
                c.c_data[0..len].copy_from_slice(new_data);
                c_fields.push(C_DATA);
            }
            _ => {}
        }
        info!("[{:?}][TXN-PAYMENT] Updating Customer\n\t  {:?}", tid, c);
        //tx.write(c_row, c);
        tx.write_field(c_row, c, c_fields);

        !tx.try_commit_piece()
    } {}

    /* W History */
    tx.wait_deps_start(6);
    tx.update_rank(6);

    while {
        let tid = tx.id().clone();
        let h_data = format!(
            "{}    {}",
            str::from_utf8(&w_name).unwrap(),
            str::from_utf8(&d_name).unwrap()
        );
        info!(
            "[{:?}][TXN-PAYMENT] Inserting History::HDATA\t  {:?}",
            tid, h_data
        );
        tables.history.push_pc_raw(
            tx,
            History::new(
                c_id_val, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data,
            ),
            &tables,
        );

        !tx.try_commit_piece()
    } {}

    true
}
