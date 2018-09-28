


use super::workload::*;
use super::numeric::*;
use super::entry::*;
use super::table::*;

use std::{
    time,
    sync::{Arc},


};

use rand::rngs::SmallRng;

use pnvm_lib::parnvm::nvm_txn::*;
use pnvm_lib::parnvm::piece::*;
use pnvm_lib::txn::*;

pub fn pc_new_order_random(tables: &Arc<Tables>, w_home: i32, rng: &mut SmallRng)
    -> TransactionParBaseOCC
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

    pc_new_order(tables, w_id, d_id, c_id, ol_cnt, &supware, &itemid, &qty, now)
    }


    fn pc_new_order(
        _tables: &TablesRef,
        w_id: i32,
        d_id: i32,
        c_id: i32,
        ol_cnt: i32,
        src_whs : &[i32],
        item_ids: &[i32],
        qty: &[i32],
        now: i32)
        -> TransactionParBaseOCC
        { 
            let wh_num = num_warehouse_get();

            /* Read & Write District */
            let tables = _tables.clone();
            let new_order_1 = move |tx: &mut TransactionParOCC| {
                let district_ref = tables.district.retrieve(&(w_id, d_id), (w_id * wh_num + d_id) as usize).unwrap().into_table_ref(None, None);
                //println!("READ : DISTRICT : {:?}", district_ref.get_id());
                let mut district = tx.read::<District>(district_ref.box_clone()).clone();

                let o_id :i32 = district.d_next_o_id;
                let d_tax :Numeric = district.d_tax;
                district.d_next_o_id = o_id +1;
                tx.write(district_ref, district);

                assert_eq!(tx.add_output(Box::new(o_id)), 0);
                assert_eq!(tx.add_output(Box::new(d_tax)), 1);
            };

            /* Read Warehouse */
            let new_order_2 = move |tx: &mut TransactionParOCC| {
                let warehouse_ref = tables.warehouse.retrieve(&w_id, w_id as usize)
                    .unwrap().into_table_ref(None, None);
                //println!("READ : WAREHOUSE : {:?}", warehouse_ref.get_id());
                let w_tax :Numeric = tx.read::<Warehouse>(warehouse_ref).w_tax;

                assert_eq!(tx.add_output(Box::new(w_tax)), 2);
            };


            /* Insert NewOrder */
            let new_order_3 = move |tx: &mut TransactionParOCC| {
                let o_id =  tx.get_output::<i32>(0).clone();
                tables.neworder.push(tx,
                                     NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id },
                                     &tables);

            };


            /* Read Customer and Insert Order */
            let new_order_4 = move |tx: &mut TransactionParOCC| {
                let tid = tx.id();
                let o_id =tx.get_output::<i32>(0);

                let customer_ref = tables.customer.retrieve(&(w_id, d_id, c_id))
                    .unwrap().into_table_ref(None, None);
                let c_discount = tx.read::<Customer>(customer_ref).c_discount;

                assert_eq!(tx.add_output(Box::new(c_discount)), 3);

                let mut all_local :i64 = 1;
                for i in 0..ol_cnt as usize {
                    if w_id != src_whs[i] {
                        all_local = 0;
                    }
                }

                info!("[{:?}][TXN-NEWORDER] Push ORDER {:?}, [w_id:{}, d_id:{}, o_id: {}, c_id: {}, cnt {}]", tid, o_id, w_id, d_id, o_id, c_id, ol_cnt);
                tables.order.push(tx,
                                  Order {
                                      o_id: *o_id, o_d_id: d_id, o_w_id: w_id, 
                                      o_c_id: c_id, o_entry_d: now,
                                      o_carrier_id: 0, 
                                      o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                                      o_all_local: Numeric::new(all_local, 1, 0)
                                  },
                                  &tables);
            };



            /* Item LOOP */
            let new_order_5 = move |tx: &mut TransactionParOCC| {
                let mut i_price_arr = Vec::with_capacity(ol_cnt as usize);
                for i in 0..ol_cnt as usize {
                    let id = item_ids[i];
                    let item_arc = tables.item.retrieve(&id, id as usize ).unwrap();
                    let item_ref = item_arc.into_table_ref(None, None);
                    //println!("READ : ITEM : {:?}", item_ref.get_id());
                    let i_price = tx.read::<Item>(item_ref).i_price;
                    i_price_arr.push(i_price);
                }
                assert_eq!(tx.add_output(Box::new(i_price_arr)),4);
            };



            /* Stock LOOP  & OrderLine LOOP*/
            let new_order_6= move |tx: &mut TransactionParOCC| {
                let tid = tx.id();
                let w_tax =  tx.get_output::<Numeric>(2).clone();
                let d_tax = tx.get_output::<Numeric>(1).clone();
                let i_price_arr = tx.get_output::<Vec<Numeric>>(4).clone();
                let c_discount = tx.get_output::<Numeric>(3).clone();
                let o_id =tx.get_output::<i32>(0);

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
                    tables.orderline.push(tx, 
                                          OrderLine {
                                              ol_o_id: *o_id, ol_d_id: d_id, ol_w_id: w_id, 
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
