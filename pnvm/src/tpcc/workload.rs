
use super::{
    table::*,
    entry::*,
    entry_ref::*,
    numeric::*,
};

use pnvm_lib::{
    occ::occ_txn::*,
    txn::*,
};

use std::{
    sync::Arc,
};

use rand;
use num::{
    abs,

};


const NUM_WAREHOUSES : i32 = 5;


fn new_order(tx: &mut TransactionOCC, 
             tables: TablesRef,
             w_id: i32,
             d_id: i32,
             c_id: i32,
             ol_cnt: i32,
             src_whs : &[i32],
             item_ids: &[i32],
             qty: &[i32],
             now: i64)
{
    let w_tax = tx.read::<Warehouse>(tables.warehouse.retrieve(&w_id).unwrap().into_table_ref(None, None, None)).w_tax;

    let c_discount = tx.read::<Customer>(tables.customer.retrieve(&(w_id, d_id, c_id)).unwrap().into_table_ref(None, None, None)).c_discount;
    let district_ref = tables.district.retrieve(&(w_id, d_id)).unwrap().into_table_ref(None, None, None);
    let mut district = tx.read::<District>(district_ref.box_clone()).clone();
    let o_id = district.d_next_o_id;
    let d_tax = district.d_tax;
    district.d_next_o_id = o_id +1;
    tx.write(district_ref, district);

    // let mut all_local :i64 = 1;
    // for i in 0..ol_cnt as usize {
    //     if w_id != src_whs[i] {
    //         all_local = 0;
    //     }
    // }
    //  
    // tables.order.push(tx,
    //                         Order {
    //                             o_id: o_id, o_d_id: d_id, o_w_id: w_id, o_c_id: c_id, o_entry_d: now,
    //                             o_carrier_id: 0, o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
    //                             o_all_local: Numeric::new(all_local, 1, 0)
    //                         });
    // tables.neworder.push(tx,
    //                            NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id });

    // for i in 0..ol_cnt as usize {
    //     let i_price = tables.item.retrieve(item_ids[i]).unwrap().read(tx).i_price;

    //     let stock_ref = tables.stock.retrieve((src_whs[i], item_ids[i])).unwrap();
    //     let stock = stock_ref.read(tx);
    //     let s_quantity = stock.s_quantity;
    //     let s_remote_cnt = stock.s_remote_cnt;
    //     let s_order_cnt = stock.s_order_cnt;
    //     let s_dist = match d_id {
    //         1 => &stock.s_dist_01,
    //         2 => &stock.s_dist_02,
    //         3 => &stock.s_dist_03,
    //         4 => &stock.s_dist_04,
    //         5 => &stock.s_dist_05,
    //         6 => &stock.s_dist_06,
    //         7 => &stock.s_dist_07,
    //         8 => &stock.s_dist_08,
    //         9 => &stock.s_dist_09,
    //         10 => &stock.s_dist_10,
    //         _ => panic!("invalid d_id: {}", d_id)
    //     };

    //     let qty = Numeric::new(qty[i] as i64, 4, 0);
    //     let mut stock_new = stock.clone();
    //     stock_new.s_quantity = if s_quantity > qty {
    //         stock_new.s_quantity - qty
    //     } else {
    //         stock_new.s_quantity + Numeric::new(91, 4, 0) - qty
    //     };

    //     if src_whs[i] != w_id {
    //         stock_new.s_remote_cnt = stock.s_remote_cnt + s_remote_cnt;
    //     } else {
    //         stock_new.s_order_cnt = s_order_cnt + Numeric::new(1, 4, 0);
    //     }

    //     stock_ref.write(tx, stock_new);

    //     let ol_amount = qty * i_price * (Numeric::new(1, 1, 0) + w_tax + d_tax) *
    //         (Numeric::new(1, 1, 0) - c_discount);

    //     tables.orderline.push(tx, OrderLine {
    //         ol_o_id: o_id, ol_d_id: d_id, ol_w_id: w_id, ol_number: i as i32 + 1, ol_i_id: item_ids[i],
    //         ol_supply_w_id: src_whs[i], ol_delivery_d: 0, ol_quantity: qty, ol_amount: ol_amount,
    //         ol_dist_info: s_dist.clone()
    //     })
    // }
}

fn new_order_random<Rng: rand::Rng>(tx: &mut TransactionOCC, tables: Arc<Tables>, now: i64, w_id: i32, rng: &mut Rng) {
    let d_id = urand(1, 1, rng);
    let c_id = nurand(1023, 1, 3000, rng);
    let ol_cnt = urand(5, 15, rng);

    let mut supware = [0 as i32;15];
    let mut itemid = [0 as i32;15];
    let mut qty = [0 as i32;15];

    for i in 0..ol_cnt as usize {
        supware[i] = if urand(1, 100, rng) > 1 {
            w_id
        } else {
            urandexcept(1, NUM_WAREHOUSES, w_id, rng)
        };
        itemid[i] = nurand(8191, 1, 100000, rng);
        qty[i] = urand(1, 10, rng);
    }

    new_order(tx, tables, w_id, d_id, c_id, ol_cnt, &supware, &itemid, &qty, now)
}


fn urand<Rng: rand::Rng>(min: i32, max: i32, rng: &mut Rng) -> i32 {
    abs(rng.gen::<i32>() % (max - min + 1)) + min
}

fn nurand<Rng: rand::Rng>(a: i32, x: i32, y: i32, rng: &mut Rng) -> i32 {
    (((abs(rng.gen::<i32>() % a) | (abs(rng.gen::<i32>() % (y - x + 1)) + x)) + 42)
     % (y - x + 1)) + x
}

fn urandexcept<Rng: rand::Rng>(min: i32, max: i32, v: i32, rng: &mut Rng) -> i32 {
    if max <= min {
         return min;
    }
    let r = abs(rng.gen::<i32>() % (max - min)) + min;
    if r >= v {
        r + 1
    } else {
        r
    }
}
