

use super::{
    table::*,
    entry::*,
    tpcc_tables::*,
    numeric::*,
    workload_common::*,
};

use pnvm_lib::{
    lock::lock_txn::*,
};

use std::{
    result,
    time,
    sync::Arc,
};

use rand::{
    thread_rng,
    rngs::SmallRng,
    Rng,
};

type Result = result::Result<(), ()>;


fn new_order(tx: &mut Transaction2PL, 
             tables: &TablesRef,
             w_id: i32,
             d_id: i32,
             c_id: i32,
             ol_cnt: i32,
             src_whs : &[i32],
             item_ids: &[i32],
             qty: &[i32],
             now: i32)
    ->Result
{
    let tid = tx.id();
    let dis_num = num_district_get();
    let warehouse_ref = tables.warehouse.retrieve(&w_id, w_id as usize).unwrap().into_table_ref(None, None);
    //println!("READ : WAREHOUSE : {:?}", warehouse_ref.get_id());
    let w_tax = match tx.read::<Warehouse>(&warehouse_ref) {
        Ok(v) => v.w_tax,
        Err(..) => return Err(())
    };
    
    let customer_ref = tables.customer.retrieve(&(w_id, d_id, c_id)).unwrap().into_table_ref(None, None);
    let c_discount = match tx.read::<Customer>(&customer_ref) {
        Ok(v) => v.c_discount,
        Err(..) => return Err(())
    };
    
    info!("[{:?}][TXN-NEWORDER] Read Customer {:?}", tid, c_id);

    let district_ref = tables.district.retrieve(&(w_id, d_id), (w_id * dis_num + d_id) as usize).unwrap().into_table_ref(None, None);
    //println!("READ : DISTRICT : {:?}", district_ref.get_id());
    let mut district = match tx.read::<District>(&district_ref)
    {
        Ok(d) => d.clone(),
        Err(_) => return Err(())
    };

    let o_id = district.d_next_o_id;
    let d_tax = district.d_tax;
    district.d_next_o_id = o_id +1;
    //tx.write(district_ref, district);
    match tx.write_field(&district_ref, district, vec![D_NEXT_O_ID]) {
        Ok(_) => {},
        Err(_) => return Err(()),
    }

     let mut all_local :i64 = 1;
     for i in 0..ol_cnt as usize {
         if w_id != src_whs[i] {
             all_local = 0;

             #[cfg(feature = "noconflict")]
             panic!("no_conflict!");
         }
     }
      
     info!("[{:?}][TXN-NEWORDER] push_lock ORDER {:?}, [w_id:{}, d_id:{}, o_id: {}, c_id: {}, cnt {}]", tid, o_id, w_id, d_id, o_id, c_id, ol_cnt);
     if tables.order
         .push_lock(tx,
               Order {
                   o_id: o_id, o_d_id: d_id, o_w_id: w_id, 
                   o_c_id: c_id, o_entry_d: now,
                   o_carrier_id: 0, o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                   o_all_local: Numeric::new(all_local, 1, 0)
               },
               tables).is_err() {
            return Err(());
         }

     info!("[{:?}][TXN-NEWORDER] push_lock NEWORDER  {:?}", tid, o_id);
     if tables.neworder.push_lock(tx,
                          NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id },
                          tables).is_err() {
        return Err(());
     }

     for i in 0..ol_cnt as usize {
         //let i_price = tables.item.retrieve(item_ids[i]).unwrap().read(tx).i_price;
         let id = item_ids[i];
         let item_arc = tables.item.retrieve(&id, id as usize ).unwrap();
         let item_ref = item_arc.into_table_ref(None, None);
         //println!("READ : ITEM : {:?}", item_ref.get_id());
         let i_price = match tx.read::<Item>(&item_ref){
            Ok(v) => v.i_price,
            Err(_) => return Err(()),
         };

         let stock_ref = tables.stock.retrieve(&(src_whs[i], item_ids[i]), src_whs[i] as usize).unwrap().into_table_ref(None, None);
         let mut stock = match tx.read::<Stock>(&stock_ref) {
            Ok(v) => v.clone(),
            Err(_) => return Err(()),
         };
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
         //tx.write(stock_ref, stock);
         if tx.write_field(&stock_ref, stock, vec![S_QUANTITY, S_ORDER_CNT, S_REMOTE_CNT]).is_err() {
            return Err(());
         }
         let ol_amount = qty * i_price * (Numeric::new(1, 1, 0) + w_tax + d_tax) *
             (Numeric::new(1, 1, 0) - c_discount);
            
         //println!("{}", s_dist);
         info!("[{:?}][TXN-NEWORDER] push_lockING ORDERLINE  (w_id:{:?}, d_id:{}, o_id: {}, ol_cnt: {})", tid, w_id, d_id, o_id, i+1);
         if tables.orderline
             .push_lock(tx, 
                   OrderLine {
                       ol_o_id: o_id, ol_d_id: d_id, ol_w_id: w_id, 
                       ol_number: i as i32 + 1, ol_i_id: item_ids[i],
                       ol_supply_w_id: src_whs[i], ol_delivery_d: 0, 
                       ol_quantity: qty, ol_amount: ol_amount,
                       ol_dist_info: s_dist
                   },
                   tables).is_err() {
                return Err(());
             }
     }

     Ok(())
}

pub fn new_order_random(tx: &mut Transaction2PL,
                        tables: &Arc<Tables>, 
                        w_home : i32,
                        rng: &mut SmallRng) 
    -> Result
{
    let num_wh = num_warehouse_get();
    let num_dis = num_district_get();
    let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32;     
    //let w_id = urand(1, NUM_WAREHOUSES, rng);
    let w_id = w_home;
    let d_id = urand(1, num_dis, rng);
    let c_id = nurand(1023, 1, 3000, rng);
    let ol_cnt = urand(5, 15, rng);

    let mut supware = [0 as i32;15];
    let mut itemid = [0 as i32;15];
    let mut qty = [0 as i32;15];

    for i in 0..ol_cnt as usize {
        //supware[i] = if true {
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

    new_order(tx, tables, w_id, d_id, c_id, ol_cnt, &supware, &itemid, &qty, now)
}
