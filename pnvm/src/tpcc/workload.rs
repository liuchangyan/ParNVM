
use super::{
    table::*,
    entry::*,
    entry_ref::*,
    numeric::*,
};

use util::Config;

use pnvm_lib::{
    occ::occ_txn::*,
    txn::*,
};


use std::{
    sync::Arc,
    time,
    char,
    str,
};

use rand::{self, 
    thread_rng,
    rngs::SmallRng, 
    Rng,
    distributions::{ Uniform, Alphanumeric, Distribution},
};
use num::{
    abs,
    pow::pow,
};


const NUM_WAREHOUSES : i32 = 5;
const NUM_INIT_ORDER: i32 = 3000;
const NUM_INIT_NEXT_ORDER : i32 = 3001;
const NUM_INIT_ITEM: i32 = 100_000;
const NUM_INIT_CUSTOMER : i32 = 3000;

pub fn prepare_workload_occ(conf: &Config, rng: &mut SmallRng) -> TablesRef {
    
    let mut tables = Tables {
        warehouse: Table::new_with_buckets(16, NUM_WAREHOUSES as usize, "warehouse"),
        district: Table::new_with_buckets(NUM_INIT_DISTRICT as usize, NUM_INIT_DISTRICT as usize, "district"),
        customer: CustomerTable::new_with_buckets(128, 64, "customer"),
        neworder: Table::new_with_buckets(32, 32768, "neworder"),
        order: Table::new_with_buckets(32, 32768, "order"),
        orderline: Table::new_with_buckets(32, 32768, "orderline"),
        item: Table::new_with_buckets(512, 256, "item"),
        history: Table::new_with_buckets(128, 128, "history"),
        stock: Table::new_with_buckets(32, 512 ,"stock"),
    };

    fill_item(&mut tables, conf, rng);
    fill_warehouse(&mut tables, conf, rng);

    //println!("{:?}", tables);
    Arc::new(tables)
}

fn fill_item(tables: &mut Tables,
             _config : &Config,
             rng: &mut SmallRng 
             )
{
    for i_id in 1..=NUM_INIT_ITEM {
        let item = Item::new(
            i_id,
            urand(1, 10_000, rng),
            rand_a_string(14, 24, rng),
            rand_numeric(1.00, 100.00, 5, 2, rng),
            rand_data(26, 50, rng),
            );
       // let item = Item {
       //     i_id : i_id,
       //     i_im_id : urand(1, 10_000, rng),
       //     i_name : rand_a_string(14, 24, rng),
       //     i_price : rand_numeric(1.00, 100.00, 5, 2, rng),
       //     i_data : rand_data(26, 50, rng),
       // };

        tables.item.push_raw(item);
    }
}


fn fill_warehouse(tables: &mut Tables, _config : &Config, rng: &mut SmallRng) {
    
    for w_id in 1..=NUM_WAREHOUSES {
       let warehouse = Warehouse::new(
           w_id, 
           rand_a_string(6, 10, rng),
           rand_a_string(10, 20, rng),
           rand_a_string(10, 20, rng),
           rand_a_string(10, 20, rng),
           rand_a_string(2, 2, rng),
           rand_zip(rng),
           rand_numeric(0.0, 0.2, 5, 4, rng),
           Numeric::new(300000, 12, 2),
           );
       //let warehouse = Warehouse {
       //    w_id : w_id, 
       //    w_name : rand_a_string(6, 10, rng),
       //    w_street_1 : rand_a_string(10, 20, rng),
       //    w_street_2 : rand_a_string(10, 20, rng),
       //    w_city : rand_a_string(10, 20, rng),
       //    w_state: rand_a_string(2, 2, rng),
       //    w_zip : rand_zip(rng),
       //    w_tax : rand_numeric(0.0, 0.2, 5, 4, rng),
       //    w_ytd : Numeric::new(300000, 12, 2),
       //};

       tables.warehouse.push_raw(warehouse);

       fill_stock(tables, _config , w_id, rng);
       fill_district(tables, _config , w_id, rng);
    }
}

const NUM_INIT_STOCK: i32 = 100_000;
fn fill_stock(tables: &mut Tables, _config: &Config, w_id : i32, rng: &mut SmallRng) 
{
    for s_id in 1..=NUM_INIT_STOCK {
        let stock = Stock::new(
            s_id,
            w_id,
            rand_numeric(10.0, 100.0, 4, 0, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            rand_a_string(24, 24, rng),
            Numeric::new(0, 8, 0),
            Numeric::new(0, 4, 0),
            Numeric::new(0, 4, 0),
            rand_data(26, 50, rng),
            );
       // let stock = Stock {
       //     s_i_id : s_id,
       //     s_w_id : w_id,
       //     s_quantity : rand_numeric(10.0, 100.0, 4, 0, rng),
       //     s_dist_01: String::from("hillo"),
       //     s_dist_02: String::from("hillo"),
       //     s_dist_03: String::from("hillo"),
       //     s_dist_04: String::from("hillo"),
       //     s_dist_05: String::from("hillo"),
       //     s_dist_06: String::from("hillo"),
       //     s_dist_07: String::from("hillo"),
       //     s_dist_08: String::from("hillo"),
       //     s_dist_09: String::from("hillo"),
       //     s_dist_10: String::from("hillo"),
       //     s_ytd : Numeric::new(0, 8, 0),
       //     s_order_cnt : Numeric::new(0, 4, 0),
       //     s_remote_cnt : Numeric::new(0, 4, 0),
       //     s_data : rand_data(26, 50, rng),
       // };
        //let stock = Stock {
        //    s_i_id : s_id,
        //    s_w_id : w_id,
        //    s_quantity : rand_numeric(10.0, 100.0, 4, 0, rng),
        //    s_dist_01: rand_a_string(24, 24, rng),
        //    s_dist_02: rand_a_string(24, 24, rng),
        //    s_dist_03: rand_a_string(24, 24, rng),
        //    s_dist_04: rand_a_string(24, 24, rng),
        //    s_dist_05: rand_a_string(24, 24, rng),
        //    s_dist_06: rand_a_string(24, 24, rng),
        //    s_dist_07: rand_a_string(24, 24, rng),
        //    s_dist_08: rand_a_string(24, 24, rng),
        //    s_dist_09: rand_a_string(24, 24, rng),
        //    s_dist_10: rand_a_string(24, 24, rng),
        //    s_ytd : Numeric::new(0, 8, 0),
        //    s_order_cnt : Numeric::new(0, 4, 0),
        //    s_remote_cnt : Numeric::new(0, 4, 0),
        //    s_data : rand_data(26, 50, rng),
        //};

        tables.stock.push_raw(stock);
    }
}

const NUM_INIT_DISTRICT :i32 = 10;
fn fill_district(tables : &mut Tables, _config : &Config, w_id : i32, rng: &mut SmallRng) 
{
    for d_id in 1..=NUM_INIT_DISTRICT {
        let district = District::new(
             d_id, 
             w_id,
             rand_a_string(6, 10, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(2, 2, rng),
             rand_zip(rng),
             rand_numeric(0.0, 0.20, 5, 4, rng),
             Numeric::new(30_000, 12, 2),
             NUM_INIT_NEXT_ORDER,
            );
       // let district = District {
       //     d_id : d_id, 
       //     d_w_id : w_id,
       //     d_name : rand_a_string(6, 10, rng),
       //     d_street_1 : rand_a_string(10, 20, rng),
       //     d_street_2 : rand_a_string(10, 20, rng),
       //     d_city : rand_a_string(10, 20, rng),
       //     d_state : rand_a_string(2, 2, rng),
       //     d_zip : rand_zip(rng),
       //     d_tax : rand_numeric(0.0, 0.20, 5, 4, rng),
       //     d_ytd : Numeric::new(30_000, 12, 2),
       //     d_next_o_id : NUM_INIT_NEXT_ORDER,
       // };

        tables.district.push_raw(district);

        fill_customer(tables,  _config, w_id, d_id, rng);
        fill_order(tables, _config, w_id, d_id, rng);
        fill_neworder(tables, _config, w_id, d_id, rng);
    }
}

const NUM_INI_NEW_ORDER_START : i32  = 2101;
const NUM_INI_NEW_ORDER_END : i32  = 3000;

fn fill_neworder(
    tables: &mut Tables,
    _config :&Config,
    w_id :i32,
    d_id : i32,
    rng: &mut SmallRng
    ) 
{
    for o_id in NUM_INI_NEW_ORDER_START..=NUM_INI_NEW_ORDER_END{
        let neworder = NewOrder {
            no_o_id : o_id,
            no_d_id : d_id,
            no_w_id : w_id,
        };

        tables.neworder.push_raw(neworder);
    }
}

fn fill_order(tables : &mut Tables, _config : &Config , w_id :i32, d_id : i32, rng: &mut SmallRng) 
{
    let mut c_ids : Vec<i32> = (1..=NUM_INIT_ORDER).collect();
    rng.shuffle(&mut c_ids);

    for o_id in 1..=NUM_INIT_ORDER {
        let o_carrier_id = if o_id < NUM_INI_NEW_ORDER_START {
            urand(1, 10, rng)
        } else {
            0
        };
        let timestamp = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32;

        let o_ol_cnt = urand(5, 15, rng);

        let order = Order::new (
             o_id,
             c_ids.pop().expect("not enough c_ids"),
             d_id,
             w_id,
             timestamp,
              o_carrier_id,
             Numeric::new(o_ol_cnt.into(), 2, 0),
             Numeric::new(1, 1, 0),
        );

        tables.order.push_raw(order);

        fill_orderline(tables, _config, w_id, d_id, o_id, o_ol_cnt,timestamp, rng);
    }
}

fn fill_orderline(tables: &mut Tables, _config : &Config, w_id : i32, d_id: i32, o_id : i32, o_ol_cnt: i32, o_entry_d : i32, rng: &mut SmallRng) 
{
    for ol_number in 1..=o_ol_cnt {
        let ol_delivery_d = if o_id < NUM_INI_NEW_ORDER_START {
            o_entry_d
        } else {
            0
        };

        let ol_amount = if o_id < NUM_INI_NEW_ORDER_START {
            Numeric::new(0, 6,2)
        } else {
            rand_numeric(0.01, 9999.99, 6, 2, rng)
        };

        let orderline = OrderLine::new(
             o_id,
             d_id,
             w_id,
             ol_number,
             urand(1, 100_000, rng),
             w_id,
             ol_delivery_d,
             Numeric::new(5, 2, 0),
             ol_amount,
             rand_a_string(24,24, rng)
        );

        tables.orderline.push_raw(orderline);
    }
}



fn fill_customer(tables : &mut Tables, _config : &Config , w_id :i32, d_id : i32, rng: &mut SmallRng) 
{
    for c_id in 1..= NUM_INIT_CUSTOMER {
        let timestamp = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32;
        let credit = if urand(1, 10, rng) == 1 {
            String::from("BC")
        } else {
            String::from("GC")
        };

        let customer = Customer::new(
             c_id,
             d_id,
             w_id, 
             rand_a_string(8, 16, rng),
             String::from("OE"),
             rand_last_name(c_id, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(10, 20, rng),
             rand_a_string(2, 2, rng),
             rand_zip(rng),
             rand_n_string(16, 16, rng),
             timestamp,
             credit,
             Numeric::new(50_000, 12, 2),
             rand_numeric(0.0, 0.5, 5, 4, rng),
             Numeric::from_str("-10.00", 12, 2).expect("invalid c_balance"),
             Numeric::from_str("10.00", 12, 2).expect("invliad c_ytd_payment"),
             Numeric::new(1, 4, 0),
             Numeric::new(0, 4, 0),
             rand_a_string(300, 500, rng),
           );

       // let customer = Customer {
       //     c_id : c_id,
       //     c_d_id : d_id,
       //     c_w_id : w_id, 
       //     c_last : rand_last_name(c_id, rng),
       //     c_middle : String::from("OE"),
       //     c_first : rand_a_string(8, 16, rng),
       //     c_street_1 : rand_a_string(10, 20, rng),
       //     c_street_2 : rand_a_string(10, 20, rng),
       //     c_city : rand_a_string(10, 20, rng),
       //     c_state : rand_a_string(2, 2, rng),
       //     c_zip: rand_zip(rng),
       //     c_phone : rand_n_string(16, 16, rng),
       //     c_since : timestamp,
       //     c_credit : credit,
       //     c_credit_lim : Numeric::new(50_000, 12, 2),
       //     c_discount : rand_numeric(0.0, 0.5, 5, 4, rng),
       //     c_balance : Numeric::from_str("-10.00", 12, 2).expect("invalid c_balance"),
       //     c_ytd_payment : Numeric::from_str("10.00", 12, 2).expect("invliad c_ytd_payment"),
       //     c_payment_cnt : Numeric::new(1, 4, 0),
       //     c_delivery_cnt : Numeric::new(0, 4, 0),
       //     c_data : rand_a_string(300, 500, rng),
       // };
        
        //println!("PUSHING CUSTOMER {}, {}, {}, {}", c_id, w_id, d_id, customer.c_last);
        tables.customer.push_raw(customer);

        fill_history(tables, _config, w_id, d_id, c_id, timestamp, rng);
    }
}

fn fill_history(
    tables: &mut Tables,
    _config: &Config, 
    w_id: i32, 
    d_id:i32,
    c_id: i32,
    timestamp: i32,
    rng: &mut SmallRng)
{
    tables.history.push_raw(History::new(
         c_id,
         d_id,
         d_id,
         w_id,
         w_id,
         timestamp,
         Numeric::new(10,6,2),
         rand_a_string(12, 24, rng),
    ));
}


fn new_order(tx: &mut TransactionOCC, 
             tables: &TablesRef,
             w_id: i32,
             d_id: i32,
             c_id: i32,
             ol_cnt: i32,
             src_whs : &[i32],
             item_ids: &[i32],
             qty: &[i32],
             now: i32)

{
    let tid = tx.commit_id();
    let warehouse_ref = tables.warehouse.retrieve(&w_id).unwrap().into_table_ref(None, None);
    //println!("READ : WAREHOUSE : {:?}", warehouse_ref.get_id());
    let w_tax = tx.read::<Warehouse>(warehouse_ref).w_tax;
    
    let customer_ref = tables.customer.retrieve(&(w_id, d_id, c_id)).unwrap().into_table_ref(None, None);
    let c_discount = tx.read::<Customer>(customer_ref).c_discount;
     info!("[{:?}][TXN-NEWORDER] Read Customer {:?}", tid, c_id);

    let district_ref = tables.district.retrieve(&(w_id, d_id)).unwrap().into_table_ref(None, None);
    //println!("READ : DISTRICT : {:?}", district_ref.get_id());
    let mut district = tx.read::<District>(district_ref.box_clone()).clone();

    let o_id = district.d_next_o_id;
    let d_tax = district.d_tax;
    district.d_next_o_id = o_id +1;
    tx.write(district_ref, district);

     let mut all_local :i64 = 1;
     for i in 0..ol_cnt as usize {
         if w_id != src_whs[i] {
             all_local = 0;
         }
     }
      
     info!("[{:?}][TXN-NEWORDER] Push ORDER {:?}, cnt {}", tid, o_id, ol_cnt);
     tables.order.push(tx,
                       Order {
                           o_id: o_id, o_d_id: d_id, o_w_id: w_id, o_c_id: c_id, o_entry_d: now,
                           o_carrier_id: 0, o_ol_cnt: Numeric::new(ol_cnt as i64, 1, 0),
                           o_all_local: Numeric::new(all_local, 1, 0)
                       },
                       tables);
     info!("[{:?}][TXN-NEWORDER] Push NEWORDER  {:?}", tid, o_id);
     tables.neworder.push(tx,
                          NewOrder { no_o_id: o_id, no_d_id: d_id, no_w_id: w_id },
                          tables);

     for i in 0..ol_cnt as usize {
         //let i_price = tables.item.retrieve(item_ids[i]).unwrap().read(tx).i_price;
         let item_arc = tables.item.retrieve(&(item_ids[i])).unwrap();
         let item_ref = item_arc.into_table_ref(None, None);
         //println!("READ : ITEM : {:?}", item_ref.get_id());
         let i_price = tx.read::<Item>(item_ref).i_price;

         let stock_ref = tables.stock.retrieve(&(src_whs[i], item_ids[i])).unwrap().into_table_ref(None, None);
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

         let ol_amount = qty * i_price * (Numeric::new(1, 1, 0) + w_tax + d_tax) *
             (Numeric::new(1, 1, 0) - c_discount);
            
         //println!("{}", s_dist);
         info!("[{:?}][TXN-NEWORDER] PUSHING ORDERLINE  {:?}", tid, o_id);
         tables.orderline.push(tx, 
                               OrderLine {
                                   ol_o_id: o_id, ol_d_id: d_id, ol_w_id: w_id, ol_number: i as i32 + 1, ol_i_id: item_ids[i],
                                   ol_supply_w_id: src_whs[i], ol_delivery_d: 0, ol_quantity: qty, ol_amount: ol_amount,
                                   ol_dist_info: s_dist
                               },
                               tables);
     }
}

pub fn new_order_random(tx: &mut TransactionOCC, tables: &Arc<Tables>, w_home : i32,rng: &mut SmallRng) {
    let now = time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32;     
    //let w_id = urand(1, NUM_WAREHOUSES, rng);
    let w_id = w_home;
    let d_id = urand(1, 10, rng);
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


pub fn payment_random(tx: &mut TransactionOCC, 
                      tables: &Arc<Tables>,
                      w_home : i32,
                      rng : &mut SmallRng,
                      ) 
{
    //let w_id = w_home;
    let w_id = urand(1, NUM_WAREHOUSES, rng);
    let d_id = urand(1, 10, rng);
        
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
    
    //if y<= 60 {
    if y <= 60 {
        let c_last = rand_last_name(nurand(255, 0, 999, rng),rng);
        payment(tx, tables, w_id, d_id, c_w_id , c_d_id, Some(c_last), None, h_amount, h_date, rng);
    } else {
        let c_id = nurand(1023, 1, 3000, rng);
        payment(tx, tables, w_id, d_id, c_w_id , c_d_id, None, Some(c_id), h_amount, h_date, rng);
    }
}

fn payment(tx: &mut TransactionOCC,
           tables : &Arc<Tables>,
           w_id : i32,
           d_id : i32,
           c_w_id : i32,
           c_d_id : i32,
           c_last : Option<String>,
           c_id : Option<i32>,
           h_amount : Numeric,
           h_date : i32,
           rng : &mut SmallRng)
{
    let tid = tx.commit_id();
    /* RW Warehouse */
    let warehouse_row = tables.warehouse.retrieve(&w_id).expect("warehouse empty").into_table_ref(None, None);
    let mut warehouse = tx.read::<Warehouse>(warehouse_row.box_clone()).clone();
    let w_name = warehouse.w_name.clone();
   // let _w_street_1 = &warehouse.w_street_1;
   // let _w_street_2 = &warehouse.w_street_2;
   // let _w_city = &warehouse.w_city;
   // let _w_state = &warehouse.w_state;
   // let _w_zip = &warehouse.w_zip;
    warehouse.w_ytd = warehouse.w_ytd +  h_amount;
    info!("[{:?}][TXN-PAYMENT] Update Warehouse::YTD {:?}", tid, warehouse.w_ytd);
    tx.write(warehouse_row, warehouse);

    /* RW District */
    let district_row = tables.district.retrieve(&(w_id, d_id)).expect("district empty").into_table_ref(None, None);
    let mut district = tx.read::<District>(district_row.box_clone()).clone();
    let d_name = district.d_name.clone();
   // let _d_street_1 = district.d_street_1;
   // let _d_street_2 = district.d_street_2;
   // let _d_city = district.d_city;
   // let _d_state = district.d_state;
   // let _d_zip = district.d_zip;
    district.d_ytd = district.d_ytd + h_amount;
    info!("[{:?}][TXN-PAYMENT] Update District::YTD\t  {:?}", tid, district.d_ytd);
    tx.write(district_row,district);

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

    /* I History */
    let h_data = format!("{}    {}", str::from_utf8(&w_name).unwrap(), str::from_utf8(&d_name).unwrap());
    info!("[{:?}][TXN-PAYMENT] Inserting History::HDATA\t  {:?}", tid, h_data);
    tables.history.push(tx,
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
                        tables);
     
}


//pub fn orderstatus_random(tx: &mut TransactionOCC, 
//                      tables: &Arc<Tables>,
//                      w_home : i32,
//                      rng : &mut SmallRng,
//                      ) 
//{
//    let d_id = urand(1, 10, rng);
//    let w_id = w_home;
//    
//    let y = urand(1, 100, rng);
//    
//    if false {
//        let c_last = rand_last_name(nurand(255, 0, 999, rng),rng);
//        orderstatus(tx, tables, w_id, d_id, c_w_id , c_d_id, Some(c_last), None, h_amount, h_date, rng);
//    } else {
//        let c_id = nurand(1023, 1, 3000, rng);
//        orderstatus(tx, tables, w_id, d_id, c_w_id , c_d_id, None, Some(c_id), h_amount, h_date, rng);
//    }
//}
//
//
//fn orderstatus(tx: &mut TransactionOCC
//               tables: &Arc<Tables>,
//               w_id : i32,
//               d_id : i32,
//               c_w_id : i32,
//               c_d_id : i32,
//               c_last : Option<String>,
//               c_id : Option<i32>,
//               )
//{
//
//    let c_row = match c_id {
//        Some(c_id) => {
//            tables.customer.retrieve(&(c_w_id, c_d_id, c_id)).expect("customer by id empty").into_table_ref(None, None)
//        },
//        
//        None => {
//           panic!("not implemented"); 
//        }
//    };
//    //TODO:
//    let o_row = tables.order.retrieve_by_cid(&(c_w_id, c_d_id, c_id)).expect("order tempty").into_table_ref(None, None);
//
//    let o_id = tx.read::<Order>(o_row).o_id;
//    //TODO: 
//    let ol_arcs = table.orderline.find_by_oid(&(c_w_id, c_d_id, o_id));
//
//    for ol_arc in ol_arcs {
//        let ol_row = ol_arc.into_table_ref(None, None);
//        let ol = tx.read::<OrderLine>(ol_row);
//    }
//}
//
//
//
//fn delivery(tx: &mut TransactionOCC
//            tables: &Arc<Tables>,
//            w_id : i32,
//            o_carrier_id: i32,
//            )
//{
//    
//    for d_id in 1..NUM_INIT_DISTRICT {
//        //TODO:
//        let no_arc = tables.neworder.retrieve_min_oid(&(w_id, d_id));
//        if no_arc.is_some() {
//            let no_row = no_arc.unwrap().into_table_ref(None, None);
//            let no_o_id = tx.read::<NewOrder>(no_row);
//            //TODO:
//            tables.neworder.delete(&(w_id, d_id, no_o_id));
//
//            let o_row = tables.order.retrieve(&(w_id, d_id, no_o_id)).expect("order empty").into_table_ref(None, None);
//            let o = tx.read::<Order>(o_row.box_clone()).clone();
//            let o_id = o.o_id;
//            let o_c_id = o.o_c_id;
//
//            o.o_carrier_id = o_carrier_id;
//            tx.write(o_row, o);
//
//            
//            let ol_arcs = table.orderline.find_by_oid(&(w_id, d_id, o_id));
//            let now = gen_now();
//            let ol_amount_sum = Numeric::new(0, 6, 2);
//            for ol_arc in ol_arcs {
//                let ol_row = ol_arc.into_table_ref(None, None);
//                let ol = tx.read::<OrderLine>(ol_row.box_clone()).clone();
//                ol_amount_sum += ol.ol_amount;
//
//                ol.ol_delivery_d = now;
//                tx.write(ol_row, ol);
//            }
//
//            
//            let c_row = table.customer.retrieve(&(w_id, d_id, o_c_id)).expect("deliver::customer not empty").into_table_ref(None, None);
//            let c = tx.read(c_row.box_clone()).clone();
//            c.c_balance += ol_amount_sum;
//            c.c_delivery_cnt += Numeric::new(1, 4, 0);
//
//            tx.write(c_row, c);
//        }
//    }
//
//}
//
//fn stocklevel(tx: &mut TransactionOCC,
//              tables : &Arc<Tables>,
//              w_id : i32,
//              d_id : i32,
//              thd: Numeric,
//              )
//{
//    let d_row = tables.district.retrieve(&(w_id, d_id)).unwrap().into_table_ref(None, None);
//    let d = tx.read::<District>(d_row);
//    let d_next_o_id = d.d_next_o_id;
//    
//    //TODO
//    let ol_arcs = tables.orderline.find_range(&(w_id, d_id), d_next_o_id - 20, d_next_o_id);
//    
//    let mut ol_i_ids = vec![];
//    for ol_arc in ol_arcs {
//        let ol_row = ol_arc.into_table_ref(None, None);
//        let ol = tx.read::<OrderLine>(ol_row);
//        ol_i_ids.push(ol.ol_i_id);
//
//    }
//    
//    let low_stock = 0;
//    for ol_i_id in ol_i_ids.iter() {
//        let stock_row = tables.stock.retrieve(&(w_id, ol_i_id)).expect("no stock").into_table_ref(None,None);
//        
//        let stock = tx.read::<Stock>(stock_row);
//        if stock.s_quantity < thd {
//            low_stock+=1;
//        }
//    }
//}
               


fn urand(min:i32, max: i32, rng : &mut SmallRng) -> i32 {
    abs(rng.gen::<i32>() % (max - min + 1)) + min
}

fn nurand(a: i32, x: i32, y: i32, rng : &mut SmallRng) -> i32 {
    (((abs(rng.gen::<i32>() % a) | (abs(rng.gen::<i32>() % (y - x + 1)) + x)) + 42)
     % (y - x + 1)) + x
}

fn urandexcept(min: i32, max: i32, v: i32, rng : &mut SmallRng) -> i32 {
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


fn rand_a_string(len_low: usize, len_high: usize, rng : &mut SmallRng) -> String {
    let len = urand(len_low as i32, len_high as i32, rng) as usize;
    rng
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect::<String>()
}

fn rand_n_string(low : i32, high : i32, rng: &mut SmallRng) -> String 
{
    let len = urand(low , high, rng) as usize;
    Uniform::new(0, 10)
        .sample_iter(rng)
        .map(|x| char::from_digit(x, 10).expect("rand_n_string error"))
        .take(len)
        .collect::<String>()
}

fn rand_zip(rng: &mut SmallRng) -> String {
    format!("{}11111", urand(0, 9999, rng))
}

fn rand_numeric(low : f64,
                high: f64, 
                len : usize, 
                precision: usize, 
                rng : &mut SmallRng
                ) -> Numeric 
{
    let val = rng.gen_range(low, high) * pow(10, precision) as f64 ;
    Numeric::new(val.trunc() as i64, len, precision)
}



fn rand_data(low : i32, high : i32, rng : &mut SmallRng ) -> String {
    let len = urand(low, high, rng);
    let has_original = urand(1, 10, rng) == 1;

    let mut string = rng
        .sample_iter(&Alphanumeric)
        .take(len as usize)
        .collect::<String>();

    if has_original {
        let pos = urand(0, len-9, rng) as usize;
        string.replace_range(pos..pos+8, "ORIGINAL");
    }

    return string;
}


fn rand_last_name(c_id : i32, rng: &mut SmallRng) -> String {
    let mut c = if c_id <= 1000 {
        urand(0,999,rng)
    } else  {
        nurand(255, 0, 999, rng)
    };
    
    format!("{}{}{}", 
            last_name_of(c/100),
            last_name_of((c%100)/10), 
            last_name_of(c%10),
            )
}


fn last_name_of(idx : i32) -> &'static str {
    match idx {
        0 => "BAR",
        1 => "OUGHT",
        2 => "ABLE",
        3 => "PRI",
        4 => "PRES",
        5 => "ESE",
        6 => "ANTI",
        7 => "CALLY",
        8 => "ATION",
        9 => "EING",
        _ => panic!("what is your last name???!!")
    }
}

fn gen_now() -> i32 {
    time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() as i32
}
