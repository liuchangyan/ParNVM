use super::{entry::*, entry_ref::*, numeric::*, table::*, tpcc_tables::*};

use util::Config;

use std::{cell::RefCell, char, rc::Rc, str, sync::Arc, time};

use num::{abs, pow::pow};

use rand::{
    self,
    distributions::{Alphanumeric, Distribution, Uniform},
    rngs::SmallRng,
    Rng,
};

thread_local! {
    pub static G_NUM_WAREHOUSES: Rc<RefCell<i32>> = Rc::new(RefCell::new(10));
    pub static G_NUM_DIS : Rc<RefCell<i32>> = Rc::new(RefCell::new(8));
}

pub fn num_warehouse_get() -> i32 {
    G_NUM_WAREHOUSES.with(|n| *n.borrow())
}

pub fn num_warehouse_set(x: i32) {
    G_NUM_WAREHOUSES.with(|n| *n.borrow_mut() = x);
}

pub fn num_district_get() -> i32 {
    G_NUM_DIS.with(|n| *n.borrow())
}

pub fn num_district_set(x: i32) {
    G_NUM_DIS.with(|n| *n.borrow_mut() = x);
}

//pub static mut num_dis :i32 = 10;
//pub static mut num_wh : i32 = 8;
pub const NUM_INIT_ORDER: i32 = 3000;
pub const NUM_INIT_NEXT_ORDER: i32 = 3001;
pub const NUM_INIT_ITEM: i32 = 100_000;
pub const NUM_INIT_CUSTOMER: i32 = 3000;

pub fn prepare_workload(conf: &Config, rng: &mut SmallRng) -> TablesRef {
    num_warehouse_set(conf.wh_num);
    num_district_set(conf.d_num);

    let num_wh = num_warehouse_get();
    let num_dis = num_district_get();
    let total_wd: usize = (num_wh * num_dis) as usize;

    let scale_ratio = conf.thread_num / num_wh as usize;
    let mut tables = if conf.partition != 0 {
        let par = conf.partition;
        Tables {
            warehouse: Table::new_with_buckets(par, conf.wh_num as usize, "warehouse"),
            district: Table::new_with_buckets(par, num_dis as usize, "district"),
            customer: CustomerTable::new_with_buckets(par, 4096, "customer"),
            neworder: NewOrderTable::new_with_buckets(par, 4096 * 16 * scale_ratio, "neworder"),
            order: OrderTable::new_with_buckets(par, 32768 * 2 * scale_ratio, "order"),
            orderline: OrderLineTable::new_with_buckets(par, 8096 * 64 * scale_ratio, "orderline"),
            item: Table::new_with_buckets(512, 256, "item"),
            history: Table::new_with_buckets(par, 51200 * scale_ratio, "history"),
            stock: Table::new_with_buckets(par, 65536 * 2, "stock"),
        }
    } else {
        Tables {
            warehouse: Table::new_with_buckets(
                total_wd as usize,
                conf.wh_num as usize,
                "warehouse",
            ),
            district: Table::new_with_buckets(total_wd, num_dis as usize, "district"),
            customer: CustomerTable::new_with_buckets(total_wd, 4096, "customer"),
            neworder: NewOrderTable::new_with_buckets(
                total_wd,
                4096 * 16 * scale_ratio,
                "neworder",
            ),
            order: OrderTable::new_with_buckets(total_wd, 32768 * 2 * scale_ratio, "order"),
            orderline: OrderLineTable::new_with_buckets(
                total_wd,
                8096 * 64 * scale_ratio,
                "orderline",
            ),
            item: Table::new_with_buckets(512, 256, "item"),
            history: Table::new_with_buckets(total_wd, 51200 * scale_ratio, "history"),
            stock: Table::new_with_buckets(total_wd, 65536 * 2, "stock"),
        }
    };

    fill_item(&mut tables, conf, rng);
    fill_warehouse(&mut tables, conf, rng);

    //println!("{:?}", tables);
    Arc::new(tables)
}

fn fill_item(tables: &mut Tables, _config: &Config, rng: &mut SmallRng) {
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

fn fill_warehouse(tables: &mut Tables, _config: &Config, rng: &mut SmallRng) {
    for w_id in 1..=_config.wh_num {
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

        fill_stock(tables, _config, w_id, rng);
        fill_district(tables, _config, w_id, rng);
    }
}

pub const NUM_INIT_STOCK: i32 = 100_000;
fn fill_stock(tables: &mut Tables, _config: &Config, w_id: i32, rng: &mut SmallRng) {
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
        tables.stock.push_raw(stock);
    }
}

fn fill_district(tables: &mut Tables, _config: &Config, w_id: i32, rng: &mut SmallRng) {
    let num_dis = num_district_get();
    for d_id in 1..=num_dis {
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
        tables.district.push_raw(district);

        fill_customer(tables, _config, w_id, d_id, rng);
        fill_order(tables, _config, w_id, d_id, rng);
        fill_neworder(tables, _config, w_id, d_id, rng);
    }
}

pub const NUM_INI_NEW_ORDER_START: i32 = 2101;
pub const NUM_INI_NEW_ORDER_END: i32 = 3000;

fn fill_neworder(tables: &mut Tables, _config: &Config, w_id: i32, d_id: i32, rng: &mut SmallRng) {
    for o_id in NUM_INI_NEW_ORDER_START..=NUM_INI_NEW_ORDER_END {
        let neworder = NewOrder {
            no_o_id: o_id,
            no_d_id: d_id,
            no_w_id: w_id,
        };

        tables.neworder.push_raw(neworder);
    }
}

fn fill_order(tables: &mut Tables, _config: &Config, w_id: i32, d_id: i32, rng: &mut SmallRng) {
    let mut c_ids: Vec<i32> = (1..=NUM_INIT_ORDER).collect();
    rng.shuffle(&mut c_ids);

    for o_id in 1..=NUM_INIT_ORDER {
        let o_carrier_id = if o_id < NUM_INI_NEW_ORDER_START {
            urand(1, 10, rng)
        } else {
            0
        };
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i32;

        let o_ol_cnt = urand(5, 15, rng);

        let order = Order::new(
            o_id,
            d_id,
            w_id,
            c_ids.pop().expect("not enough c_ids"),
            timestamp,
            o_carrier_id,
            Numeric::new(o_ol_cnt.into(), 2, 0),
            Numeric::new(1, 1, 0),
        );

        tables.order.push_raw(order);

        fill_orderline(tables, _config, w_id, d_id, o_id, o_ol_cnt, timestamp, rng);
    }
}

fn fill_orderline(
    tables: &mut Tables,
    _config: &Config,
    w_id: i32,
    d_id: i32,
    o_id: i32,
    o_ol_cnt: i32,
    o_entry_d: i32,
    rng: &mut SmallRng,
) {
    for ol_number in 1..=o_ol_cnt {
        let ol_delivery_d = if o_id < NUM_INI_NEW_ORDER_START {
            o_entry_d
        } else {
            0
        };

        let ol_amount = if o_id < NUM_INI_NEW_ORDER_START {
            Numeric::new(0, 6, 2)
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
            rand_a_string(24, 24, rng),
        );

        tables.orderline.push_raw(orderline);
    }
}

fn fill_customer(tables: &mut Tables, _config: &Config, w_id: i32, d_id: i32, rng: &mut SmallRng) {
    for c_id in 1..=NUM_INIT_CUSTOMER {
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i32;
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

        tables.customer.push_raw(customer);

        fill_history(tables, _config, w_id, d_id, c_id, timestamp, rng);
    }
}

fn fill_history(
    tables: &mut Tables,
    _config: &Config,
    w_id: i32,
    d_id: i32,
    c_id: i32,
    timestamp: i32,
    rng: &mut SmallRng,
) {
    tables.history.push_raw(History::new(
        c_id,
        d_id,
        d_id,
        w_id,
        w_id,
        timestamp,
        Numeric::new(10, 6, 2),
        rand_a_string(12, 24, rng),
    ));
}

pub fn urand(min: i32, max: i32, rng: &mut SmallRng) -> i32 {
    abs(rng.gen::<i32>() % (max - min + 1)) + min
}

pub fn nurand(a: i32, x: i32, y: i32, rng: &mut SmallRng) -> i32 {
    (((abs(rng.gen::<i32>() % a) | (abs(rng.gen::<i32>() % (y - x + 1)) + x)) + 42) % (y - x + 1))
        + x
}

pub fn urandexcept(min: i32, max: i32, v: i32, rng: &mut SmallRng) -> i32 {
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

pub fn rand_a_string(len_low: usize, len_high: usize, rng: &mut SmallRng) -> String {
    let len = urand(len_low as i32, len_high as i32, rng) as usize;
    rng.sample_iter(&Alphanumeric).take(len).collect::<String>()
}

pub fn rand_n_string(low: i32, high: i32, rng: &mut SmallRng) -> String {
    let len = urand(low, high, rng) as usize;
    Uniform::new(0, 10)
        .sample_iter(rng)
        .map(|x| char::from_digit(x, 10).expect("rand_n_string error"))
        .take(len)
        .collect::<String>()
}

pub fn rand_zip(rng: &mut SmallRng) -> String {
    format!("{}11111", urand(0, 9999, rng))
}

pub fn rand_numeric(
    low: f64,
    high: f64,
    len: usize,
    precision: usize,
    rng: &mut SmallRng,
) -> Numeric {
    let val = rng.gen_range(low, high) * pow(10, precision) as f64;
    Numeric::new(val.trunc() as i64, len, precision)
}

pub fn rand_data(low: i32, high: i32, rng: &mut SmallRng) -> String {
    let len = urand(low, high, rng);
    let has_original = urand(1, 10, rng) == 1;

    let mut string = rng
        .sample_iter(&Alphanumeric)
        .take(len as usize)
        .collect::<String>();

    if has_original {
        let pos = urand(0, len - 9, rng) as usize;
        string.replace_range(pos..pos + 8, "ORIGINAL");
    }

    return string;
}

pub fn rand_last_name(c_id: i32, rng: &mut SmallRng) -> String {
    let c = if c_id <= 1000 {
        urand(0, 999, rng)
    } else {
        nurand(255, 0, 999, rng)
    };

    format!(
        "{}{}{}",
        last_name_of(c / 100),
        last_name_of((c % 100) / 10),
        last_name_of(c % 10),
    )
}

pub fn last_name_of(idx: i32) -> &'static str {
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
        _ => panic!("what is your last name???!!"),
    }
}

pub fn gen_now() -> i32 {
    time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i32
}
