
use table::{Key, Table};
use numeric::Numeric;
use std::{
    sync::Arc,
    hash::Hash,
};


use pnvm_lib::{
    tcore::*,
    txn::TxnInfo,
};

pub type WarehouseTable = Table<Warehouse, i32>;
pub type DistrictTable = Table<District, (i32, i32)>;
pub type CustomerTable = Table<Customer, (i32, i32, i32)>;
pub type NewOrderTable = Table<NewOrder, (i32, i32, i32)>;
pub type  OrderTable = Table<Order, (i32, i32, i32)>;
pub type  OrderLineTable = Table<OrderLine, (i32, i32, i32, i32)>;
pub type  ItemTable = Table<Item, i32>;
pub type  StockTable = Table<Stock, (i32, i32)>;

pub struct Tables {
   pub warehouse: WarehouseTable,
   pub district: DistrictTable,
   pub customer: CustomerTable,
   pub neworder: NewOrderTable,
   pub order: OrderTable,
   pub orderline: OrderLineTable,
   pub item: ItemTable,
   pub stock: StockTable,
}

pub type TablesRef = Arc<Tables>;


pub trait TableRef
{
    fn into_table_ref(self, Option<usize>, Option<Arc<TxnInfo>>, Option<Arc<Tables>>) -> Box<dyn TRef>;
}


#[derive(Clone)]
pub struct Warehouse {
    w_id: i32,
    w_name: String,
    w_street_1: String,
    w_street_2: String,
    w_city: String,
    w_state: String,
    w_zip: String,
    pub w_tax: Numeric, // numeric(4, 4)
    w_ytd: Numeric, // numeric(12, 2)
}

impl Key<i32> for Warehouse {
    #[inline(always)]
    fn primary_key(&self) -> i32 {
        self.w_id
    }
}



#[derive(Clone)]
pub struct District {
    d_id: i32,
    d_w_id: i32,
    d_name: String,
    d_street_1: String,
    d_street_2: String,
    d_city: String,
    d_state: String,
    d_zip: String,
    pub d_tax: Numeric, // numeric(4, 4)
    d_ytd: Numeric, // numeric(12,2)
    pub d_next_o_id: i32,
}

impl Key<(i32, i32)> for District {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
        (self.d_w_id, self.d_id)
    }
}


#[derive(Clone)]
pub struct Customer {
    c_id: i32,
    c_d_id: i32,
    c_w_id: i32,
    c_first: String,
    c_middle: String,
    c_last: String,
    c_street_1: String,
    c_street_2: String,
    c_city: String,
    c_state: String,
    c_zip: String,
    c_phone: String,
    c_since: i32, // Timestamp
    c_credit: String,
    c_credit_lim: Numeric, // numeric(12,2)
    pub c_discount: Numeric, // numeric(4, 4)
    c_balance: Numeric, // numeric(12,2)
    c_ytd_paymenr: Numeric, // numeric(12,2)
    c_payment_cnt: Numeric, // numeric(4,0)
    c_delivery_cnt: Numeric, // numeric(4,0)
    c_data: String,
}

impl Key<(i32, i32, i32)> for Customer {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.c_w_id, self.c_d_id, self.c_id)
    }
}





#[derive(Clone)]
pub struct NewOrder {
    pub no_o_id: i32,
    pub no_d_id: i32,
    pub no_w_id: i32,
}


impl Key<(i32, i32, i32)> for NewOrder {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32 , i32) {
        (self.no_w_id, self.no_d_id, self.no_o_id)
    }
}


#[derive(Clone)]
pub struct Order {
    pub o_id: i32,
    pub o_d_id: i32,
    pub o_w_id: i32,
    pub o_c_id: i32,
    pub o_entry_d: i64, // Timestamp
    pub o_carrier_id: i32,
    pub o_ol_cnt: Numeric, // numeric(2,0)
    pub o_all_local: Numeric, // numeric(1, 0)
}

impl Key<(i32, i32, i32)> for Order {

    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.o_w_id, self.o_d_id, self.o_id)
    }
}

#[derive(Clone)]
pub struct OrderLine {
    pub ol_o_id: i32,
    pub ol_d_id: i32,
    pub ol_w_id: i32,
    pub ol_number: i32,
    pub ol_i_id: i32,
    pub ol_supply_w_id: i32,
    pub ol_delivery_d: i32,
    pub ol_quantity: Numeric, // numeric(2,0)
    pub ol_amount: Numeric, // numeric(6, 2)
    pub ol_dist_info: String,
}

impl Key<(i32, i32, i32, i32)> for OrderLine {
    #[inline(always)] 
    fn primary_key(&self) -> (i32, i32, i32, i32) {
        (self.ol_w_id, self.ol_d_id, self.ol_o_id, self.ol_number)
    }
}


#[derive(Clone)]
pub struct Item {
    i_id: i32,
    i_im_id: i32,
    i_name: String,
    pub i_price: Numeric, // numeric(5,2)
    i_data: String,
}

impl Key<i32> for Item {
    #[inline(always)] 
    fn primary_key(&self) -> i32 {
        self.i_id
    }
}



#[derive(Clone)]
pub struct Stock {
    pub s_i_id: i32,
    pub s_w_id: i32,
    pub s_quantity: Numeric, // numeric(4,0)
    pub s_dist_01: String,
    pub s_dist_02: String,
    pub s_dist_03: String,
    pub s_dist_04: String,
    pub s_dist_05: String,
    pub s_dist_06: String,
    pub s_dist_07: String,
    pub s_dist_08: String,
    pub s_dist_09: String,
    pub s_dist_10: String,
    pub s_ytd: Numeric, // numeric(8,0)
    pub s_order_cnt: Numeric, // numeric(4, 0)
    pub s_remote_cnt: Numeric, // numeric(4,0)
    pub s_data: String,
}

impl Key<(i32, i32)> for Stock {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
        (self.s_w_id, self.s_i_id)
    }
}



