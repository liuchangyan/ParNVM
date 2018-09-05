
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


#[derive(Clone, Debug)]
pub struct Warehouse {
   pub w_id: i32,
   pub w_name: String,
   pub w_street_1: String,
   pub w_street_2: String,
   pub w_city: String,
   pub w_state: String,
   pub w_zip: String,
   pub w_tax: Numeric, // numeric(4, 4)
   pub w_ytd: Numeric, // numeric(12, 2)
}

impl Key<i32> for Warehouse {
    #[inline(always)]
    fn primary_key(&self) -> i32 {
        self.w_id
    }
}



#[derive(Clone, Debug)]
pub struct District {
   pub  d_id: i32,
   pub  d_w_id: i32,
   pub  d_name: String,
   pub  d_street_1: String,
   pub  d_street_2: String,
   pub  d_city: String,
   pub  d_state: String,
   pub  d_zip: String,
   pub  d_tax: Numeric, // numeric(4, 4)
   pub  d_ytd: Numeric, // numeric(12,2)
   pub  d_next_o_id: i32,
}

impl Key<(i32, i32)> for District {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
        (self.d_w_id, self.d_id)
    }
}


#[derive(Clone, Debug)]
pub struct Customer {
   pub c_id: i32,
   pub c_d_id: i32,
   pub c_w_id: i32,
   pub c_first: String,
   pub c_middle: String,
   pub c_last: String,
   pub c_street_1: String,
   pub c_street_2: String,
   pub c_city: String,
   pub c_state: String,
   pub c_zip: String,
   pub c_phone: String,
   pub c_since: i32, // Timestamp
   pub c_credit: String,
   pub c_credit_lim: Numeric, // numeric(12,2)
   pub c_discount: Numeric, // numeric(4, 4)
   pub c_balance: Numeric, // numeric(12,2)
   pub c_ytd_payment: Numeric, // numeric(12,2)
   pub c_payment_cnt: Numeric, // numeric(4,0)
   pub c_delivery_cnt: Numeric, // numeric(4,0)
   pub c_data: String,
}

impl Key<(i32, i32, i32)> for Customer {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.c_w_id, self.c_d_id, self.c_id)
    }
}





#[derive(Clone, Debug)]
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


#[derive(Clone, Debug)]
pub struct Order {
    pub o_id: i32,
    pub o_d_id: i32,
    pub o_w_id: i32,
    pub o_c_id: i32,
    pub o_entry_d: i32, // Timestamp
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

#[derive(Clone, Debug)]
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


#[derive(Clone, Debug)]
pub struct Item {
    pub i_id: i32,
    pub i_im_id: i32,
    pub i_name: String,
    pub i_price: Numeric, // numeric(5,2)
    pub i_data: String,
}

impl Key<i32> for Item {
    #[inline(always)] 
    fn primary_key(&self) -> i32 {
        self.i_id
    }
}



#[derive(Clone, Debug)]
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


#[derive(Clone, Debug)]
pub struct History {
   pub h_c_id : i32,
   pub h_c_d_id : i32,
   pub h_c_w_id : i32,
   pub h_d_id : i32,
   pub h_w_id : i32,
   pub h_date : i32 , //timestamp
   pub h_amount : Numeric, //numeric(6,2)
   pub h_data : String,
}

impl Key<i32> for History {
    #[inline(always)]
    fn primary_key(&self) -> i32 {
        0
    }
}


