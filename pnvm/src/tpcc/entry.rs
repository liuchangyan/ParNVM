//************************************************
//Entry types defnitions
//
//Types: 
//- Warehouse
//- NewOrder
//- ....

use numeric::Numeric;
use std::{
    hash::Hash, sync::Arc,
    fmt::{Debug, self},
};
use table::{Key, Table};
use workload_occ::{num_district_get, num_warehouse_get};

use pnvm_lib::{tcore::*, txn::TxnInfo};

#[inline]
fn copy_from_string(dest : &mut [u8], src: String) {
    dest[..src.len()].copy_from_slice(src.as_bytes());
}

#[derive(Clone, Debug)]
pub struct Warehouse {
    pub w_id: i32,
    pub w_name: [u8; 10],
    pub w_street_1: [u8; 20],
    pub w_street_2: [u8; 20],
    pub w_city: [u8; 20],
    pub w_state: [u8; 2],
    pub w_zip: [u8; 9],
    pub w_tax: Numeric, // Numeric(4, 4)
    pub w_ytd: Numeric, // Numeric(12, 2)
}

impl Key<i32> for Warehouse {
    #[inline(always)]
    fn primary_key(&self) -> i32 {
        self.w_id
    }

    fn bucket_key(&self) -> usize {
        self.w_id as usize
    }
}

impl Warehouse {
    pub fn new(
        w_id: i32,
        w_name_str : String,
        w_street_1_str: String,
        w_street_2_str : String,
        w_city_str: String,
        w_state_str: String,
        w_zip_str: String,
        w_tax : Numeric,
        w_ytd: Numeric,
        ) -> Self 
    {
        let mut w_name : [u8;10] = Default::default();
        copy_from_string(&mut w_name, w_name_str);
        let mut w_street_1 : [u8;20] = Default::default();
        copy_from_string(&mut w_street_1, w_street_1_str);
        let mut w_street_2 : [u8;20] = Default::default();
        copy_from_string(&mut w_street_2, w_street_2_str);
        let mut w_city : [u8; 20] = Default::default();
        copy_from_string(&mut w_city, w_city_str);
        let mut w_state : [u8;2] = Default::default();
        copy_from_string(&mut w_state, w_state_str);
        let mut w_zip : [u8; 9] = Default::default();
        copy_from_string(&mut w_zip, w_zip_str);

        Warehouse {
            w_id,
            w_name,
            w_street_1,
            w_street_2,
            w_city,
            w_state,
            w_zip,
            w_tax,
            w_ytd,
        }
    }
}

#[derive(Clone, Debug)]
pub struct District {
    pub d_id: i32,
    pub d_w_id: i32,
    pub d_name: [u8; 10],
    pub d_street_1: [u8; 20],
    pub d_street_2: [u8; 20],
    pub d_city: [u8; 20],
    pub d_state: [u8; 2],
    pub d_zip: [u8; 9],
    pub d_tax: Numeric, // Numeric(4, 4)
    pub d_ytd: Numeric, // Numeric(12,2)
    pub d_next_o_id: i32,
}

impl Key<(i32, i32)> for District {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
        (self.d_w_id, self.d_id)
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.d_w_id * wh_num + self.d_id) as usize
    }
}

impl District {
    pub fn new(
        d_id: i32,
        d_w_id: i32,
        d_name_str: String,
        d_street_1_str: String,
        d_street_2_str: String,
        d_city_str: String,
        d_state_str: String,
        d_zip_str: String,
        d_tax: Numeric, // Numeric(4, 4)
        d_ytd: Numeric, // Numeric(12,2)
        d_next_o_id: i32,
        ) -> Self 
    {
        let mut d_name : [u8;10] = Default::default();
        let mut d_street_1 : [u8;20] = Default::default();
        let mut d_street_2 : [u8;20] = Default::default();
        let mut d_city : [u8;20] = Default::default();
        let mut d_state : [u8;2] = Default::default();
        let mut d_zip : [u8;9] = Default::default();

        copy_from_string(&mut d_name, d_name_str);
        copy_from_string(&mut d_street_1, d_street_1_str);
        copy_from_string(&mut d_street_2, d_street_2_str);
        copy_from_string(&mut d_city, d_city_str);
        copy_from_string(&mut d_state,d_state_str);
        copy_from_string(&mut d_zip,d_zip_str );

        District {
            d_id,
            d_w_id,
            d_name,
            d_street_1,
            d_street_2,
            d_city,
            d_state,
            d_zip,
            d_tax, // Numeric(4, 4)
            d_ytd, // Numeric(12,2)
            d_next_o_id,
        }
    }

}

#[derive(Clone)]
pub struct Customer {
    pub c_id: i32,
    pub c_d_id: i32,
    pub c_w_id: i32,
    pub c_first: [u8; 16],
    pub c_middle: [u8; 2],
    pub c_last: [u8; 16],
    pub c_street_1: [u8; 20],
    pub c_street_2: [u8; 20],
    pub c_city: [u8; 20],
    pub c_state: [u8; 2],
    pub c_zip: [u8; 9],
    pub c_phone: [u8; 16],
    pub c_since: i32, // Timestamp
    pub c_credit: [u8; 2],
    pub c_credit_lim: Numeric,   // Numeric(12,2)
    pub c_discount: Numeric,     // Numeric(4, 4)
    pub c_balance: Numeric,      // Numeric(12,2)
    pub c_ytd_payment: Numeric,  // Numeric(12,2)
    pub c_payment_cnt: Numeric,  // Numeric(4,0)
    pub c_delivery_cnt: Numeric, // Numeric(4,0)
    pub c_data: [u8; 500],
}

impl Key<(i32, i32, i32)> for Customer {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.c_w_id, self.c_d_id, self.c_id)
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.c_w_id * wh_num + self.c_d_id) as usize
    }
}

impl Debug for Customer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "customer ohohoh")
    }
}

impl Customer {
    pub fn new(
        c_id: i32,
        c_d_id: i32,
        c_w_id: i32,
        c_first_str: String,
        c_middle_str: String,
        c_last_str: String,
        c_street_1_str: String,
        c_street_2_str: String,
        c_city_str: String,
        c_state_str: String,
        c_zip_str: String,
        c_phone_str: String,
        c_since: i32, // Timestamp
        c_credit_str: String,
        c_credit_lim: Numeric,   // Numeric(12,2)
        c_discount: Numeric,     // Numeric(4, 4)
        c_balance: Numeric,      // Numeric(12,2)
        c_ytd_payment: Numeric,  // Numeric(12,2)
        c_payment_cnt: Numeric,  // Numeric(4,0)
        c_delivery_cnt: Numeric, // Numeric(4,0)
        c_data_str: String,
        )-> Self {

            let mut c_first : [u8;16] = Default::default();
            let mut c_middle :[u8;2] = Default::default();
            let mut c_last : [u8;16] = Default::default();
            let mut c_street_1 : [u8;20] = Default::default();
            let mut c_street_2 : [u8;20] = Default::default();
            let mut c_city : [u8;20] = Default::default();
            let mut c_state : [u8;2] = Default::default();
            let mut c_zip : [u8;9] = Default::default();
            let mut c_phone : [u8;16] = Default::default();
            let mut c_credit : [u8;2] = Default::default();
            let mut c_data : [u8;500] = [0 ; 500]; 

            copy_from_string(&mut c_first, c_first_str);
            copy_from_string(&mut c_middle, c_middle_str);
            copy_from_string(&mut c_last, c_last_str);
            copy_from_string(&mut c_street_1, c_street_1_str);
            copy_from_string(&mut c_street_2, c_street_2_str);
            copy_from_string(&mut c_city, c_city_str);
            copy_from_string(&mut c_state, c_state_str);
            copy_from_string(&mut c_zip, c_zip_str);
            copy_from_string(&mut c_data, c_data_str);
            copy_from_string(&mut c_credit, c_credit_str);
            copy_from_string(&mut c_phone, c_phone_str);

            Customer {
                c_id,
                c_d_id,
                c_w_id,
                c_first,
                c_middle,
                c_last,
                c_street_1,
                c_street_2,
                c_city,
                c_state,
                c_zip,
                c_phone,
                c_since, // Timestamp
                c_credit,
                c_credit_lim,   // Numeric(12,2)
                c_discount,     // Numeric(4, 4)
                c_balance,      // Numeric(12,2)
                c_ytd_payment,  // Numeric(12,2)
                c_payment_cnt,  // Numeric(4,0)
                c_delivery_cnt, // Numeric(4,0)
                c_data,
            }
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
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.no_w_id, self.no_d_id, self.no_o_id)
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.no_w_id * wh_num + self.no_d_id) as usize
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
    pub o_ol_cnt: Numeric,    // Numeric(2,0)
    pub o_all_local: Numeric, // Numeric(1, 0)
}

impl Key<(i32, i32, i32)> for Order {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32) {
        (self.o_w_id, self.o_d_id, self.o_id)
    }
    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.o_w_id * wh_num + self.o_d_id) as usize
    }
}

impl Order {
    pub fn new(
        o_id: i32,
        o_d_id: i32,
        o_w_id: i32,
        o_c_id: i32,
        o_entry_d: i32, // Timestamp
        o_carrier_id: i32,
        o_ol_cnt: Numeric,    // Numeric(2,0)
        o_all_local: Numeric, // Numeric(1, 0)
    ) -> Self 
    {
        Order {
            o_id,
            o_d_id,
            o_w_id,
            o_c_id,
            o_entry_d, // Timestamp
            o_carrier_id,
            o_ol_cnt,    // Numeric(2,0)
            o_all_local, // Numeric(1, 0)
        }
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
    pub ol_quantity: Numeric, // Numeric(2,0)
    pub ol_amount: Numeric,   // Numeric(6, 2)
    pub ol_dist_info: [u8; 24],
}

impl Key<(i32, i32, i32, i32)> for OrderLine {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32, i32, i32) {
        (self.ol_w_id, self.ol_d_id, self.ol_o_id, self.ol_number)
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.ol_w_id * wh_num + self.ol_d_id) as usize
    }
}

impl OrderLine {
    pub fn new(
        ol_o_id: i32,
        ol_d_id: i32,
        ol_w_id: i32,
        ol_number: i32,
        ol_i_id: i32,
        ol_supply_w_id: i32,
        ol_delivery_d: i32,
        ol_quantity: Numeric, // Numeric(2,0)
        ol_amount: Numeric,   // Numeric(6, 2)
        ol_dist_info_str: String,
        ) -> Self 
    {
        let mut ol_dist_info : [u8;24] = Default::default();
        copy_from_string(&mut ol_dist_info, ol_dist_info_str);


        OrderLine {
            ol_o_id,
            ol_d_id,
            ol_w_id,
            ol_number,
            ol_i_id,
            ol_supply_w_id,
            ol_delivery_d,
            ol_quantity, // Numeric(2,0)
            ol_amount,   // Numeric(6, 2)
            ol_dist_info,
        }
    }
}

#[derive(Clone)]
pub struct Item {
    pub i_id: i32,
    pub i_im_id: i32,
    pub i_name: [u8; 24],
    pub i_price: Numeric, // Numeric(5,2)
    pub i_data: [u8; 50],
}

impl Debug for Item 
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "item ooo")
    }
}

impl Item {
    pub fn new(
        i_id: i32,
        i_im_id: i32,
        i_name_str: String,
        i_price: Numeric,
        i_data_str : String
    ) -> Item 
    {
        let mut i_name : [u8;24] = Default::default();
        copy_from_string(&mut i_name, i_name_str);
        let mut i_data : [u8;50] = [0;50];
        copy_from_string(&mut i_data, i_data_str);

        Item {
            i_id,
            i_im_id,
            i_name,
            i_price,
            i_data
        }
    }
}

impl Key<i32> for Item {
    #[inline(always)]
    fn primary_key(&self) -> i32 {
        self.i_id
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        (self.i_id) as usize
    }
}

#[derive(Clone)]
pub struct Stock {
    pub s_i_id: i32,
    pub s_w_id: i32,
    pub s_quantity: Numeric, // Numeric(4,0)
    pub s_dist_01: [u8; 24],
    pub s_dist_02: [u8; 24],
    pub s_dist_03: [u8; 24],
    pub s_dist_04: [u8; 24],
    pub s_dist_05: [u8; 24],
    pub s_dist_06: [u8; 24],
    pub s_dist_07: [u8; 24],
    pub s_dist_08: [u8; 24],
    pub s_dist_09: [u8; 24],
    pub s_dist_10: [u8; 24],
    pub s_ytd: Numeric,        // Numeric(8,0)
    pub s_order_cnt: Numeric,  // Numeric(4, 0)
    pub s_remote_cnt: Numeric, // Numeric(4,0)
    pub s_data: [u8; 50],
}

impl Debug for Stock 
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "stock")
    }
}

impl Key<(i32, i32)> for Stock {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
        (self.s_w_id, self.s_i_id)
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        self.s_w_id as usize
    }
}

impl Stock {
    pub fn new(
        s_i_id: i32,
        s_w_id: i32,
        s_quantity: Numeric, // Numeric(4,0)
        s_dist_01_str : String,
        s_dist_02_str : String,
        s_dist_03_str : String,
        s_dist_04_str : String,
        s_dist_05_str : String,
        s_dist_06_str : String,
        s_dist_07_str : String,
        s_dist_08_str : String,
        s_dist_09_str : String,
        s_dist_10_str : String,
        s_ytd: Numeric,        // Numeric(8,0)
        s_order_cnt: Numeric,  // Numeric(4, 0)
        s_remote_cnt: Numeric, // Numeric(4,0)
        s_data_str: String,
        ) -> Self 
    {

        let mut s_dist_01: [u8;24] = Default::default();
        let mut s_dist_02: [u8;24] = Default::default();
        let mut s_dist_03: [u8;24] = Default::default();
        let mut s_dist_04: [u8;24] = Default::default();
        let mut s_dist_05: [u8;24] = Default::default();
        let mut s_dist_06: [u8;24] = Default::default();
        let mut s_dist_07: [u8;24] = Default::default();
        let mut s_dist_08: [u8;24] = Default::default();
        let mut s_dist_09: [u8;24] = Default::default();
        let mut s_dist_10: [u8;24] = Default::default();
        let mut s_data : [u8;50] = [0; 50];
        copy_from_string(&mut s_dist_01, s_dist_01_str);
        copy_from_string(&mut s_dist_02, s_dist_02_str);
        copy_from_string(&mut s_dist_03, s_dist_03_str);
        copy_from_string(&mut s_dist_04, s_dist_04_str);
        copy_from_string(&mut s_dist_05, s_dist_05_str);
        copy_from_string(&mut s_dist_06, s_dist_06_str);
        copy_from_string(&mut s_dist_07, s_dist_07_str);
        copy_from_string(&mut s_dist_08, s_dist_08_str);
        copy_from_string(&mut s_dist_09, s_dist_09_str);
        copy_from_string(&mut s_dist_10, s_dist_10_str);
        copy_from_string(&mut s_data,    s_data_str);

        Stock {
            s_i_id,
            s_w_id,
            s_quantity, // Numeric(4,0)
            s_dist_01, 
            s_dist_02, 
            s_dist_03, 
            s_dist_04, 
            s_dist_05, 
            s_dist_06, 
            s_dist_07, 
            s_dist_08, 
            s_dist_09, 
            s_dist_10, 
            s_ytd    ,    // Numeric(8,0)
            s_order_cnt,  // Numeric(4, 0)
            s_remote_cnt, // Numeric(4,0)
            s_data,
        }
    }
}

#[derive(Clone, Debug)]
pub struct History {
    pub h_c_id: i32,
    pub h_c_d_id: i32,
    pub h_c_w_id: i32,
    pub h_d_id: i32,
    pub h_w_id: i32,
    pub h_date: i32,       //timestamp
    pub h_amount: Numeric, //Numeric(6,2)
    pub h_data: [u8; 24],
}

impl Key<(i32, i32)> for History {
    #[inline(always)]
    fn primary_key(&self) -> (i32, i32) {
       (self.h_w_id, self.h_d_id) 
    }

    #[inline(always)]
    fn bucket_key(&self) -> usize {
        let wh_num = num_warehouse_get();
        (self.h_w_id * wh_num + self.h_d_id) as usize
    }
}

impl History {
    pub fn new(
        h_c_id: i32,
        h_c_d_id: i32,
        h_c_w_id: i32,
        h_d_id: i32,
        h_w_id: i32,
        h_date: i32,       //timestamp
        h_amount: Numeric, //Numeric(6,2)
        h_data_str:String,
        )->Self 
    {

        let mut h_data : [u8;24] = Default::default();
        copy_from_string(&mut h_data, h_data_str);

        History {
            h_c_id,
            h_c_d_id,
            h_c_w_id,
            h_d_id,
            h_w_id,
            h_date,       //timestamp
            h_amount, //Numeric(6,2)
            h_data,
        }
    }

}
