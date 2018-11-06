use std::mem;
extern crate rand;
extern crate pnvm_sys;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate libc;
use pnvm_sys::*;
use std::{
    thread,
    sync::Barrier,
    sync::Arc,
};


fn main() {
    env_logger::init().unwrap();
    multi_threads(1);
}

const PMEM_TEST_PATH_ABS: &str = "../data";

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
    pub c_credit_lim: i32,   // i32(12,2)
    pub c_discount: i32,     // i32(4, 4)
    pub c_balance: i32,      // i32(12,2)
    pub c_ytd_payment: i32,  // i32(12,2)
    pub c_payment_cnt: i32,  // i32(4,0)
    pub c_delivery_cnt: i32, // i32(4,0)
    pub c_data: [u8; 500],
}

impl Customer {
    pub fn new( )-> Self {
        let  c_first : [u8;16] = Default::default();
        let  c_middle :[u8;2] = Default::default();
        let  c_last : [u8;16] = Default::default();
        let  c_street_1 : [u8;20] = Default::default();
        let  c_street_2 : [u8;20] = Default::default();
        let  c_city : [u8;20] = Default::default();
        let  c_state : [u8;2] = Default::default();
        let  c_zip : [u8;9] = Default::default();
        let  c_phone : [u8;16] = Default::default();
        let  c_credit : [u8;2] = Default::default();
        let  c_data : [u8;500] = [1 ; 500]; 

        let c_id = 0;
        let c_d_id =  1;
        let c_w_id = 1;
        let c_since = 1;
        let c_credit_lim = 1;
        let c_discount = 1;
        let c_balance = 1;
        let c_ytd_payment = 1;
        let c_payment_cnt = 1;
        let c_delivery_cnt = 1;


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
            c_credit_lim,   // i32(12,2)
            c_discount,     // i32(4, 4)
            c_balance,      // i32(12,2)
            c_ytd_payment,  // i32(12,2)
            c_payment_cnt,  // i32(4,0)
            c_delivery_cnt, // i32(4,0)
            c_data,
        }
    }

}

use std::time::{Duration, Instant};
// size in MB
fn single_write_dram(size: usize) {
    let mut counter = 0;
    let size =  size << 20;
    let pmem = mmap_file(String::from(PMEM_TEST_PATH_ABS), size);
    let dram_data = Box::into_raw(Box::new(Customer::new()));
    let offset_max =  size/ mem::size_of::<Customer>();
    let start = Instant::now();
    let total = 1000000;
    let cus_size = mem::size_of::<Customer>();
    //let offset = rand::random::<usize>() % offset_max; 
    let  mut prev = 0;
    unsafe{
        while counter < total {
            let paddr = pmem.offset((((prev+1000) % offset_max) * cus_size) as isize);
            prev = (prev+100) % offset_max;

            //flush_clwb(paddr as *const libc::c_void, cus_size);
           memcpy_nodrain(paddr, 
                          dram_data as *mut u8, 
                          cus_size);
            counter += 1;
        }
    }

    println!("write:counter : {}, time: {:?}",  counter, start.elapsed());
}


fn single_write_drain(size: usize) {
    let mut counter = 0;
    let size =  size <<20;
    let pmem = mmap_file(String::from(PMEM_TEST_PATH_ABS), size);
    let dram_data = Box::into_raw(Box::new(Customer::new()));
    let offset_max =  size/ mem::size_of::<Customer>();
    let start = Instant::now();
    let total = 10000000; 
    let cus_size = mem::size_of::<Customer>();
    //let offset = rand::random::<usize>() % offset_max; 
    let  mut prev = 0;
    unsafe{
        while counter < total {
            let paddr = pmem.offset((((prev+1000) % offset_max) * cus_size) as isize);
            prev = (prev+100) % offset_max;
            memcpy_persist(paddr, 
                           dram_data as *mut u8, 
                           cus_size);
            pmem_drain();
            counter += 1;
        }
    }
    println!("drain(): counter : {}, time: {:?}",  counter, start.elapsed());

}

fn pmem_direct(size: usize) {
    
}


fn multi_threads(thread_num : usize) {
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(thread_num));
    for i in 1..=thread_num {
        let builder = thread::Builder::new().name(format!("{}", i)); 
        let barrier = barrier.clone();
        let handle = builder.spawn(move || {
            barrier.wait();
            //single_write_drain(512);
            single_write_dram(128);
        }).unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
