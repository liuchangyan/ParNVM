#![feature(ptr_wrapping_offset_from)]

use std::mem;
extern crate rand;
extern crate pnvm_sys;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate libc;
extern crate config;


use pnvm_sys::*;
use std::{
    thread,
    sync::Barrier,
    sync::Arc,
    ptr,
};


fn main() {
    env_logger::init().unwrap();
    //multi_threads(1);
    let config = parse_config();


    multi_clwb(&config);
}

const PMEM_TEST_PATH_ABS: &str = "../data";

fn multi_clwb(config: &Config) {
    let bench = Arc::new(prep_bench(config));
    let barrier = Arc::new(Barrier::new(config.nthread));

    //Do Warm up 
    memset_persist(bench.pmem_addr, 0, bench.size);
    

    let mut opsps_avg = 0.0;
    for _j in 0..config.nrepeats {
        let mut handles = Vec::new();
        for i in 0..config.nthread {
            let builder = thread::Builder::new().name(format!("{}", i)); 
            let barrier = barrier.clone();
            let chunk_size = config.chunk_size;
            let nops = config.nops;
            let bench = bench.clone();


            let handle = builder.spawn(move || {
                barrier.wait();

                let start = Instant::now();

                /* Perform the benchmark for each thread */
                do_memcpy(bench, chunk_size, nops, i);

                let end = Instant::now();
                (start, end)
            }).unwrap();

            handles.push(handle);
        }

        let total_time = Duration::new(0, 0);
        let mut starts = vec![];
        let mut ends = vec![];
        for handle in handles {
            match handle.join() {
                Ok((thd_start, thd_end)) => {
                    starts.push(thd_start);
                    ends.push(thd_end);
                },
                Err(_) => panic!("thread panics"),
            }
        }


        /* Sort the times to get the min start and max end */
        starts.sort_unstable();
        ends.sort_unstable();

        let total_begin = starts[0];
        let total_end = ends[config.nthread-1];
        let dur = total_end.duration_since(total_begin);
        let total_ops = (config.nops * config.nthread) as f64;
        let dur_secs = (dur.as_secs() as f64 + dur.subsec_nanos() as f64 / 1_000_000_000.0) as f64;
        opsps_avg +=  total_ops/ dur_secs;
    }

    let bandwidth = opsps_avg/config.nrepeats as f64 *config.chunk_size as f64 / (1024 * 1024) as f64;
    println!("{},{},{},{},{}", config.mode,config.nthread, config.nops, config.chunk_size, bandwidth) ;

}

fn do_memcpy(bench: Arc<Bench>,  chunk_size: usize, nops: usize, thd_idx: usize) {
    for i in 0..nops {
        let offset = thd_idx * nops + bench.rand_offsets[i];
        let src = bench.src_addr.wrapping_add( offset * chunk_size);
        let dest = bench.pmem_addr.wrapping_add(offset * chunk_size);
        memcpy_nodrain(dest,src, chunk_size);
    }
}

fn prep_bench(config: &Config) -> Bench{ 
    let size = config.nthread * config.chunk_size * config.nops;
    let pmem_addr =  mmap_file(String::from(PMEM_TEST_PATH_ABS), size);

    let src_addr = Box::into_raw(vec!['x';size].into_boxed_slice()) as *mut u8;

    check_continuous(src_addr as *mut char, 'x', size);

    let n_rand_offsets = config.nthread * config.nops;

    let rand_offsets :Vec<usize> = (0..n_rand_offsets)
        .map(|_| {
            rand::random::<usize>() % config.nops
        }).collect();


   let bench =  Bench {
       pmem_addr,
       src_addr,
       rand_offsets,
       size,
    };

    //println!("Bench: {:?}", bench);
    bench
}

fn check_continuous(addr : *mut char, c: char, len:usize) {
    unsafe{
        for i in 0..len {
            assert_eq!(ptr::read(addr), c);
        }
    }
}

#[derive(Debug)]
struct Bench {
    pmem_addr: *mut u8,
    src_addr: *mut u8,
    rand_offsets: Vec<usize>,
    size: usize,
}

impl Drop for Bench {
    fn drop(&mut self) {
        /* Unmap the file */  
        unsafe { unmap(self.pmem_addr, self.size)};

        /* Box the src */
        let x = unsafe {Vec::from_raw_parts(self.src_addr, self.size, self.size)};
    }
}

unsafe impl Sync for Bench{}
unsafe impl Send for Bench{}


#[derive(Clone, Debug)]
struct Config {
    nthread: usize,
    chunk_size : usize,
    nops: usize,
    nrepeats : usize,
    mode : String,
}


fn parse_config() -> Config {
    
    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name("Settings"))
        .unwrap()
        .merge(config::Environment::with_prefix("BENCH"))
        .unwrap();


    Config {
        nthread : settings.get_int("THREAD_NUM").unwrap() as usize,
        chunk_size : settings.get_int("CHUNK_SIZE").unwrap() as usize,
        nops: settings.get_int("OPS_NUM").unwrap() as usize,
        nrepeats: settings.get_int("REPEAT_NUM").unwrap() as usize,
        mode : settings.get_str("MODE").unwrap(),
    }
}

#[repr(C)]
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

    pub fn field_offset(&self) -> [u8;32] {
        let mut fields : [u8;32] = [0;32];
        let base : *const u8 = self as *const _ as *const u8;
        fields[0] = (&self.c_id as *const _ as *const u8)
            .wrapping_offset_from(base) as u8;
        fields[1] = (&self.c_d_id as *const _ as *const u8)
            .wrapping_offset_from(base) as u8;

        fields
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


