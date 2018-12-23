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

use rand::{
    rngs::SmallRng,
    thread_rng,
    SeedableRng,
    Rng,
};


fn main() {
    env_logger::init().unwrap();
    //multi_threads(1);
    let config = parse_config();


    multi_clwb(&config);
    //rng_test(&config);
}

const PMEM_TEST_PATH_ABS: &str = "../data";
const DISK_TEST_PATH_ABS: &str = "../v-data";

fn rng_test(config: &Config) {
    let bench = Arc::new(prep_bench(config));
    let barrier = Arc::new(Barrier::new(config.nthread));

    let mut handles = Vec::new();
    for i in 0..config.nthread {
        let builder = thread::Builder::new().name(format!("{}", i)); 
        let barrier = barrier.clone();
    
        let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();

        let handle = builder.spawn(move || {
            barrier.wait();

            let start = Instant::now();

            for j in 1..10 {
                let k :u32 = rng.gen::<u32>();
                //println!("[T-{}] [{}] - [{}]", i, j, k);
            }
        }).unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }


}

fn memcp_dst_test(config: &Config) {
    let bench = Arc::new(prep_bench(config));

}

fn multi_clwb(config: &Config) {
    let bench = Arc::new(prep_bench(config));
    let barrier = Arc::new(Barrier::new(config.nthread));

    //Do Warm up 
    if !config.no_warmup{
        memset_persist(bench.dest_addr, 0, bench.size);
    }
    

    let mut opsps_avg = 0.0;
    for _j in 0..config.nrepeats {
        let mut handles = Vec::new();
        for i in 0..config.nthread {
            let builder = thread::Builder::new().name(format!("{}", i)); 
            let barrier = barrier.clone();
            let bench = bench.clone();


            let handle = builder.spawn(move || {
                barrier.wait();

                let start = Instant::now();

                let data : Vec<u8>= vec![i as u8;bench.chunk_size];

                /* Perform the benchmark for each thread */
                match bench.bench_type.as_ref() {
                    "Memcopy" => do_memcpy(&bench, i),
                    "FlushFreq" => do_flush_freq(&bench,i, data.as_slice()),
                    _ => panic!("unknown bench type"),

                }

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
    
    if config.print_header {
        println!("copy_mode,thd_block_size,nthread,nops,chunk_size,bandwidth,flush_freq, rd_after_w");
    }

    println!("{},{},{},{},{},{},{},{}", 
             config.mode,
             config.thd_block_size,
             config.nthread, 
             config.nops, 
             config.chunk_size, 
             bandwidth,
             config.flush_freq,
             config.rd_after_w,
             ) ;

}


#[inline]
fn get_copy_addr(bench: &Arc<Bench>, i :usize, thd :usize) 
    -> (*mut u8, *mut u8)
{
    let offset = thd * bench.thd_block_size + bench.rand_offsets[i] % bench.thd_block_size;
    let src = bench.src_addr.wrapping_add( offset * bench.chunk_size);
    let dest = bench.dest_addr.wrapping_add(offset * bench.chunk_size);

    (src, dest)

}

fn do_memcpy(bench: &Arc<Bench>, thd_idx: usize) {
    let chunk_size = bench.chunk_size;
    for i in 0..bench.nops {
        let (src, dest) = get_copy_addr(bench, i, thd_idx);

        match bench.copy_mode {
            BenchCopyMode::PMDKNoDrain(_) =>  memcpy_nodrain(dest,src, chunk_size),
            BenchCopyMode::Simple => unsafe {
                src.copy_to(dest, chunk_size)
            },
        }
    }
}

fn do_flush_freq(bench: &Arc<Bench>,  thd_idx: usize, data: &[u8]) 
{
    let mut cnt = 0;
    let chunk_size = bench.chunk_size;
    let mut records = Vec::with_capacity(bench.nops);

    for i in 0..bench.nops {
        let (src, dest) = get_copy_addr(bench,i, thd_idx);

        match bench.copy_mode {
            BenchCopyMode::PMDKNoDrain(_) => {
                unsafe {ptr::copy(data.as_ptr(), src, chunk_size)};
                records.push((src, dest));
                cnt += 1;

                if cnt == bench.flush_freq {
                    for j in i-cnt+1..=i {
                        let (src, dest) = records[i];
                        memcpy_nodrain(dest, src, chunk_size);
                        if bench.rd_after_w {
                            for i in 0..chunk_size {
                                unsafe {
                                    let x = dest.offset(i as isize).read();
                                    if (x < 0) {
                                        panic!("never here");
                                    }
                                }
                            }
                        }
                    }
                    //unsafe {pmem_drain()};
                    cnt = 0;
                }
            },

            BenchCopyMode::Simple => {
                unsafe {ptr::copy(data.as_ptr(), dest, chunk_size)};
                records.push((src, dest));
                cnt += 1;

                if cnt == bench.flush_freq {
                    for j in i-cnt+1..=i {
                        let (_src, dest) = records[i];
                        pnvm_sys::flush(dest, chunk_size);
                        if bench.rd_after_w {
                            for i in 0..chunk_size {
                                unsafe {
                                    let x = dest.offset(i as isize).read();
                                    if (x < 0) {
                                        panic!("never here");
                                    }
                                }
                            }
                        }
                    }
                    //unsafe {pmem_drain()};
                    cnt = 0;
                }
            }
        }

    }

}

fn prep_bench(config: &Config) -> Bench { 
    //let size = config.nthread * config.chunk_size * config.nops;
    let size= config.nthread * config.chunk_size * config.thd_block_size;
    
    let dest_addr =  match config.dest_mode.as_ref() {
        "MmapNVM"  => mmap_file(String::from(PMEM_TEST_PATH_ABS), size),
        "MmapDRAM"  => memmap::MmapMut::map_anon(size).unwrap().as_mut_ptr() as *mut u8,
        "MmapDisk" => mmap_file(String::from(DISK_TEST_PATH_ABS), size),
        "Heap" => Box::into_raw(vec!['y';size].into_boxed_slice()) as *mut u8,
        _ => panic!("unknown mode"),
    };

    let src_addr = Box::into_raw(vec!['x';size].into_boxed_slice()) as *mut u8;

    check_continuous(src_addr as *mut char, 'x', size);

    let n_rand_offsets = config.nthread * config.nops;

    let rand_offsets :Vec<usize> = (0..n_rand_offsets)
        .map(|_| {
            rand::random::<usize>() % config.nops
        }).collect();

    
    let copy_mode = match config.mode.as_ref() {
        "movnt-clflush" 
            | "movnt-empty" 
            | "movnt-clwb"
            | "movnt-clflushopt"
            | "mov-empty" 
            | "mov-clwb"
            | "mov-clflushopt"
            | "mov-clflush"
            => BenchCopyMode::PMDKNoDrain(config.mode.clone()),
        "simple" =>BenchCopyMode::Simple,
        _ => panic!("Unknown copy mode"),
    };

   let bench =  Bench {
       dest_addr,
       src_addr,
       rand_offsets,
       size,
       copy_mode,
       flush_freq: config.flush_freq,
       chunk_size: config.chunk_size,
       nops: config.nops,
       bench_type : config.bench_type.clone(),
       thd_block_size: config.thd_block_size,
       file_size : config.file_size,
       rd_after_w : config.rd_after_w,
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
    rd_after_w : bool,
    dest_addr: *mut u8,
    src_addr: *mut u8,
    rand_offsets: Vec<usize>,
    chunk_size: usize,
    file_size :usize,
    thd_block_size: usize,
    nops: usize,
    size: usize,
    copy_mode: BenchCopyMode,
    flush_freq: usize,
    bench_type: String,
}

impl Drop for Bench {
    fn drop(&mut self) {
        /* Unmap the file */  
        unsafe { unmap(self.dest_addr, self.size)};

        /* Box the src */
        let x = unsafe {Vec::from_raw_parts(self.src_addr, self.size, self.size)};
    }
}

unsafe impl Sync for Bench{}
unsafe impl Send for Bench{}

/* Bench config options */

#[derive(Clone, Debug)]
enum BenchCopyMode {
    Simple,
    PMDKNoDrain(String)
}


#[derive(Clone, Debug)]
struct Config {
    nthread: usize,
    chunk_size : usize,
    file_size : usize, // total file size UNUSED
    thd_block_size: usize, // each block copy region
    nops: usize,
    nrepeats : usize,
    mode : String,
    dest_mode : String, // mmapnvm or heap
    bench_type : String, // FlushFeq or Memcpy
    print_header : bool,
    rd_after_w: bool,
    no_warmup: bool,
    flush_freq: usize, 
}

#[derive(Copy, Clone)]
enum ConfigDestMode {
    MMapNVM,
    MMapDRAM,
    Heap,
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
        file_size : settings.get_int("FILE_SIZE").unwrap() as usize,
        thd_block_size: settings.get_int("THD_BLOCK_SIZE").unwrap() as usize,
        nops: settings.get_int("OPS_NUM").unwrap() as usize,
        nrepeats: settings.get_int("REPEAT_NUM").unwrap() as usize,
        mode : settings.get_str("PMDK_MODE").unwrap(),
        dest_mode : settings.get_str("DEST_MODE").unwrap(),
        bench_type: settings.get_str("BENCH_TYPE").unwrap(),
        flush_freq : settings.get_int("FLUSH_FREQ").unwrap() as usize,
        print_header: settings.get_bool("PRINT_HEADER").unwrap(),
        rd_after_w: settings.get_bool("READ_AFTER_WRITE").unwrap(),
        no_warmup: settings.get_bool("NO_WARMUP").unwrap(),
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


