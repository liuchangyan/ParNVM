#![feature(duration_extras)]
#![feature(alloc, raw_vec_internals)]
#![feature(generic_associated_types)]
#![feature(box_into_raw_non_null)]
#![feature(ptr_wrapping_offset_from)]
#![feature(no_more_cas)]

#![allow(dead_code, unused_imports, unused_variables)]


extern crate pnvm_lib;

#[cfg(any(feature = "pmem", feature ="disk"))]
extern crate pnvm_sys;

#[cfg(feature = "profile")]
extern crate flame;

extern crate config;
extern crate rand;
extern crate zipf;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate parking_lot;

extern crate alloc;
extern crate num;

extern crate core;

mod util;
mod tpcc;

use tpcc::*;



use util::*;

use rand::{
    rngs::SmallRng,
    thread_rng,
    SeedableRng,
    Rng,
};

use std::{
    fs::File,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread, time::{self, Duration, Instant},
};

use pnvm_lib::{
    occ::*,
    parnvm::nvm_txn_2pl::{TransactionPar},
    parnvm::nvm_txn_occ::{TransactionParOCC},
    tbox::*,
    tcore::*,
    txn::*,
    lock::*,
};

//#[cfg(feature = "pmem")]
//#[global_allocator]
//static GLOBAL: GPMem = GPMem;


//use std::alloc::{System, GlobalAlloc};
//#[global_allocator]
//static GLOBAL: System = System;

fn main() {
    env_logger::init().unwrap();

    let conf = util::read_env();
    warn!("{:?}", conf);

   // #[cfg(feature = "pmem")]
   // println!("PMEM on");
    
    
    #[cfg(feature = "pmem")]
    error!("has_auto_flush {}, \n has_hw_flush {} ", 
             pnvm_sys::has_auto_flush(),
             pnvm_sys::has_hw_drain());

    match conf.test_name.as_ref() {
        "OCC" => run_occ_micro(conf),
        "SINGLE" => run_single(conf),
        "PNVM_OCC" => run_nvm_occ_micro(conf),
        "TPCC_OCC" => run_tpcc(conf, TxnType::OCC),
        "TPCC_NVM" => run_pc_tpcc(conf, WorkloadType::Full),
        "NO_2PL" => run_tpcc(conf, TxnType::Lock),
        "NO_NVM" => run_pc_tpcc(conf, WorkloadType::NewOrder),
        "MICRO_2PL" => run_micro_2pl(conf),
        _ => panic!("unknown test name"),
    }
}

fn run_micro_2pl(conf: Config) {
    //Prepare object pools
    let values : Vec<Arc<TBox<u32>>> = (0..conf.obj_num as u32)
        .map(|x| TBox::new(x))
        .collect();

    let keys = util::generate_data(&conf);

    let barrier = Arc::new(Barrier::new(conf.thread_num));
    let mut handles = vec![];

    for i in 1..=conf.thread_num {
        let builder = thread::Builder::new().name(format!("TXN-{}", i + 1));
        let conf = conf.clone();
        let barrier = barrier.clone();
        let values = values.clone();
        let duration_in_secs = conf.duration;
        let thd_keys = keys[i-1].clone();

        handles.push(
            builder
            .spawn(move || {
                let duration = Duration::new(duration_in_secs, 0);
                TidFac::set_thd_mask(i as u32);
                OidFac::set_obj_mask(i as u64);

                BenchmarkCounter::start();

                let mut counter = 0;
                let start = Instant::now();
                let mut elapsed = Duration::default();

                while elapsed < duration {
                    elapsed = start.elapsed();
                    let read_keys = util::zipf_keys(conf.set_size, conf.obj_num, conf.zipf_coeff);
                    let write_keys = util::zipf_keys(conf.set_size, conf.obj_num, conf.zipf_coeff);
                    let tid = TidFac::get_thd_next();

                    'work: loop {
                        let tx = &mut lock_txn::Transaction2PL::new(tid);

                        /* Read Lock*/
                        let mut read_trefs = vec![];
                        for k in read_keys.iter(){
                            let tbox = &values[*k as usize];
                            let tref = tbox.clone().into_box_ref();
                            match tx.read_lock_tref(&tref) {
                                Ok(_) => {
                                    read_trefs.push(tref);
                                },
                                Err(_) => {
                                    tx.abort();
                                    continue 'work;
                                }
                            }
                        }

                        /* Write Lock*/
                        let mut write_trefs = vec![];
                        for k in write_keys.iter(){
                            let tbox = &values[*k as usize];
                            let tref = tbox.clone().into_box_ref();
                            match tx.write_lock_tref(&tref) {
                                Ok(_) => {
                                    write_trefs.push(tref);
                                },
                                Err(_) => {
                                    tx.abort();
                                    continue 'work;
                                }
                            }
                        }
                        
                        let tid :u32 = tid.into();
                        for tref in read_trefs.iter() {
                            let v  = tx.read::<u32>(&tref);
                            info!("Read {} : {}", tid, v);
                        }

                        for tref in write_trefs.iter() {
                            let oid = tref.get_id();
                            tx.write::<u32>(&tref, tid);
                            info!("Write {:?} : {} ", oid, tid);
                        }

                        tx.commit();
                        break;
                    }
                }
                BenchmarkCounter::copy()
            }).unwrap());
    }

    report_stat(handles, conf);
}

//fn run_nvm(conf: Config) {
//    let workload = util::TestHelper::prepare_workload_nvm(&conf);
//    let work = workload.work_;
//    let mut handles = Vec::new();
//    let barrier = Arc::new(Barrier::new(conf.thread_num));
//
//    let atomic_cnt = Arc::new(AtomicUsize::new(1));
//    let mut prep_time = time::Duration::new(0, 0);
//
//
//    #[cfg(feature = "profile")]
//    flame::start("benchmark");
//
//    for i in 0..conf.thread_num {
//        /* Per thread preparation */
//        let conf = conf.clone();
//        let barrier = barrier.clone();
//        let thread_txn_base = work[i].clone();
//        let builder = thread::Builder::new().name(format!("TXN-{}", i + 1));
//        let atomic_clone = atomic_cnt.clone();
//
//        let handle = builder
//            .spawn(move || {
//                TidFac::set_thd_mask(i as u32);
//                barrier.wait();
//                BenchmarkCounter::start();
//
//                for _ in 0..conf.round_num {
//                    /* Get tid */
//                    let tid = TidFac::get_thd_next();
//
//                    #[cfg(feature = "profile")]
//                    {
//                        flame::start(format!("start_txn - {:?}", tid));
//                    }
//
//                    let mut tx = TransactionPar::new_from_base(&thread_txn_base, tid);
//
//                    TransactionPar::register(tx);
//                    TransactionPar::execute();
//
//                    #[cfg(feature = "profile")]
//                    {
//                        flame::end(format!("start_txn - {:?}", tid));
//                    }
//                }
//
//                BenchmarkCounter::copy()
//            })
//        .unwrap();
//
//        handles.push(handle);
//    }
//    let thd_num = conf.thread_num;
//
//    report_stat(handles, conf);
//
//    #[cfg(feature = "profile")]
//    {
//        flame::end("benchmark");
//        let mut f = File::create(format!("profile/nvm.profile.{}", thd_num).as_str()).unwrap();
//        flame::dump_text_to_writer(f);
//    }
//}
//
//
//

fn run_nvm_occ_micro(conf: Config) {
    #[cfg(all(feature = "pmem",any(feature = "wdrain", feature = "dir")))]
    PmemFac::init();
    let workload = util::TestHelper::prepare_workload_nvm_occ(&conf);
    let work = workload.work_;
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let prep_time = time::Duration::new(0, 0);

    let warm_up_time = conf.warmup_time;
    let mut no_warmup = conf.no_warmup;



    #[cfg(feature = "profile")]
    flame::start("benchmark");

    for i in 0..conf.thread_num {
        /* Per thread preparation */
        let conf = conf.clone();
        let barrier = barrier.clone();
        let thread_txn_base = work[i].clone();
        let builder = thread::Builder::new().name(format!("TXN-{}", i + 1));
        let atomic_clone = atomic_cnt.clone();

        let duration_in_secs = conf.duration;

        let handle = builder
            .spawn(move || {
                //Thread-local setup
                TidFac::set_thd_mask(i as u32);
                
                #[cfg(all(feature = "pmem",any(feature = "wdrain", feature = "dir")))]
                PmemFac::init();

                barrier.wait();
                let duration = Duration::new(duration_in_secs, 0);
                let mut start = Instant::now();
                let mut elapsed = Duration::default();
                let mut prev_ts = 0;
                BenchmarkCounter::start();


                while elapsed < duration {
                    elapsed = start.elapsed();

                    if !no_warmup {
                        if elapsed.as_secs() == warm_up_time {
                            no_warmup = true;
                            BenchmarkCounter::reset_cnt();
                            start = Instant::now();
                            elapsed = start.elapsed();
                            prev_ts = 0;
                        }
                    }

                    if elapsed.as_secs() == prev_ts + 2 {
                        BenchmarkCounter::timestamp();
                        prev_ts = elapsed.as_secs();
                    }


                    /* Get tid */
                    let tid = TidFac::get_thd_next();

                    #[cfg(feature = "profile")]
                    {
                        flame::start(format!("start_txn - {:?}", tid));
                    }

                    let mut tx = TransactionParOCC::new_from_base(&thread_txn_base, tid, Box::new(1));

                    tx.execute_txn();

                    #[cfg(feature = "profile")]
                    {
                        flame::end(format!("start_txn - {:?}", tid));
                    }
                }

                BenchmarkCounter::copy()
            })
        .unwrap();

        handles.push(handle);
    }
    let thd_num = conf.thread_num;

    report_stat(handles, conf);

    #[cfg(feature = "profile")]
    {
        flame::end("benchmark");
        let mut f = File::create(format!("profile/nvm.profile.{}", thd_num).as_str()).unwrap();
        flame::dump_text_to_writer(f);
    }

}

fn run_occ_micro(conf: Config) {
    let mtx = Arc::new(Mutex::new(0));
    let dataset = util::TestHelper::prepare_workload_occ(&conf).get_dataset();
    let keys = dataset.keys;
    let maps = dataset.maps;

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    #[cfg(feature = "profile")]
    flame::start("benchmark_start");

    for i in 0..conf.thread_num {
        let conf = conf.clone();
        let atomic_clone = atomic_cnt.clone();
        let barrier = barrier.clone();
        let builder = thread::Builder::new().name(format!("TID-{}", i + 1));
        let keys = keys[i].clone();
        let maps = maps.clone();
        let mtx = mtx.clone();

        let handle = builder
            .spawn(move || {

                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                PmemFac::init();

                TidFac::set_thd_mask(i as u32);
                barrier.wait();
                BenchmarkCounter::start();

                for _ in 0..conf.round_num {
                    let tid = TidFac::get_thd_next();

                    let tx = &mut occ_txn::TransactionOCC::new(tid);
                    let tid = tid.clone();
                    #[cfg(feature = "profile")]
                    {
                        flame::start(format!("start_txn - {:?}", tid));
                    }

                    while {
                        #[cfg(feature = "profile")]
                        {
                            flame::start("clone keys");
                        }
                        let read_keys = keys.read_keys.clone();
                        let write_keys = keys.write_keys.clone();

                        #[cfg(feature = "profile")]
                        {
                            flame::end("clone keys");
                        }


                        #[cfg(feature = "profile")]
                        {
                            flame::start("data");
                        }
                        for map in maps.iter() {
                            for read in read_keys.iter() {
                                let id = tx.id();
                                let tref = map.get(&read).unwrap().get();
                                //let val = tx.read(&tobj);
                                let val = tx.read::<u32>(tref.clone().into_box_ref());
                                debug!("[{:?}] Read {:?}", id , val);
                            }

                            for write in write_keys.iter() {
                                let tref = map.get(&write).unwrap().get();
                                let val :u32 = tx.id().into();
                                debug!("[{:?}] Write {:?}", tx.id(), val);
                                tx.write(tref.clone().into_box_ref(), val);
                            }
                        }
                        #[cfg(feature = "profile")]
                        {
                            flame::end("data");
                        }

                        let res = tx.try_commit();
                        !res
                    } {}

                    info!("[THREAD {:} - TXN {:?}] COMMITS", i + 1, tid);

                    #[cfg(feature = "profile")]
                    {
                        flame::end(format!("start_txn - {:?}", tid));
                    }
                }

                BenchmarkCounter::copy()
            })
        .unwrap();

        handles.push(handle);
    }

    let thd_num :usize = conf.thread_num;
    report_stat(handles, conf);

    #[cfg(feature = "profile")]
    {
        flame::end("benchmark_start");
        let mut f = File::create(format!("profile/occ.profile.{}", thd_num).as_str()).unwrap();
        flame::dump_text_to_writer(f);
    }
}


//Running of TPCC with PPNVM piece contention management
fn run_pc_tpcc(conf: Config, kind: WorkloadType) {
    let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
    //FIXME: rename the function, parepare workload
    let tables = tpcc::workload_common::prepare_workload(&conf, &mut rng);

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    #[cfg(feature = "profile")]
    flame::start("benchmark_start");

    for i in 1..=conf.thread_num {
        let conf = conf.clone();
        let atomic_clone = atomic_cnt.clone();
        let barrier = barrier.clone();
        let builder = thread::Builder::new().name(format!("TID-{}", i + 1));
        let tables = tables.clone();
        let duration_in_secs = conf.duration;
        let wh_num = conf.wh_num;
        let d_num = conf.d_num;

        let mut no_warmup = conf.no_warmup;
        let warm_up_time = conf.warmup_time;

        /* Spawn worker thread */
        let handle = builder
            .spawn(move || {
                TidFac::set_thd_mask(i as u32);
                OidFac::set_obj_mask(i as u64);
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                PmemFac::init();


                tpcc::workload_common::num_warehouse_set(wh_num);
                tpcc::workload_common::num_district_set(d_num);

                let duration = Duration::new(duration_in_secs, 0);
                let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
                
                let w_home = (i as i32 )% wh_num +1;
                let d_home = (i as i32) % d_num + 1;
                let new_order_base = tpcc::workload_ppnvm::pc_new_order_base(&tables);
                let payment_base = tpcc::workload_ppnvm::pc_payment_base(&tables);
                let orderstatus_base = tpcc::workload_ppnvm::pc_orderstatus_base(&tables);

                let o_carrier_id :i32 = rng.gen::<i32>() % 10 + 1;
                let delivery_base = tpcc::workload_ppnvm::pc_delivery_base(&tables, w_home, o_carrier_id);

                let thd = tpcc::numeric::Numeric::new(rng.gen_range(10, 21), 2, 0);
                let stocklevel_base = tpcc::workload_ppnvm::pc_stocklevel_base(&tables, w_home, d_home, thd);
                
                let get_time = util_get_avg_get_time();
                barrier.wait();

                let mut start = Instant::now();
                BenchmarkCounter::set_get_time(get_time);
                BenchmarkCounter::start();
                let mut elapsed  = Duration::default();
                let mut prev_timestamp = 0;

                
                /* Run the workload */
                while elapsed < duration {
                    elapsed = start.elapsed();
                    let tid = TidFac::get_thd_next();
                    let j : u32= rng.gen::<u32>() % 100;
                    
                    if !no_warmup {
                        if elapsed.as_secs() == warm_up_time {
                            no_warmup = true;
                            BenchmarkCounter::reset_cnt();
                            start = Instant::now();
                            elapsed = start.elapsed();
                            prev_timestamp = 0;
                        }
                    }

                    if elapsed.as_secs() == prev_timestamp + 2 {
                        BenchmarkCounter::timestamp();
                        prev_timestamp = elapsed.as_secs();
                    }

                    
                    //FIXME: pass by ref rather than box it
                    match kind {
                        WorkloadType::Full => {
                            let mut tx = match j {
                                12...55 => {
                                    let inputs = tpcc::workload_ppnvm::pc_new_order_input(w_home, &mut rng);
                                    TransactionParOCC::new_from_base(&new_order_base, tid, Box::new(inputs))
                                },
                                0...4 => {
                                    let inputs = tpcc::workload_ppnvm::pc_orderstatus_input(w_home, &mut rng);
                                    TransactionParOCC::new_from_base(&orderstatus_base, tid, Box::new(inputs))
                                },
                                4...8 => {
                                    let inputs = tpcc::workload_ppnvm::pc_delivery_input(w_home, &mut rng);
                                    TransactionParOCC::new_from_base(&delivery_base, tid, Box::new(inputs))
                                },
                                8...12 => {
                                    TransactionParOCC::new_from_base(&stocklevel_base, tid, Box::new(-1))
                                },
                                55...100 => {
                                    let inputs = tpcc::workload_ppnvm::pc_payment_input(w_home, &mut rng);
                                    TransactionParOCC::new_from_base(&payment_base, tid, Box::new(inputs))
                                },
                                _ => panic!("invalid tx mix")
                            };

                            //#[cfg(all(feature = "pmem", feature = "pdrain"))]
                            //match j {
                            //    12...55 => {

                            //        tpcc::workload_ppnvm::pc_new_order_stock_pc(tables.clone(), &mut tx);

                            //    },
                            //    _ => {},
                            //}
                            
                            tx.execute_txn();
                        },
                        WorkloadType::NewOrder => {
                            let mut tx ={
                                let inputs = tpcc::workload_ppnvm::pc_new_order_input(w_home, &mut rng);
                                TransactionParOCC::new_from_base(&new_order_base, tid, Box::new(inputs))
                            };

                            //#[cfg(all(feature = "pmem", feature = "pdrain"))]
                            //{

                            //    tpcc::workload_ppnvm::pc_new_order_stock_pc(tables.clone(), &mut tx);
                            //}

                            tx.execute_txn();
                        },
                
                    }
                    info!("[THREAD {:} - TXN {:?}] COMMITS", i + 1, tid);
                }

                BenchmarkCounter::copy()
            })
        .unwrap();

        handles.push(handle);
    }

    let thd_num :usize = conf.thread_num;
    report_stat(handles, conf);


    #[cfg(feature = "profile")]
    {
        flame::end("benchmark_start");
        let mut f = File::create(format!("profile/occ.profile.{}", thd_num).as_str()).unwrap();
        flame::dump_text_to_writer(f);
    }
}

//Run the OCC contention management TPCC workload
fn run_tpcc(conf: Config, txn_type: TxnType) {
    let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
    let tables = tpcc::workload_common::prepare_workload(&conf, &mut rng);

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    #[cfg(feature = "profile")]
    flame::start("benchmark_start");

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    PmemFac::init();

    for i in 1..=conf.thread_num {
        let conf = conf.clone();
        let atomic_clone = atomic_cnt.clone();
        let barrier = barrier.clone();
        let builder = thread::Builder::new().name(format!("TID-{}", i + 1));
        let tables = tables.clone();

        //Configuration
        let duration_in_secs = conf.duration;
        let wh_num = conf.wh_num;
        let d_num = conf.d_num;
        let mut no_warmup = conf.no_warmup;
        let warm_up_time = conf.warmup_time;



        let handle = builder
            .spawn(move || {
                /* Thread local initialization */
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                PmemFac::init();

                TidFac::set_thd_mask(i as u32);
                OidFac::set_obj_mask(i as u64);

                tpcc::workload_common::num_warehouse_set(wh_num);
                tpcc::workload_common::num_district_set(d_num);
                let duration = Duration::new(duration_in_secs, 0);
                let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
                let w_home = (i as i32 )% wh_num +1;
                let d_home = (i as i32) % d_num + 1;

                let get_time = util_get_avg_get_time();
                barrier.wait();

                let mut start = Instant::now();
                BenchmarkCounter::set_get_time(get_time);
                BenchmarkCounter::start();
                let mut elapsed  = Duration::default();
                let mut prev_timestamp = 0;
                //for j in 0..conf.round_num {
               

                //Warm up for each thread
                
                while elapsed < duration {
                    elapsed = start.elapsed();

                    //Warm up for x seconds
                    if !no_warmup {
                        if elapsed.as_secs() == warm_up_time {
                            no_warmup = true;
                            BenchmarkCounter::reset_cnt();
                            start = Instant::now();
                            elapsed = start.elapsed();
                            prev_timestamp = 0;
                            //println!("Warm up done");
                        }
                    }

                    if elapsed.as_secs() == prev_timestamp + 2 {
                        BenchmarkCounter::timestamp();
                        prev_timestamp = elapsed.as_secs();
                    }

                    BenchmarkCounter::get_time();
                    let tid = TidFac::get_thd_next();
                    let j : u32= rng.gen::<u32>() % 100;
                    let mut abort_cnt = 0;

                    match txn_type {
                        TxnType::OCC => {
                            let tid = tid.clone();
                            let tx = &mut occ_txn::TransactionOCC::new(tid);
                            while {
                                info!("\n------------------TXN[{:?} Starts-----------------\n", tid);
                                if j > 55 {
                                    tpcc::workload_occ::new_order_random(tx, &tables, w_home,  &mut rng);
                                } else if j < 4 {
                                    tpcc::workload_occ::orderstatus_random(tx, &tables, w_home, &mut rng);
                                } else if j < 8  {
                                    let o_carrier_id :i32 = rng.gen::<i32>() % 10 + 1;
                                    tpcc::workload_occ::delivery(tx, &tables, w_home, o_carrier_id);
                                } else if j < 12 {
                                    let thd = tpcc::numeric::Numeric::new(rng.gen_range(10, 21), 2, 0);
                                    tpcc::workload_occ::stocklevel(tx, &tables, w_home, d_home, thd);
                                }
                                else{
                                    tpcc::workload_occ::payment_random(tx, &tables,w_home  ,  &mut rng);
                                }

                                let res = tx.try_commit();

                                if res && j > 55  {
                                    BenchmarkCounter::new_order_done();
                                }

                                !res
                            } {}
                        },
                        TxnType::Lock => {
                            let tid = tid.clone();
                            let tx = &mut lock_txn::Transaction2PL::new(tid);
                            while {
                                info!("\n------------------TXN[{:?} Starts-----------------\n", tid);
                                let res = tpcc::workload_2pl::new_order_random(tx, &tables, w_home, &mut rng);
                                //let res = if j > 55 {
                                //    tpcc::workload_2pl::new_order_random(tx, &tables, w_home,  &mut rng)
                                //} else if j < 4 {
                                //    tpcc::workload_2pl::orderstatus_random(tx, &tables, w_home, &mut rng)
                                //} else if j < 8  {
                                //    let o_carrier_id :i32 = rng.gen::<i32>() % 10 + 1;
                                //    tpcc::workload_2pl::delivery(tx, &tables, w_home, o_carrier_id)
                                //} else if j < 12 {
                                //    let thd = tpcc::numeric::Numeric::new(rng.gen_range(10, 21), 2, 0);
                                //    tpcc::workload_2pl::stocklevel(tx, &tables, w_home, d_home, thd)
                                //}
                                //else{
                                //    tpcc::workload_2pl::payment_random(tx, &tables,w_home  ,  &mut rng)
                                //};

                                if res.is_ok() {
                                    tx.commit();
                                    warn!("[THREAD {:} - TXN {:?}] COMMITS", i , tid);
                                    false
                                } else {
                                    tx.abort();
                                    warn!("[THREAD {:} - TXN {:?}] ABORTS ", i , tid);
                                    true
                                }
                            } {}
                        }
                    }

                    info!("[THREAD {:} - TXN {:?}] COMMITS", i + 1, tid);
                }

                BenchmarkCounter::copy()
            })
        .unwrap();

        handles.push(handle);
    }

    let thd_num :usize = conf.thread_num;
    report_stat(handles, conf);

    #[cfg(feature = "profile")]
    {
        flame::end("benchmark_start");
        
        let mut f = File::create(format!("profile/occ.profile.{}", thd_num).as_str()).unwrap();
        flame::dump_text_to_writer(f);
    }
}

fn run_single(conf : Config) {
    
    let mut handles = vec![];

    let start = time::Instant::now();
    let data = util::TestHelper::prepare_workload_single(&conf);
    let keys = data.keys;
    let maps = data.maps;

    for i in 0..conf.thread_num {
        let _prep_start = time::Instant::now();
        let conf = conf.clone();
        let builder = thread::Builder::new().name(format!("TID-{}", i + 1));

        //Use OCC's workload

        let read_keys = keys.read_keys.clone();
        let write_keys = keys.write_keys.clone();
        let maps = maps.clone();

        let handle = builder
            .spawn(move || {
                for _ in 0..conf.round_num {
                    for map in maps.iter() {
                        for read in read_keys.iter() {
                            #[cfg(feature = "profile")]
                            {
                                flame::start("map::get");
                            }
                            let tobj = map.get(&read).unwrap().read();
                            #[cfg(feature = "profile")]
                            {
                                flame::end("map::get");
                            }
                            let val = **tobj;
                        }

                        for write in write_keys.iter() {
                            #[cfg(feature = "profile")]
                            {
                                flame::start("map::get");
                            }
                            let mut tobj = map.get(&write).unwrap().write();
                            #[cfg(feature = "profile")]
                            {
                                flame::end("map::get");
                            }
                            **tobj = *write;
                        }
                    }
                }
            }).unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_time = start.elapsed();
    println!(
        "{},{},{}, {}, {},{:?}",
        conf.thread_num,
        conf.obj_num,
        conf.set_size,
        conf.zipf_coeff,
        conf.pc_num,
        total_time.as_secs() as u32 * 1000 + total_time.subsec_millis(),
        );

}

fn report_stat(
    handles: Vec<thread::JoinHandle<BenchmarkCounter>>,
    conf: Config,
    ) {
    let mut total_abort = 0;
    let mut total_success = 0;
    let mut total_pc_abort = 0;
    let mut total_pc_success = 0;
    let mut total_time = time::Duration::new(0, 0);
    let mut total_mmap_cnt = 0;
    let mut total_flush = 0;
    let mut total_log = 0;
    let mut total_timestamps = vec![0;17];
    

    let mut total_new_order = 0; 

    for handle in handles {
        //#[cfg(benchmark)]
        match handle.join() {
            Ok(per_thd) => {
                total_success += per_thd.success_cnt;
                total_abort += per_thd.abort_cnt;
                total_pc_abort += per_thd.abort_piece_cnt;
                total_pc_success += per_thd.success_piece_cnt;
                total_new_order += per_thd.new_order_cnt;
                total_mmap_cnt += per_thd.mmap_cnt;
                total_flush += per_thd.pmem_flush_size;
                total_log += per_thd.pmem_log_size;
                total_time = std::cmp::max(total_time, per_thd.duration - per_thd.avg_get_time * per_thd.get_time_cnt);
                for i in 0..per_thd.success_over_time.len() {
                    total_timestamps[i+1] += per_thd.success_over_time[i];
                }
            }
            Err(_) => warn!("thread panics"),
        }
    }


    println!(
        "{}, {}, {},{},{}, {},{},{:?},{},{}",
        conf.thread_num,
        conf.wh_num,
        total_success,
        total_abort,
        total_pc_success,
        total_pc_abort,
        total_mmap_cnt,
        total_time.as_secs() as u32 * 1000 + total_time.subsec_millis(),
        total_log / 1024 / 1024  / total_time.as_secs() as u32,
        total_flush / 1024 / 1024  / total_time.as_secs() as u32
        );
    
    for i in (1..total_timestamps.len()).rev() {
        if total_timestamps[i] > total_timestamps[i-1] {
            total_timestamps[i] -= total_timestamps[i-1]; 
        }
    }
    //println!("{:?}", total_timestamps);
    //
    //let total_time =  start.elapsed() - spin_time;
   // println!(
   //     "{},{},{}, {}, {}, {}, {}, {:?}, {}",
   //     conf.thread_num,
   //     conf.obj_num,
   //     conf.set_size,
   //     conf.zipf_coeff,
   //     conf.pc_num,
   //     total_success,
   //     total_abort,
   //     total_time.as_secs() as u32 * 1000 + total_time.subsec_millis(),
   //     total_new_order,
   //     )
}

#[derive(Copy, Clone)]
enum TxnType{
    OCC,
    Lock,
}

#[derive(Copy, Clone)]
enum WorkloadType {
    Full,
    NewOrder,
}
