#![feature(duration_extras, global_allocator)]
#![feature(alloc, raw_vec_internals)]
#![feature(generic_associated_types)]
#![feature(box_into_raw_non_null)]

extern crate pnvm_lib;
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
    parnvm::nvm_txn::{TransactionPar, TransactionParOCC},
    tbox::*,
    tcore::*,
    txn::*,
};

#[cfg(feature = "pmem")]
#[global_allocator]
static GLOBAL: GPMem = GPMem;


//use std::alloc::{System, GlobalAlloc};
//#[global_allocator]
//static GLOBAL: System = System;

fn main() {
    env_logger::init().unwrap();

    let conf = util::read_env();
    warn!("{:?}", conf);
    match conf.test_name.as_ref() {
        "OCC" => run_occ(conf),
//        "PNVM" => run_nvm(conf),
        "SINGLE" => run_single(conf),
        "PNVM_OCC" => run_nvm_occ(conf),
        "TPCC_OCC" => run_occ_tpcc(conf),
        "TPCC_NVM" => run_pc_tpcc(conf),
        _ => panic!("unknown test name"),
    }
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
fn run_nvm_occ(conf: Config) {
    let workload = util::TestHelper::prepare_workload_nvm_occ(&conf);
    let work = workload.work_;
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut prep_time = time::Duration::new(0, 0);


    #[cfg(feature = "profile")]
    flame::start("benchmark");

    for i in 0..conf.thread_num {
        /* Per thread preparation */
        let conf = conf.clone();
        let barrier = barrier.clone();
        let thread_txn_base = work[i].clone();
        let builder = thread::Builder::new().name(format!("TXN-{}", i + 1));
        let atomic_clone = atomic_cnt.clone();

        let handle = builder
            .spawn(move || {
                TidFac::set_thd_mask(i as u32);
                barrier.wait();
                BenchmarkCounter::start();

                for _ in 0..conf.round_num {
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

fn run_occ(conf: Config) {
    let mtx = Arc::new(Mutex::new(0));
    let mut dataset = util::TestHelper::prepare_workload_occ(&conf).get_dataset();
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


fn run_pc_tpcc(conf: Config) {
    let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
    //FIXME: rename the function, parepare workload
    let tables = tpcc::workload::prepare_workload(&conf, &mut rng);

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


        let handle = builder
            .spawn(move || {
                TidFac::set_thd_mask(i as u32);
                OidFac::set_obj_mask(i as u64);
                tpcc::workload::num_warehouse_set(wh_num);
                tpcc::workload::num_district_set(d_num);

                let duration = Duration::new(duration_in_secs, 0);
                let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
                barrier.wait();
                
                let w_home = (i as i32 )% wh_num +1;
                let d_home = (i as i32) % d_num + 1;
                let new_order_base = tpcc::pc_gen::pc_new_order_base(&tables);
                let payment_base = tpcc::pc_gen::pc_payment_base(&tables);
                let orderstatus_base = tpcc::pc_gen::pc_orderstatus_base(&tables);

                let o_carrier_id :i32 = rng.gen::<i32>() % 10 + 1;
                let delivery_base = tpcc::pc_gen::pc_delivery_base(&tables, w_home, o_carrier_id);

                let thd = tpcc::numeric::Numeric::new(rng.gen_range(10, 21), 2, 0);
                let stocklevel_base = tpcc::pc_gen::pc_stocklevel_base(&tables, w_home, d_home, thd);
                
                let start = Instant::now();
                BenchmarkCounter::start();

                //for j in 0..conf.round_num {
                while start.elapsed() < duration {
                    let tid = TidFac::get_thd_next();
                    let j : u32= rng.gen::<u32>() % 100;
                    
                    //FIXME: pass by ref rather than box it
                    let mut tx = match j {
                        12...55 => {
                            let inputs = tpcc::pc_gen::pc_new_order_input(w_home, &mut rng);
                            TransactionParOCC::new_from_base(&new_order_base, tid, Box::new(inputs))
                        },
                        0...4 => {
                            let inputs = tpcc::pc_gen::pc_orderstatus_input(w_home, &mut rng);
                            TransactionParOCC::new_from_base(&orderstatus_base, tid, Box::new(inputs))
                        },
                        4...8 => {
                            let inputs = tpcc::pc_gen::pc_delivery_input(w_home, &mut rng);
                            TransactionParOCC::new_from_base(&delivery_base, tid, Box::new(inputs))
                        },
                        8...12 => {
                            TransactionParOCC::new_from_base(&stocklevel_base, tid, Box::new(-1))
                        },
                        55...100 => {
                            let inputs = tpcc::pc_gen::pc_payment_input(w_home, &mut rng);
                            TransactionParOCC::new_from_base(&payment_base, tid, Box::new(inputs))
                        },
                        _ => panic!("invalid tx mix")
                    };

                    tx.execute_txn();
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
fn run_occ_tpcc(conf: Config) {
    let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
    let tables = tpcc::workload::prepare_workload(&conf, &mut rng);

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


        let handle = builder
            .spawn(move || {
                TidFac::set_thd_mask(i as u32);
                OidFac::set_obj_mask(i as u64);
                tpcc::workload::num_warehouse_set(wh_num);
                tpcc::workload::num_district_set(d_num);
                let duration = Duration::new(duration_in_secs, 0);
                let mut rng = SmallRng::from_rng(&mut thread_rng()).unwrap();
                barrier.wait();
                let start = Instant::now();
                BenchmarkCounter::start();
                let w_home = (i as i32 )% wh_num +1;
                let d_home = (i as i32) % d_num + 1;

                //for j in 0..conf.round_num {
                while start.elapsed() < duration {
                    let tid = TidFac::get_thd_next();
                    let tx = &mut occ_txn::TransactionOCC::new(tid);
                    let tid = tid.clone();
                    let j : u32= rng.gen::<u32>() % 100;



                    while {
                        info!("\n------------------TXN[{:?} Starts-----------------\n", tid);
                        if j > 55 {
                            tpcc::workload::new_order_random(tx, &tables, w_home,  &mut rng);
                        } else if j < 4 {
                            tpcc::workload::orderstatus_random(tx, &tables, w_home, &mut rng);
                        } else if j < 8  {
                            let o_carrier_id :i32 = rng.gen::<i32>() % 10 + 1;
                            tpcc::workload::delivery(tx, &tables, w_home, o_carrier_id);
                        } else if j < 12 {
                            let thd = tpcc::numeric::Numeric::new(rng.gen_range(10, 21), 2, 0);
                            tpcc::workload::stocklevel(tx, &tables, w_home, d_home, thd);
                        }
                        else{
                            tpcc::workload::payment_random(tx, &tables,w_home  ,  &mut rng);
                        }

                        let res = tx.try_commit();
                        
                        if res && j % 2 == 0 {
                            BenchmarkCounter::new_order_done();
                        }

                        !res
                    } {}

                    info!("[THREAD {:} - TXN {:?}] COMMITS", i + 1, tid);
                }

                BenchmarkCounter::copy()
            })
        .unwrap();

        handles.push(handle);
    }

    let thd_num :usize = conf.thread_num;
    report_stat(handles, conf);

   // println!("{:?}", tables);
    


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
    let mut maps = data.maps;

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
        handle.join();
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
                total_time = std::cmp::max(total_time, per_thd.duration);
            }
            Err(_) => warn!("thread panics"),
        }
    }


    println!(
        "{}, {}, {},{},{}, {}, {:?}",
        conf.thread_num,
        conf.wh_num,
        total_success,
        total_abort,
        total_pc_success,
        total_pc_abort,
        total_time.as_secs() as u32 * 1000 + total_time.subsec_millis(),
        )
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
