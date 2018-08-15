#![feature(duration_extras, global_allocator)]
extern crate pnvm_lib;

#[cfg(feature = "profile")]
extern crate flame;

extern crate config;
extern crate rand;
extern crate zipf;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate parking_lot;

mod util;

use util::*;

use std::{
    fs::File,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread, time,
};

use pnvm_lib::{
    occ::*,
    parnvm::nvm_txn::{TransactionPar},
    tbox::*,
    tcore::*,
    txn::*,
};

#[cfg(feature = "pmem")]
#[global_allocator]
static GLOBAL: GPMem = GPMem;

fn main() {
    env_logger::init().unwrap();

    let conf = util::read_env();
    warn!("{:?}", conf);
    match conf.test_name.as_ref() {
        "OCC" => run_occ(conf),
        "PNVM" => run_nvm(conf),
        "SINGLE" => run_single(conf),
        _ => panic!("unknown test name"),
    }
}

fn run_nvm(conf: Config) {
    let workload = util::TestHelper::prepare_workload_nvm(&conf);
    let work = workload.work_;
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut prep_time = time::Duration::new(0, 0);

    let start = time::Instant::now();

    #[cfg(feature = "profile")]
    flame::start("benchmark");

    for i in 0..conf.thread_num {
        /* Per thread preparation */
        let _prep_start = time::Instant::now();
        let conf = conf.clone();
        let barrier = barrier.clone();
        let thread_txn_base = work[i].clone();
        let builder = thread::Builder::new().name(format!("TXN-{}", i + 1));
        let atomic_clone = atomic_cnt.clone();

        prep_time += _prep_start.elapsed();

        let handle = builder
            .spawn(move || {
                TidFac::set_thd_mask(i as u32);
                barrier.wait();
                for _ in 0..conf.round_num {
                    /* Get tid */
                    let tid = TidFac::get_thd_next();

                    #[cfg(feature = "profile")]
                    {
                        flame::start(format!("start_txn - {:?}", tid));
                    }

                    let mut tx = TransactionPar::new_from_base(&thread_txn_base, tid);

                    TransactionPar::register(tx);
                    TransactionPar::execute();

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

    report_stat(handles, start, prep_time, conf);

    #[cfg(feature = "profile")]
    {
        flame::end("benchmark");
        let mut f = File::create("profile/nvm.profile").unwrap();
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
    let start = time::Instant::now();
    let barrier = Arc::new(Barrier::new(conf.thread_num));
    let mut prep_time = time::Duration::new(0, 0);

    #[cfg(feature = "profile")]
    flame::start("benchmark_start");

    for i in 0..conf.thread_num {
        let _prep_start = time::Instant::now();
        let conf = conf.clone();
        let atomic_clone = atomic_cnt.clone();
        let barrier = barrier.clone();
        let builder = thread::Builder::new().name(format!("TID-{}", i + 1));
        let keys = keys[i].clone();
        let maps = maps.clone();
        let mtx = mtx.clone();

        prep_time += _prep_start.elapsed();
        let handle = builder
            .spawn(move || {
                barrier.wait();

                for _ in 0..conf.round_num {
                    let now = time::Instant::now();
                    let id = atomic_clone.fetch_add(1, Ordering::SeqCst) as u32;
                    BenchmarkCounter::add_time(now.elapsed());
                    let tx = &mut occ_txn::TransactionOCC::new(Tid::new(id));
                    let tid = Tid::new(id);
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
                                #[cfg(feature = "profile")]
                                {
                                    flame::start("read_start");
                                }
                                let id = tx.commit_id();
                                let tobj = map.get(&read).unwrap();
                                //let val = tx.read(&tobj);
                                let val = tx.read(tobj.get());
                                debug!("[{:?}] Read {:?}", id , val);
                                #[cfg(feature = "profile")]
                                {
                                    flame::end("read_start");
                                }
                            }

                            for write in write_keys.iter() {
                                let tobj = map.get(&write).unwrap();
                                let val :u32 = tx.commit_id().into();
                                debug!("[{:?}] Write {:?}", tx.commit_id(), val);
                                tx.write(tobj.get(), val);
                            }
                        }
                        #[cfg(feature = "profile")]
                        {
                            flame::end("data");
                        }

                        let res = tx.try_commit();
                        !res
                    } {}

                    info!("[THREAD {:} - TXN {:}] COMMITS", i + 1, id);

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

    report_stat(handles, start, prep_time, conf);

    #[cfg(feature = "profile")]
    {
        flame::end("benchmark_start");
        let mut f = File::create("profile/occ.profile").unwrap();
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
    start: time::Instant,
    prep_time: time::Duration,
    conf: Config,
    ) {
    let mut total_abort = 0;
    let mut total_success = 0;
    let mut spin_time = time::Duration::new(0, 0);

    for handle in handles {
        //#[cfg(benchmark)]
        match handle.join() {
            Ok(per_thd) => {
                total_success += per_thd.success_cnt;
                total_abort += per_thd.abort_cnt;
                spin_time += per_thd.duration;
            }
            Err(_) => warn!("thread panics"),
        }
    }
    //let total_time =  start.elapsed() - spin_time;
    let total_time = start.elapsed();
    println!(
        "{},{},{}, {}, {}, {}, {}, {:?}, {:?}, {:?}",
        conf.thread_num,
        conf.obj_num,
        conf.set_size,
        conf.zipf_coeff,
        conf.pc_num,
        total_success,
        total_abort,
        total_time.as_secs() as u32 * 1000 + total_time.subsec_millis(),
        spin_time.as_secs() as u32 * 1000 + spin_time.subsec_millis(),
        prep_time.as_secs() as u32 * 1000 + prep_time.subsec_millis(),
        )
}
