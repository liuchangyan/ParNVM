#![feature(duration_extras)]
extern crate pnvm_lib;

extern crate rand;
extern crate config;
extern crate zipf;

#[macro_use]
extern crate log;
extern crate env_logger;

mod util;

use util::*;

use std::{
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
    thread,
    time,
};

use pnvm_lib::{
    txn::*,
    tcore::*,
    tbox::*,
    occ::*,
    parnvm::nvm_txn::{TransactionPar,TxnRegistry},
};





fn main() {
    env_logger::init().unwrap();
    pnvm_lib::tcore::init();
    
    let conf = util::read_env();
    warn!("{:?}", conf);
    
    //run_occ(conf);
    run_nvm(conf);
}



fn run_nvm(conf : Config) {
    let workload = util::TestHelper::prepare_workload_nvm(&conf);
    let work = workload.work_;
    let regis = workload.registry_;
    let mut handles = Vec::new();

    let atomic_cnt = Arc::new(AtomicUsize::new(1));

    for i in 0..conf.thread_num {
        /* Per thread preparation */
        let conf = conf.clone();
        let thread_txn_base = work[i].clone();
        let builder = thread::Builder::new()
            .name(format!("TID-{}", i+1));
        let atomic_clone = atomic_cnt.clone();
        let regis = regis.clone();

        let handle = builder.spawn(move || {
            TxnRegistry::set_thread_registry(regis);
            for _ in 0..conf.round_num {
                let id= atomic_clone.fetch_add(1, Ordering::SeqCst) as u32;
                let mut tx = TransactionPar::new_from_base(&thread_txn_base, Tid::new(id));

                tx.register_txn();
                tx.execute_txn();
            }
        }).unwrap();

        handles.push(handle);
    }


    for handle in handles {
        handle.join().unwrap();
    }
}


fn run_occ(conf : Config) {

    let mtx = Arc::new(Mutex::new(0));
    let mut objs = util::TestHelper::prepare_workload_occ(&conf).get_dataset();
    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let start = time::Instant::now();
    

    for i in 0..conf.thread_num {
        let conf = conf.clone();
        let read_set = objs.read.pop().unwrap();
        let write_set = objs.write.pop().unwrap();
        let atomic_clone = atomic_cnt.clone();
        let builder = thread::Builder::new()
            .name(format!("TID-{}", i+1));
        
        let mtx = mtx.clone();
        let handle = builder.spawn(move || {
            {
                let _ = mtx.lock().unwrap();
                pnvm_lib::tcore::init();
            }

            for _ in 0..conf.round_num {
                let now = time::Instant::now();
                let id= atomic_clone.fetch_add(1, Ordering::SeqCst) as u32;
                BenchmarkCounter::add_time(now.elapsed());

                let tx = &mut occ_txn::TransactionOCC::new(Tid::new(id), conf.use_pmem);
            

                for read in read_set.iter() {
                    let val = tx.read(&read);
                    debug!("[THREAD {:} TXN {:}] READ {:}", i+1, id,  val);
                }

                for write in write_set.iter() {
                    tx.write(&write, (i+1) as u32);
                    debug!("[THREAD {:} TXN {:}] WRITE {:}",i+1,  id, i+1);
                }
                let res = tx.try_commit();
                info!("[THREAD {:} - TXN {:}] COMMITS {:} ",i+1,  id, res);
                
            }

            //#[cfg(benchmark)]
            BenchmarkCounter::copy()
        }).unwrap();

        handles.push(handle);
    }

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
            },
            Err(_) => warn!("thread panics")
        }
    }
   let total_time =  start.elapsed() - spin_time;

    println!("{}, {}, {}, {}, {}, {}, {:?}, {}", 
             conf.thread_num,
             conf.obj_num,
             conf.set_size,
             conf.zipf_coeff,
             total_success,
             total_abort,
             total_time.as_secs() as u32 *1000  + total_time.subsec_millis(),
             conf.use_pmem
             )

}




