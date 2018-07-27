#![feature(duration_extras, global_allocator)]
extern crate pnvm_lib;

#[cfg(feature="profile")]
extern crate flame;

extern crate rand;
extern crate config;
extern crate zipf;

#[macro_use]
extern crate log;
extern crate env_logger;

mod util;

use util::*;

use std::{
    sync::{Barrier,Arc, Mutex, atomic::{AtomicUsize, Ordering}},
    thread,
    time,
    fs::File,
};

use pnvm_lib::{
    txn::*,
    tcore::*,
    tbox::*,
    occ::*,
    parnvm::nvm_txn::{TransactionPar,TxnRegistry},
};

#[global_allocator]
static GLOBAL: GPMem  = GPMem;

fn main() {
    env_logger::init().unwrap();
    pnvm_lib::tcore::init();

    let conf = util::read_env();
    warn!("{:?}", conf);
    match conf.test_name.as_ref() {
        "OCC" => run_occ(conf),
        "PNVM" => run_nvm(conf),
        _ => panic!("unknown test name")
    }
}



fn run_nvm(conf : Config) {
    let workload = util::TestHelper::prepare_workload_nvm(&conf);
    let work = workload.work_;
    let regis = workload.registry_;
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(conf.thread_num));

    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut prep_time = time::Duration::new(0,0);

    let start = time::Instant::now();
    for i in 0..conf.thread_num {
        /* Per thread preparation */
        let _prep_start = time::Instant::now();
        let conf = conf.clone();
        let barrier = barrier.clone();
        let thread_txn_base = work[i].clone();
        let builder = thread::Builder::new()
            .name(format!("TXN-{}", i+1));
        let atomic_clone = atomic_cnt.clone();
        let regis = regis.clone();

        prep_time += _prep_start.elapsed();

        let handle = builder.spawn(move || {
            TxnRegistry::set_thread_registry(regis);
            barrier.wait();
            for _ in 0..conf.round_num {
                /* Get tid */
                let now = time::Instant::now();
                let id= atomic_clone.fetch_add(1, Ordering::SeqCst) as u32;
                BenchmarkCounter::add_time(now.elapsed());

                let mut tx = TransactionPar::new_from_base(&thread_txn_base, Tid::new(id));

                tx.register_txn();
                let mut i = 0;
                while i<100000 { i+=1;} 
                tx.execute_txn();
            }
            
            BenchmarkCounter::copy()
        }).unwrap();

        handles.push(handle);
    }


    report_stat(handles, start, prep_time,  conf);

    #[cfg(feature="profile")]
    {
        let mut f = File::create("profile/nvm.profile").unwrap();
        flame::dump_text_to_writer(f);
    }
}


fn run_occ(conf : Config) {

    let mtx = Arc::new(Mutex::new(0));
    let mut objs = util::TestHelper::prepare_workload_occ(&conf).get_dataset();
    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let start = time::Instant::now();
    let barrier = Arc::new(Barrier::new(conf.thread_num));
    let mut prep_time = time::Duration::new(0,0);

    for i in 0..conf.thread_num {
        let _prep_start = time::Instant::now();
        let conf = conf.clone();
        let read_set = objs.read.pop().unwrap();
        let write_set = objs.write.pop().unwrap();
        let atomic_clone = atomic_cnt.clone();
        let barrier = barrier.clone();
        let builder = thread::Builder::new()
            .name(format!("TID-{}", i+1));
        
        let mtx = mtx.clone();

        prep_time += _prep_start.elapsed();
        let handle = builder.spawn(move || {
            barrier.wait();
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
                let mut i = 1;

                while i<100000 { i+=1;}

                while tx.try_commit() != true {}
                info!("[THREAD {:} - TXN {:}] COMMITS",i+1,  id);
                
            }

            //#[cfg(benchmark)]
            BenchmarkCounter::copy()
        }).unwrap();

        handles.push(handle);
    }


    report_stat(handles, start,prep_time, conf);


}

fn report_stat(handles : Vec<thread::JoinHandle<BenchmarkCounter>>, start: time::Instant, prep_time: time::Duration, conf: Config) {
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
    println!("{},{},{}, {}, {}, {}, {}, {}, {:?}, {:?}, {:?}, {}", 
             conf.thread_num,
             conf.obj_num,
             conf.set_size,
             conf.zipf_coeff,
             conf.cfl_pc_num,
             conf.cfl_txn_num,
             total_success,
             total_abort,
             total_time.as_secs() as u32 *1000  + total_time.subsec_millis(),
             spin_time.as_secs() as u32 *1000  + total_time.subsec_millis(),
             prep_time.as_secs() as u32 *1000 + prep_time.subsec_millis(),
             conf.use_pmem
             )

}




