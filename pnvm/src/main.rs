#![feature(duration_extras)]
extern crate pnvm_lib;
extern crate rand;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate config;
extern crate zipf;

use std::{
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
    thread,
    time,
};
use rand::{
    distributions::Distribution,
};

use pnvm_lib::{
    txn::*,
    tcore::*,
    tbox::*,
    occ::*,
    parnvm::*,
};


use zipf::ZipfDistribution;



fn main() {
    env_logger::init().unwrap();
    pnvm_lib::tcore::init();
    
    let conf = read_env();
    warn!("{:?}", conf);
    
    run_occ(conf);
    //run_own(conf);

}

fn run_own(conf : Config {



}

fn run_occ(conf : Config) {

    let mtx = Arc::new(Mutex::new(0));
    let mut objs = prepare_data(&conf);
    let atomic_cnt = Arc::new(AtomicUsize::new(1));
    let mut handles = vec![];
    let start = time::Instant::now();

    for i in 0..conf.thread_num {
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


pub struct DataSet {
    read : Vec<Vec<TObject<u32>>>,
    write : Vec<Vec<TObject<u32>>>,
}

fn prepare_data(conf : &Config) -> DataSet {
    let pool : Vec<TObject<u32>> = (0..conf.obj_num).map(|x| TBox::new(x as u32)).collect();
    let mut read_idx = vec![0;conf.obj_num];
    let mut write_idx  = vec![0;conf.obj_num];


    let mut dataset = DataSet {
        read : Vec::with_capacity(conf.thread_num),
        write: Vec::with_capacity(conf.thread_num),
    };

    let mut rng = rand::thread_rng();
    let dis = ZipfDistribution::new(conf.obj_num-1, conf.zipf_coeff).unwrap();

    for i in 0..conf.thread_num {
        dataset.read.push(Vec::new());
        dataset.write.push(Vec::new());

        for _ in 0..conf.set_size {
            
            let rk  = dis.sample(&mut rng);
            let wk  = dis.sample(&mut rng);
            
            read_idx[rk]+=1;
            write_idx[wk]+=1;
             

            dataset.read[i].push(Arc::clone(&pool[rk]));
            dataset.write[i].push(Arc::clone(&pool[wk]));
        }
    }
    
    read_idx.sort();
    read_idx.reverse();
    write_idx.sort();
    write_idx.reverse();

    let (read_top, _) = read_idx.split_at(conf.obj_num/10 as usize);
    let (write_top, _) = write_idx.split_at(conf.obj_num/10 as usize);

    debug!("Read: {:?}", read_top);
    debug!("Write: {:?}", write_top);

    dataset
}


#[derive(Debug, Clone, Copy)]
struct Config {
    thread_num: usize,
    obj_num:usize,
    set_size:usize,
    round_num:usize,
    zipf_coeff: f64,
    use_pmem: bool,
}

fn read_env() -> Config {
    let mut settings = config::Config::default();


    settings.merge(config::File::with_name("Settings")).unwrap()
        .merge(config::Environment::with_prefix("PNVM")).unwrap();


    Config {
        thread_num: settings.get_int("THREAD_NUM").unwrap() as usize,
        obj_num : settings.get_int("OBJ_NUM").unwrap() as usize,
        set_size : settings.get_int("SET_SIZE").unwrap() as usize,
        round_num : settings.get_int("ROUND_NUM").unwrap() as usize,
        zipf_coeff: settings.get_float("ZIPF_COEFF").unwrap() as f64,
        use_pmem : settings.get_bool("USE_PMEM").unwrap() as bool,
    }

}





