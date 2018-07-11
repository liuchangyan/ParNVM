extern crate pnvm_lib;
extern crate rand;

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate config;

use std::{
    sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}},
    thread,
};
use rand::{
    thread_rng,
    Rng,
};

use pnvm_lib::{
    txn::*,
    tcore::*,
    tbox::*,
};



fn main() {
    env_logger::init().unwrap();
    pnvm_lib::tcore::init();
    
    let conf = read_env();
    println!("{:?}", conf);

    let mtx = Arc::new(Mutex::new(0));
    let mut objs = prepare_data(&conf);
    let atomic_cnt = Arc::new(AtomicUsize::new(1));

    let mut handles = vec![];

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
                let id= atomic_clone.fetch_add(1, Ordering::SeqCst) as u32;
                let tx = &mut Transaction::new(Tid::new(id));

                for read in read_set.iter() {
                    debug!("[THREAD {:} TXN {:}] READ {:}", i+1, id,  tx.read(&read));
                }

                for write in write_set.iter() {
                    tx.write(&write, (i+1) as u32);
                    debug!("[THREAD {:} TXN {:}] WRITE {:}",i+1,  id, i+1);
                }

                info!("[THREAD {:} - TXN {:}] COMMITS {:} ",i+1,  id, tx.try_commit());
            }
        }).unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}


pub struct DataSet {
    read : Vec<Vec<TObject<u32>>>,
    write : Vec<Vec<TObject<u32>>>,
}

fn prepare_data(conf : &Config) -> DataSet {
    let mut rng = thread_rng();
    let pool : Vec<TObject<u32>> = (0..conf.obj_num).map(|x| TBox::new(x as u32)).collect();

    let mut dataset = DataSet {
        read : Vec::with_capacity(conf.thread_num),
        write: Vec::with_capacity(conf.thread_num),
    };

    for i in 0..conf.thread_num {
        dataset.read.push(Vec::new());
        dataset.write.push(Vec::new());

        for _ in 0..conf.set_size {
            let rk : usize = rng.gen_range(0, conf.obj_num);
            let wk : usize = rng.gen_range(0, conf.obj_num); 

            dataset.read[i].push(Arc::clone(&pool[rk]));
            dataset.write[i].push(Arc::clone(&pool[wk]));
        }
    }
    dataset
}


#[derive(Debug, Clone, Copy)]
struct Config {
    thread_num: usize,
    obj_num:usize,
    set_size:usize,
    round_num:usize,
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
    }

}





