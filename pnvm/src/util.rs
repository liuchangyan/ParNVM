
//Mod for test workload preparation
extern crate rand;
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
use zipf::ZipfDistribution;

use pnvm_lib::{
    txn::*,
    tcore::*,
    tbox::*,
    occ::*,
    parnvm::*,
};


pub struct TestHelper {
}


impl TestHelper {
    pub fn prepare_workload(config: &Config) -> Workload {
        match config.test_name.as_ref() {
            "OCC" => Workload{ dataset_: prepare_data(config) },
            "ParNVM" => WorkloadNVM::new_parnvm(config),
            _ => panic!("Unknown test type {}", config.test_name)
        }
    }

}

pub struct Workload {
    pub dataset_: DataSet
}

pub struct WorkloadNVM {
    pub dataset_ :  DataSet,
    pub registry_ : nvm_txn::TxnRegistryPtr,
}


impl Workload {
    pub fn new_parnvm(conf: &Config) -> Workload {

        let threads_work = Vec<Vec<nvm_txn::TransactionPar>>::with_capacity(conf.thread_num);
        let cfl_txn_num = conf.CFL_TXN_NUM;
        let cfl_pc_num = conf.CFL_PC_NUM;
        let pc_num = conf.PC_NUM;
        
        //Prepare registry 
        let txn_names : Vec<String> = make_txn_names(conf.thread_num);
        let regis = nvm_txn::TxnRegistry::new_with_names(txn_names);
        let regis_ptr = Arc::new(RwLock::new(regis));
        nvm_txn::TxnRegistry::set_thread_registry(regis_ptr.clone());

        //Prepare data
        let data : Vec<Arc<RwLock<HashMap<u32, u32>>>> = (0..conf.obj_num).map(|_x| Arc::new(RwLock::new(HashMap::new()))).collect();


        //Prepare TXNs
        //For now, thread_num == txn_num
        for thread_i in 0..conf.thread_num {
            let tx_name = make_txn_name(thread_i);
            threads_work.push(make_txns(tx_name, conf, &data));
        }
        

    }

    fn make_txns(tx_name : String, conf: &Config, data : &Vec<Arc<RwLock<HashMap<u32, u32>>>>) -> Vec<nvm_txn::TransactionPar> {
        let mut works = Vec::with_capacity(conf.txn_num);       
        for txn_i in 0..conf.txn_num {
            


        }
    }



    fn make_txn_names(thread_num : usize) -> Vec<String> {
        let let names = Vec::with_capacity(thread_num);

        for i in 0..thread_num {
            names.push(format!("TXN_{}", i));
        }

        names

    }

    fn make_piece_callback() -> 

}


pub struct DataSet {
    pub read : Vec<Vec<TObject<u32>>>,
    pub write : Vec<Vec<TObject<u32>>>,
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



#[derive(Debug, Clone)]
pub struct Config {
  pub   thread_num: usize,
  pub   obj_num:usize,
  pub   set_size:usize,
  pub   round_num:usize,
  pub   zipf_coeff: f64,
  pub   use_pmem: bool,
  pub   test_name : String
}

pub fn read_env() -> Config {
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
        test_name : settings.get_str("TEST_NAME").unwrap() ,
    }

}



