
//Mod for test workload preparation
extern crate rand;
extern crate config;
extern crate zipf;

use std::{
    sync::{Arc, RwLock},
    collections::HashMap,
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
    parnvm::{dep::*, piece::*, nvm_txn::*},
};


pub struct TestHelper {
}


impl TestHelper {
    pub fn prepare_workload_occ(config: &Config) ->  WorkloadOCC {
        //WorkloadOCC{dataset_: WorkloadOCC::prepare_data(config)}
        WorkloadOCC{dataset_: WorkloadOCC::prepare_data_hardcoded(config)}
    }

    pub fn prepare_workload_nvm(config: &Config) -> WorkloadNVM {
        WorkloadNVM::new_parnvm(config)
    }

}


pub struct WorkloadOCC{
    dataset_: DataSet
}

impl WorkloadOCC {
    pub fn get_dataset(self) -> DataSet {
        self.dataset_
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

        //let (read_top, _) = read_idx.split_at(conf.obj_num/10 as usize);
        //let (write_top, _) = write_idx.split_at(conf.obj_num/10 as usize);

        //debug!("Read: {:?}", read_top);
        //debug!("Write: {:?}", write_top);

        dataset
    }


    fn prepare_data_hardcoded(conf : &Config) -> DataSet {
        let pool: Vec<TObject<u32>> = (0..conf.obj_num).map(|x| TBox::new(x as u32)).collect();
        let mut dataset = DataSet {
            read : Vec::with_capacity(conf.thread_num),
            write: Vec::with_capacity(conf.thread_num),
        };
        
        let mut next_item = conf.cfl_pc_num;

        for thread_id in 0..conf.thread_num {
            dataset.read.push(Vec::new());
            dataset.write.push(Vec::new());

            if thread_id < conf.cfl_txn_num {
                for i in 0..conf.cfl_pc_num { /* Conflicting Txns */
                    //Read and Write same TBox
                    dataset.read[thread_id].push(Arc::clone(&pool[i]));
                    dataset.write[thread_id].push(Arc::clone(&pool[i]));
                }

                for i in conf.cfl_pc_num..conf.pc_num {
                    dataset.read[thread_id].push(Arc::clone(&pool[next_item]));
                    dataset.write[thread_id].push(Arc::clone(&pool[next_item]));
                    next_item+=1;
                }
            } else { /* Non conflicting txns */
                for i in 0..conf.pc_num {
                    dataset.read[thread_id].push(Arc::clone(&pool[next_item]));
                    dataset.write[thread_id].push(Arc::clone(&pool[next_item]));
                    next_item+=1;
                }
            }
        }

        trace!("data: {:#?}", dataset);

        dataset
    }
}

pub struct WorkloadNVM {
    pub registry_ : TxnRegistryPtr,
    pub work_ : Vec<TransactionParBase>
}



impl WorkloadNVM{
    pub fn new_parnvm(conf: &Config) -> WorkloadNVM{

        let mut threads_work = Vec::with_capacity(conf.thread_num);

        //Prepare registry 
        let txn_names : Vec<String> = WorkloadNVM::make_txn_names(conf.thread_num);
        let regis = TxnRegistry::new_with_names(txn_names);
        let regis_ptr = Arc::new(RwLock::new(regis));
        TxnRegistry::set_thread_registry(regis_ptr.clone());

        //Prepare data
        //let data : Vec<Arc<RwLock<HashMap<u32, u32>>>> = (0..conf.obj_num).map(|_x| Arc::new(RwLock::new(HashMap::new()))).collect();
        let data : Vec<Arc<RwLock<u32>>> = (0..conf.obj_num).map(|x| Arc::new(RwLock::new(x as u32))).collect();


        //Prepare TXNs
        //For now, thread_num == txn_num
        let next_item_id = conf.cfl_txn_num; 
        for thread_i in 0..conf.thread_num {
            let txn_i = thread_i;
            let tx_name = WorkloadNVM::make_txn_name(thread_i);
            let (next_item_id, txn_base) = WorkloadNVM::make_txn_base(txn_i, tx_name, conf, &data, next_item_id);
            threads_work.push(txn_base);
        }
        
        
        WorkloadNVM {
            registry_: regis_ptr,
            work_ : threads_work
        }
    }

    fn make_txn_base(tx_id: usize, tx_name : String, conf: &Config, data : &Vec<Arc<RwLock<u32>>>, next_item: usize) -> (usize, TransactionParBase)  {

        let mut pieces = vec![];
        let mut is_conflict_txn : bool = false;
        let mut next_item = next_item;
        let mut dep = Dep::new();

        if tx_id < conf.cfl_txn_num {
            is_conflict_txn = true;
        }

        //Create closures

        for piece_id in 0..conf.pc_num {
            let mut data_map;
            if piece_id <= conf.cfl_pc_num && is_conflict_txn {
                data_map = data[piece_id].clone();
                WorkloadNVM::add_dep(conf, piece_id, tx_id, &mut dep);
            } else {
                data_map = data[next_item].clone();
                next_item += 1;
            }

            let callback = move || {
//                //Read 
//                {
//                    let map = data_map.read().unwrap();
//
//                    for iter in 0..50{
//                        let i = rand::random::<u32>();
//
//                        if let Some(txn) = map.get(&i) {
//                            println!("Map[{}] Set by [TXN-{}]", i, txn);
//                        }
//                    }
//                }
//
//                //Write
//                {
//                    let mut map = data_map.write().unwrap();
//                    for iter in 0..20 {
//                        let i = rand::random::<u32>();
//                        map.insert(i, tx_id as u32);
//                    }
//                }
                { 
                    let val = data_map.read().unwrap();
                    info!("Set by TXN-{}", *val);
                }
                {
                    let mut val = data_map.write().unwrap();
                    *val = tx_id as u32;
                }

                1
            };

            let piece = Piece::new(Pid::new(piece_id as u32), 
                                   tx_name.clone(),  
                                   Arc::new(Box::new(callback)),
                                   "cb");

            pieces.push(piece);
        }
        pieces.reverse();

        (next_item, TransactionParBase::new(dep, pieces, tx_name.clone()))
    }

    fn add_dep(conf : &Config, pid : usize, tx_id : usize, dep : &mut Dep) {
        let cfl_txn_num = conf.cfl_txn_num;
        let pid = Pid::new(pid as u32);
        for i in 0..cfl_txn_num {
            //No conflict with self yet
            if i != tx_id {
                dep.add(pid,ConflictInfo::new(WorkloadNVM::make_txn_name(i), 
                                              pid, 
                                              ConflictType::Write));
            }
        }
    }



    fn make_txn_names(thread_num : usize) -> Vec<String> {
        let mut names = Vec::with_capacity(thread_num);

        for i in 0..thread_num {
            names.push(format!("TXN_{}", i));
        }

        names

    }

    fn make_txn_name(thread_num: usize) -> String {
        format!("TXN_{}", thread_num)
    }

}

#[derive(Debug)]
pub struct DataSet {
    pub read : Vec<Vec<TObject<u32>>>,
    pub write : Vec<Vec<TObject<u32>>>,
}




#[derive(Debug, Clone)]
pub struct Config {
    pub   thread_num: usize,
    pub   obj_num:usize,
    pub   set_size:usize,
    pub   round_num:usize,
    pub   zipf_coeff: f64,
    pub   use_pmem: bool,
    pub   test_name : String,
    pub cfl_pc_num: usize,
    pub cfl_txn_num: usize,
    pub pc_num: usize,
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

        cfl_txn_num : settings.get_int("CFL_TXN_NUM").unwrap() as usize,
        cfl_pc_num : settings.get_int("CFL_PC_NUM").unwrap() as usize,
        pc_num : settings.get_int("PC_NUM").unwrap() as usize,
    }

}



