//Mod for test workload preparation
extern crate config;
extern crate rand;
extern crate zipf;

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use rand::distributions::Distribution;
use zipf::ZipfDistribution;

use pnvm_lib::{
    occ::*,
    parnvm::{map::*, dep::*, nvm_txn::*, piece::*},
    tbox::*,
    tcore::*,
    txn::*,
};

pub struct TestHelper {}

impl TestHelper {
    pub fn prepare_workload_occ(config: &Config) -> WorkloadOCC {
        //WorkloadOCC{dataset_: WorkloadOCC::prepare_data(config)}
        WorkloadOCC {
            dataset_: WorkloadOCC::prepare_data_hardcoded(config),
        }
    }

    pub fn prepare_workload_nvm(config: &Config) -> WorkloadNVM {
        WorkloadNVM::new_parnvm(config)
    }
}

pub struct WorkloadOCC {
    dataset_: DataSet,
}

impl WorkloadOCC {
    pub fn get_dataset(self) -> DataSet {
        self.dataset_
    }

    fn prepare_data(conf: &Config) -> DataSet {
        let pool: Vec<TObject<u32>> = (0..conf.obj_num).map(|x| TBox::new(x as u32)).collect();
        let mut read_idx = vec![0; conf.obj_num];
        let mut write_idx = vec![0; conf.obj_num];

        let mut dataset = DataSet {
            read: Vec::with_capacity(conf.thread_num),
            write: Vec::with_capacity(conf.thread_num),
        };

        let mut rng = rand::thread_rng();
        let dis = ZipfDistribution::new(conf.obj_num - 1, conf.zipf_coeff).unwrap();

        for i in 0..conf.thread_num {
            dataset.read.push(Vec::new());
            dataset.write.push(Vec::new());

            for _ in 0..conf.set_size {
                let rk = dis.sample(&mut rng);
                let wk = dis.sample(&mut rng);

                read_idx[rk] += 1;
                write_idx[wk] += 1;

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

    fn prepare_data_hardcoded(conf: &Config) -> DataSet {
        let pool: Vec<TObject<u32>> = (0..conf.obj_num).map(|x| TBox::new(x as u32)).collect();
        let mut dataset = DataSet {
            read: Vec::with_capacity(conf.thread_num),
            write: Vec::with_capacity(conf.thread_num),
        };

        let mut next_item = conf.cfl_pc_num;

        for thread_id in 0..conf.thread_num {
            dataset.read.push(Vec::new());
            dataset.write.push(Vec::new());

            if thread_id < conf.cfl_txn_num {
                for i in 0..conf.cfl_pc_num {
                    /* Conflicting Txns */
                    //Read and Write same TBox
                    dataset.read[thread_id].push(Arc::clone(&pool[i]));
                    dataset.write[thread_id].push(Arc::clone(&pool[i]));
                }

                /* Non-conflicting pieces */
                for i in conf.cfl_pc_num..conf.pc_num {
                    dataset.read[thread_id].push(Arc::clone(&pool[next_item]));
                    dataset.write[thread_id].push(Arc::clone(&pool[next_item]));
                    next_item += 1;
                }
            } else {
                /* Non conflicting txns */
                for i in 0..conf.pc_num {
                    dataset.read[thread_id].push(Arc::clone(&pool[next_item]));
                    dataset.write[thread_id].push(Arc::clone(&pool[next_item]));
                    next_item += 1;
                }
            }
        }

        trace!("data: {:#?}", dataset);

        dataset
    }
}

pub struct WorkloadNVM {
    pub work_: Vec<TransactionParBase>,
}

impl WorkloadNVM {
    pub fn new_parnvm(conf: &Config) -> WorkloadNVM {
        let mut threads_work = Vec::with_capacity(conf.thread_num);

        //Prepare registry
        let txn_names: Vec<String> = WorkloadNVM::make_txn_names(conf.thread_num);

        //Prepare data
        let maps: Vec<Arc<PMap<u32, u32>>> = (0..conf.pc_num)
            .map(|x| Arc::new(PMap::new_with_keys((0..conf.obj_num as u32).collect())))
            .collect();


        let keys = Self::generate_data(conf);

        //Prepare TXNs
        //   For now, thread_num == txn_num
        //
        let next_item_id = conf.cfl_txn_num;
        for thread_i in 0..conf.thread_num {
            let txn_i = thread_i;
            let tx_name = WorkloadNVM::make_txn_name(thread_i);
            let txn_base =
                WorkloadNVM::make_txn_base(txn_i, tx_name, conf, &maps, &keys[thread_i]);
            threads_work.push(txn_base);
        }

        debug!("{:#?}", threads_work);
        WorkloadNVM {
            work_: threads_work,
        }
    }

    fn generate_data(conf : &Config) -> Vec<ThreadData<u32>> {
        
        let mut dataset : Vec<ThreadData<u32>> = (0..conf.thread_num).map(|i| ThreadData::new()).collect();

        let mut rng = rand::thread_rng();
        let dis = ZipfDistribution::new(conf.obj_num - 1, conf.zipf_coeff).unwrap();

        for i in 0..conf.thread_num {
            let data = &mut dataset[i];

            for _ in 0..conf.set_size {

                let rk = dis.sample(&mut rng) as u32;
                let wk = dis.sample(&mut rng) as u32;

                data.add_read(rk);
                data.add_write(wk);
            }

            data.read_keys.sort();
            data.write_keys.sort();
        }
        dataset
    }

    fn make_txn_base(
        tx_id: usize,
        tx_name: String,
        conf: &Config,
        maps: &Vec<Arc<PMap<u32, u32>>>,
        data: &ThreadData<u32>,
    ) -> TransactionParBase {

        let mut pieces = Vec::new();

        //Create closures
        for piece_id in 0..conf.pc_num {
            let mut data_map = maps[piece_id].clone();
            let read_keys = data.read_keys.clone();
            let write_keys = data.write_keys.clone();

            let spin_time = conf.spin_time;
            let set_size = conf.set_size;

            let callback = move |tx: &mut TransactionPar| {
                let mut w_v = vec![];
                let mut r_v = vec![];
                let mut w_g : Vec<PMutexGuard<u32>> = vec![];
                let mut r_g : Vec<PMutexGuard<u32>>= vec![];


                for i in 0..set_size {
                    w_v.push(data_map.get(&write_keys[i]).unwrap());
                    r_v.push(data_map.get(&read_keys[i]).unwrap());
                }
                
                //Get the write locks 
                for x in w_v.iter() {
                    w_g.push(x.write(tx));
                }

                //Get the read locks
                for x in r_v.iter() {
                    r_g.push(x.read(tx));
                }

                //TODO: Do persist here
                
                //Do reads
                for i in 0..set_size {
                    let x = *(r_g[i]).as_ref().unwrap();
                    debug!("Read {:?}", x);
                }
                
                //Do writes
                for i in 0..set_size {
                    let w = &mut w_g[i];
                    *w.as_mut().unwrap() = tx_id as u32;
                }

                //TODO
                1
            };

            let piece = Piece::new(
                Pid::new(piece_id as u32),
                tx_name.clone(),
                Arc::new(Box::new(callback)),
                "cb",
                piece_id,
            );

            pieces.push(piece);
        }

        pieces.reverse();

        TransactionParBase::new( pieces, tx_name.clone())
    }

//    fn add_dep(conf: &Config, pid: usize, tx_id: usize, dep: &mut Dep) {
//        let cfl_txn_num = conf.cfl_txn_num;
//        let pid = Pid::new(pid as u32);
//        for i in 0..cfl_txn_num {
//            //No conflict with self yet
//            if i != tx_id {
//                dep.add(
//                    pid,
//                    ConflictInfo::new(WorkloadNVM::make_txn_name(i), pid, ConflictType::Write),
//                );
//            }
//        }
//    }

    fn make_txn_names(thread_num: usize) -> Vec<String> {
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
    pub read: Vec<Vec<TObject<u32>>>,
    pub write: Vec<Vec<TObject<u32>>>,
}


pub struct DataSetPar<M> 
where M: Clone,
{
    pub data : Vec<ThreadData<M>>
}

pub struct ThreadData<M> 
where M: Clone,
{
    pub read_keys: Vec<M>,
    pub write_keys:  Vec<M>,
}


impl<M> ThreadData<M>
where M: Clone,
{
    pub fn add_read(&mut self, m: M) {
        self.read_keys.push(m);
    }

    pub fn add_write(&mut self, m: M) {
        self.write_keys.push(m);
    }

    pub fn new()-> ThreadData<M>  {
        ThreadData {
            read_keys: vec![],
            write_keys: vec![]
        }
    }
}



#[derive(Debug, Clone)]
pub struct Config {
    pub thread_num: usize,
    pub obj_num: usize,
    pub set_size: usize,
    pub round_num: usize,
    pub zipf_coeff: f64,
    pub test_name: String,
    pub cfl_pc_num: usize,
    pub cfl_txn_num: usize,
    pub pc_num: usize,
    pub spin_time: usize,
}

pub fn read_env() -> Config {
    let mut settings = config::Config::default();

    settings
        .merge(config::File::with_name("Settings"))
        .unwrap()
        .merge(config::Environment::with_prefix("PNVM"))
        .unwrap();

    Config {
        thread_num: settings.get_int("THREAD_NUM").unwrap() as usize,
        obj_num: settings.get_int("OBJ_NUM").unwrap() as usize,
        set_size: settings.get_int("SET_SIZE").unwrap() as usize,
        round_num: settings.get_int("ROUND_NUM").unwrap() as usize,
        zipf_coeff: settings.get_float("ZIPF_COEFF").unwrap() as f64,
        test_name: settings.get_str("TEST_NAME").unwrap(),

        cfl_txn_num: settings.get_int("CFL_TXN_NUM").unwrap() as usize,
        cfl_pc_num: settings.get_int("CFL_PC_NUM").unwrap() as usize,
        pc_num: settings.get_int("PC_NUM").unwrap() as usize,
        spin_time: settings.get_int("SPIN_TIME").unwrap() as usize,
    }
}
