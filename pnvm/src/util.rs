//u32od for test workload preparation
extern crate config;
extern crate rand;
extern crate zipf;

#[cfg(feature = "profile")]
extern crate flame;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    iter::FromIterator,
};
use std::{
    fmt::Debug,
    hash::Hash,
};

use parking_lot::RwLock;

use rand::distributions::Distribution;
use zipf::ZipfDistribution;

use pnvm_lib::{
    occ::{map::*, occ_txn::*},
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
            dataset_: WorkloadOCC::prepare_data(config),
        }
    }
    
    pub fn prepare_workload_nvm_occ(config: &Config) -> WorkloadNVMOCC {
        WorkloadNVMOCC::new_parnvm_occ(config)
    }

 //   pub fn prepare_workload_nvm(config: &Config) -> WorkloadNVM {
 //       WorkloadNVM::new_parnvm(config)
 //   }

    pub fn prepare_workload_single(config : &Config) -> WorkloadSingle {
        WorkloadSingle::new(config)
    }
}


pub struct WorkloadSingle {
    pub keys : ThreadData,
    pub maps : Vec<Arc<HashMap<u32, RwLock<Box<u32>>>>>
}

impl WorkloadSingle {
    pub fn new(conf:&Config) ->  WorkloadSingle {
        let mut maps : Vec<Arc<HashMap<u32, RwLock<Box<u32>>>>> = (0..conf.pc_num).map(|i| Arc::new(HashMap::new())).collect(); 
        let keys = generate_data(conf)[0].clone();

        for map in maps.iter_mut() {
            keys.fill_single(map);
        }

        WorkloadSingle {
            keys,
            maps,
        }
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
        let maps : Vec<Arc<TMap<u32, u32>>> = (0..conf.pc_num).map(|i| Arc::new(TMap::new_with_options((conf.set_size * conf.thread_num*64) as u16))).collect();

        //Prepare data
        let keys = generate_data(conf);

        for map in maps.iter() {
            for data in keys.iter() {
                data.fill_tmap(map);
            }
        }

        DataSet {
            keys : keys,
            maps : maps,
        }
    }

}

pub struct WorkloadNVM {
    pub work_: Vec<TransactionParBase>,
} 

pub struct WorkloadNVMOCC { 
    pub work_: Vec<TransactionParBaseOCC>,
}

impl WorkloadNVMOCC
{
    pub fn new_parnvm_occ(conf: &Config) -> WorkloadNVMOCC {
        let mut threads_work = Vec::with_capacity(conf.thread_num);

        //Prepare maps
        let txn_names: Vec<String> = WorkloadNVM::make_txn_names(conf.thread_num);
        let maps : Vec<Arc<TMap<u32, u32>>> = (0..conf.pc_num).map(|i| Arc::new(TMap::new_with_options((conf.set_size * conf.thread_num*64) as u16))).collect();
        //Prepare data
        let keys = generate_data(conf); 
        for map in maps.iter() {
            for data in keys.iter() {
                data.fill_tmap(map);
            }
        }

        //Prepare TXNs
        //   For now, thread_num == txn_num
        //
        let next_item_id = conf.cfl_txn_num;
        for thread_i in 0..conf.thread_num {
            let txn_i = thread_i;
            let tx_name = WorkloadNVM::make_txn_name(thread_i);
            let txn_base = WorkloadNVMOCC::make_txn_base_occ(txn_i, tx_name, conf, &maps, &keys[thread_i]);
            threads_work.push(txn_base);
        }

        WorkloadNVMOCC {
            work_: threads_work,
        }

    }

    fn make_txn_base_occ(
        tx_id: usize,
        tx_name: String,
        conf: &Config,
        data_map: &Vec<Arc<TMap<u32, u32>>>, 
        data: &ThreadData,
        ) -> TransactionParBaseOCC {

        let mut pieces = Vec::new();

        //Create closures
        for piece_id in 0..conf.pc_num {
            let data_map = data_map[piece_id].clone();
            let read_keys = data.read_keys.clone();
            let write_keys = data.write_keys.clone();

            let set_size = conf.set_size;


            let write_set : HashSet<u32> = HashSet::from_iter(write_keys.clone().into_iter()); 
            let read_set : HashSet<u32> = HashSet::from_iter(read_keys.clone().into_iter());

            let mut write_vec :Vec<_> = write_set.into_iter().map(|x| (x, 0)).collect();
            let mut read_vec : Vec<_> = read_set.into_iter().map(|x| (x, 1)).collect();

            let mut comb_vec = vec![];
            comb_vec.append(&mut write_vec);
            comb_vec.append(&mut read_vec);

            comb_vec.sort_unstable_by_key(|(x,r)| *x);

            let callback = move |tx: &mut TransactionParOCC| {
                //let mut rw_v = vec![];

                let id = *tx.id();

                for (x, rw) in comb_vec.iter() {
                    let tref = TInt::new(data_map.get(&x).expect("map get panic").get().clone());
                    if *rw == 1 { /* Read */
                        let v = tx.read::<u32>(Box::new(tref));
                        debug!("[{:?}] Read {:?}", id, v);
                    } else {
                        let val: u32 = tx.id().into();
                        tx.write(Box::new(tref), val);
                        debug!("[{:?}] Write {:?}", id, val);
                    }
                }

            };

            let piece = PieceOCC::new(
                Pid::new(piece_id as u32),
                tx_name.clone(),
                Arc::new(Box::new(callback)),
                "cb",
                piece_id,
                );

            pieces.push(piece);
        }

        pieces.reverse();

        TransactionParBaseOCC::new(pieces, tx_name.clone())
    }

}

impl WorkloadNVM {
    pub fn new_parnvm(conf: &Config) -> WorkloadNVM {
        let mut threads_work = Vec::with_capacity(conf.thread_num);

        //Prepare maps
        let txn_names: Vec<String> = WorkloadNVM::make_txn_names(conf.thread_num);
        let maps : Vec<Arc<PMap<u32, u32>>> = (0..conf.pc_num).map(|i| Arc::new(PMap::new_with_size(0, (conf.thread_num * conf.set_size *64) as u16))).collect();

        //Prepare data
        let keys = generate_data(conf);

        for map in maps.iter() {
            for data in keys.iter() {
                data.fill_pmap(map);
            }
        }

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


    fn make_txn_base(
        tx_id: usize,
        tx_name: String,
        conf: &Config,
        data_map: &Vec<Arc<PMap<u32, u32>>>, 
        data: &ThreadData,
        ) -> TransactionParBase {

        let mut pieces = Vec::new();

        //Create closures
        for piece_id in 0..conf.pc_num {
            let data_map = data_map[piece_id].clone();
            let read_keys = data.read_keys.clone();
            let write_keys = data.write_keys.clone();

            let set_size = conf.set_size;


            let write_set : HashSet<u32> = HashSet::from_iter(write_keys.clone().into_iter()); 
            let read_set : HashSet<u32> = HashSet::from_iter(read_keys.clone().into_iter());

            let mut write_vec :Vec<_> = write_set.into_iter().map(|x| (x, 0)).collect();
            let mut read_vec : Vec<_> = read_set.into_iter().map(|x| (x, 1)).collect();

            let mut comb_vec = vec![];
            comb_vec.append(&mut write_vec);
            comb_vec.append(&mut read_vec);

            comb_vec.sort_unstable_by_key(|(x,r)| *x);

            let callback = move |tx: &mut TransactionPar| {
                //let mut rw_v = vec![];
                let mut w_g : Vec<PMutexGuard<u32>> = vec![];
                let mut r_g : Vec<PMutexGuard<u32>>= vec![];

                let mut vs = vec![];

                #[cfg(feature = "profile")]
                {
                    flame::start("acquire locks");
                }

                

                //Get the values references
                for (x, rw) in comb_vec.iter() {
                    if *rw == 1{
                        let v = data_map.get(&x).expect("map get panic").get();
                        r_g.push(v.read(tx));
                        vs.push(v);
                    } else {
                        let v = data_map.get(&x).expect("map get panic").get();
                        w_g.push(v.write(tx));
                        vs.push(v);
                    }
                }



                #[cfg(feature = "profile")]
                {
                    flame::end("acquire locks");
                }

                #[cfg(feature="pmem")]
                tx.persist_logs();
                //TODO: Do persist here

                #[cfg(feature = "profile")]
                {
                    flame::start("modify data");
                }
                //Do readsstart
                for i in r_g.iter_mut() {

                    #[cfg(feature = "profile")]
                    {
                        flame::start("read");
                    }
                    let x = *i.as_ref().expect("unwrapping reads");
                    //debug!("[{:?}] Read {:?}", tx.id(), x);
                    #[cfg(feature = "profile")]
                    {
                        flame::end("read");
                    }
                }

                //Do writes
                let tid :u32 = tx.id().clone().into();
                for mut i in w_g.iter_mut() {
                    #[cfg(feature = "profile")]
                    {
                        flame::start("write");
                    }
                    let w = &mut i;
                    *w.as_mut().expect("unwrapping writes") = tid ;
                    //debug!("[{:?}] Write {:?}", tx.id(), tid);
                    #[cfg(feature = "profile")]
                    {
                        flame::end("write");
                    }
                }

                #[cfg(feature = "profile")]
                {
                    flame::end("modify data");
                }
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

fn generate_data(conf : &Config) -> Vec<ThreadData> {

    let mut dataset : Vec<ThreadData> = (0..conf.thread_num).map(|i| ThreadData::new()).collect();

    let mut rng = rand::thread_rng();
    let dis = ZipfDistribution::new(conf.obj_num - 1, conf.zipf_coeff).unwrap();

    for i in 0..conf.thread_num {
        let data = &mut dataset[i];

        for _ in 0..conf.set_size {

            let rk = dis.sample(&mut rng) as u32;
            let mut wk = dis.sample(&mut rng) as u32;

           // while data.has_read(wk) {
           //     wk = dis.sample(&mut rng) as u32; 
           // }

            data.add_read(rk);
            data.add_write(wk);
        }

        data.read_keys.sort();
        data.write_keys.sort();

    }
    dataset
}


pub struct DataSet {
    pub keys : Vec<ThreadData>,
    pub maps : Vec<Arc<TMap<u32, u32>>>,
}


pub struct DataSetPar
{
    pub data : Vec<ThreadData>
}

#[derive(Debug, Clone)]
pub struct ThreadData
{
    pub read_keys: Vec<u32>,
    pub write_keys:  Vec<u32>,
}


impl ThreadData
{
    pub fn add_read(&mut self, m: u32) {
        self.read_keys.push(m);
    }

    pub fn add_write(&mut self, m: u32) {
        self.write_keys.push(m);
    }

    pub fn has_read(&self, m : u32) -> bool {
        for x in self.read_keys.iter() {
            if *x == m {
                return true;
            }
        }
        false
    }

    //FIXu32E: r/w non overlapping
    pub fn fill_pmap(&self, map: &Arc<PMap<u32, u32>>) {
        for v in self.read_keys.iter() {
            map.insert(v.clone(), PValue::new_default(v.clone()));
        }

        for v in self.write_keys.iter() {
            map.insert(v.clone(), PValue::new_default(v.clone()));
        }
    }

    pub fn fill_tmap(&self, map: &Arc<TMap<u32, u32>>) {
        for v in self.read_keys.iter() {
            map.insert(v.clone(), Arc::new(TBox::new_default(v.clone())));
        }

        for v in self.write_keys.iter() {
            map.insert(v.clone(), Arc::new(TBox::new_default(v.clone())));
        }

    }

    pub fn fill_single(&self, map: &mut Arc<HashMap<u32, RwLock<Box<u32>>>>) {
        for v in self.read_keys.iter() {
            Arc::get_mut(map).unwrap().insert(v.clone(), RwLock::new(Box::new(v.clone())));
        }

        for v in self.write_keys.iter() {
            Arc::get_mut(map).unwrap().insert(v.clone(), RwLock::new(Box::new(v.clone())));
        }

    }

    pub fn new()-> ThreadData {
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
    pub duration: u64,
    pub wh_num: i32,
    pub d_num: i32,
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
        duration: settings.get_int("DURATION").unwrap() as u64,
        wh_num: settings.get_int("WH_NUM").unwrap() as i32,
        d_num : settings.get_int("D_NUM").unwrap() as i32,
    }
}
