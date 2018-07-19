
use txn::{
    self,
    Transaction,
    Tid,
};

use super::piece::*;
use super::dep::*;

use std::{
    collections::{ HashSet, HashMap},
    sync::{Arc, RwLock},
    thread,
};

pub struct TransactionPar<F>
where F : FnMut() -> i32
{
    conflicts_ : Dep,
    all_ps_ : Vec<Piece<F>>,
    deps_ : HashSet<Tid>,
    id_ : Tid,
    name_ : String 
}


impl<F> TransactionPar<F>
where F : FnMut() -> i32
{

    pub fn new(pieces : Vec<Piece<F>>, cfl: Dep, id : Tid, name: String) -> TransactionPar<F> {
        TransactionPar{
            all_ps_: pieces,
            conflicts_: cfl,
            deps_ : HashSet::new(), 
            id_ : id,
            name_ : name,
        }
    }

    
    pub fn can_run(&mut self, pid : &Pid) -> Option<Tid> {
        let conflicts = self.conflicts_.get_conflict_info(pid);

        match conflicts {
            Some(conflicts) => {
                let regis_ptr = TxnRegistry::get_thread_registry();
                let txn_regis_g = regis_ptr.read().unwrap();
                let txn_regis = &*txn_regis_g;

                for conflict in conflicts.iter() {
                    let cfl_name = &conflict.txn_name_;
                    let cfl_pid = &conflict.piece_id_;

                    let cand_tids = txn_regis.registry_
                        .get(cfl_name)
                        .expect(format!("can_run:: txn name not correct : {:}", cfl_name).as_str());

                    for candidate_id in cand_tids.iter() {
                        let info_g = txn_regis.instances_
                            .get(candidate_id)
                            .expect(format!("can_run::registry inconsistent data, id{:?}",
                                            candidate_id).as_str())
                            .read()
                            .unwrap();

                        match (*info_g).check_state(cfl_pid) {
                            PieceState::Ready | PieceState::Running => {
                                if self.deps_.contains(candidate_id) {
                                    return Some(candidate_id.clone());
                                }
                            },

                            PieceState::Executed | PieceState::Persisted => {
                                self.deps_.insert(candidate_id.clone());
                            }
                        }
                    }
                }
                
                None
            },
            None => None 
        }
    }
    //pub fn execute_txn();
    //pub fn can_run();
    //pub fn get_next_piece();
    //pub fn has_next_piece();
    //pub fn prepare_log();
    //pub fn execute_piece();
    //pub fn add_dep();
    
    pub fn execute_txn(&mut self) {
        while let Some(mut piece) = self.get_next_piece() {
            let pid = piece.id();
            match self.can_run(&pid) {
                None => {
                    piece.run();
                }, 
                Some(dep) => {
                    if self.has_next_piece() {
                        self.add_piece(piece);
                    } else {
                        //self.spin_on(piece);
                    }
                }

            }
        }

       // while !self.can_commit() {
       //     thread::yield_now();
       // }

        self.commit();
    }

    pub fn id(&self) -> Tid {
        self.id_
    }

    pub fn name(&self) -> String {
        self.name_.clone()
    }

    pub fn get_next_piece(&mut self) -> Option<Piece<F>> {
        self.all_ps_.pop()
    }

    pub fn add_dep(&mut self, tid : Tid) {
        self.deps_.insert(tid);
    }

    pub fn has_next_piece(&self) -> bool {
        self.all_ps_.is_empty()
    }

    pub fn add_piece(&mut self, piece : Piece<F>) {
        self.all_ps_.push(piece)
    }

    pub fn commit(&self) {

    }

    //pub fn spin_on(&self, piece : Piece<F>, pid : Pid) -> i32 {
    //    //FIXME
    //    //while !piece::has_run(pid) {
    //    //    thread::yield_now();
    //    //}

    //    piece.run()
    //}

}


type TxnRegistryPtr = Arc<RwLock<TxnRegistry>>;
thread_local!{
    pub static TXN_REGISTRY : TxnRegistryPtr = Arc::new(RwLock::new(TxnRegistry::new()));
}



pub struct TxnRegistry {
    pub registry_ : HashMap<String, HashSet<Tid>>,
    pub instances_ : HashMap<Tid, Arc<RwLock<TxnInfo>>>
}

impl TxnRegistry {
    pub fn get_thread_registry() -> TxnRegistryPtr {
        TXN_REGISTRY.with( |ref ptr| Arc::clone(ptr))
    }



    pub fn new() -> TxnRegistry {
        TxnRegistry {
            registry_: HashMap::new(),
            instances_: HashMap::new(),
        }
    }

    pub fn new_with_names(names : Vec<String>) -> TxnRegistry {
        let mut registry_ : HashMap<String, HashSet<Tid>> = HashMap::new();
        for name in names.into_iter() {
            registry_.insert(name, HashSet::new());
        }

        TxnRegistry {
            registry_ : registry_,
            instances_ :HashMap::new(),
        }
    }

    pub fn set_names(&mut self, names: Vec<String>)  {
        for name in names.into_iter() {
            self.registry_.insert(name, HashSet::new());
        }
    }

    pub fn register(&mut self,  txn_name:String, tid: Tid, txn_info : Arc<RwLock<TxnInfo>>) {
        self.instances_.insert(tid, txn_info);
        
        let mut info_set = self.registry_
            .get_mut(&txn_name)
            .expect(format!("txn name is not found {:?}", txn_name).as_str());
        info_set.insert(tid);
    }

    pub fn checkout(&mut self, txn_name :String, tid: Tid) -> Option<Arc<RwLock<TxnInfo>>>{
        let mut info_set = self.registry_
            .get_mut(&txn_name)
            .expect(format!("txn name is not found {:?}", txn_name).as_str());

        info_set.remove(&tid);
        
        self.instances_.remove(&tid)
    }

}


#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct TxnName {
    name : String    
}



pub struct TxnInfo {
    ps_info_ : HashMap<Pid, PieceState>,
}


impl TxnInfo {
    pub fn new( pids : Vec<Pid>) -> TxnInfo {
        let mut ps_info_ = HashMap::new();

        for pid in pids.into_iter() {
            ps_info_.insert(pid, PieceState::Ready);
        }

        TxnInfo{
            ps_info_
        }
    }

    pub fn check_state(&self, pid : &Pid) -> &PieceState {
        self.ps_info_.get(pid).expect(format!("check_state:: piece missing, pid={:?}", pid).as_str())
    }
}




