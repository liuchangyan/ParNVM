
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

pub struct TransactionPar
{
    conflicts_ : Dep,
    all_ps_ : Vec<Piece>,
    deps_ : HashSet<Tid>,
    id_ : Tid,
    name_ : String 
}


impl TransactionPar
{

    pub fn new(pieces : Vec<Piece>, cfl: Dep, id : Tid, name: String) -> TransactionPar  {
        TransactionPar{
            all_ps_: pieces,
            conflicts_: cfl,
            deps_ : HashSet::new(), 
            id_ : id,
            name_ : name,
        }
    }

    
    pub fn can_run(&mut self, piece : &Piece) -> Option<(Arc<RwLock<TxnInfo>>, Pid)> {
        let pid = piece.id();
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

                    for cand_tid in cand_tids.iter() {
                        let info_ptr = txn_regis.instances_
                            .get(cand_tid)
                            .expect(format!("can_run::registry inconsistent data, id{:?}",
                                            cand_tid).as_str());

                        let info_g = info_ptr.read().unwrap();

                        match (*info_g).check_state(cfl_pid) {
                            PieceState::Ready | PieceState::Running => {
                                if self.deps_.contains(cand_tid) {
                                    return Some((info_ptr.clone(), cfl_pid.clone()));
                                }
                            },

                            PieceState::Executed | PieceState::Persisted => {
                                self.deps_.insert(cand_tid.clone());
                            }
                        }
                    }
                }
                
                None
            },
            None => None 
        }
    }
    //TODO:
    //pub fn prepare_log();
    
    pub fn register_txn(&mut self) {
        let regis_ptr = TxnRegistry::get_thread_registry();
        let mut regis = regis_ptr.write().unwrap();

        let pids  = self.all_ps_.iter()
            .map(|piece| piece.id().clone())
            .collect();

        let txn_info = TxnInfo::new(pids);

        (*regis).register(self.name().clone(), self.id().clone(), Arc::new(RwLock::new(txn_info)));
    }

    pub fn execute_txn(&mut self) {
        while let Some(mut piece) = self.get_next_piece() {
            match self.can_run(&piece) {
                None => {
                    self.execute_piece(&mut piece); 
                }, 
                Some((info, cfl_pid)) => {
                    if self.has_next_piece() {
                        self.add_piece(piece);
                    } else {
                        self.spin_on(&mut piece, info, cfl_pid);
                    }
                }

            }
        }

        self.wait_for_dep();
        self.commit();
    }

    pub fn execute_piece(&self, piece : &mut Piece) {
        let regis_ptr = TxnRegistry::get_thread_registry();
        let regis = regis_ptr.read().unwrap();
        let pid = piece.id().clone();

        let info_ptr = (*regis).get_info_by_id(self.id()).expect("execute_piece:: info should not be none");
        {

            let mut info = info_ptr.write().unwrap();
            (*info).update_state(pid.clone(), PieceState::Running);
        }
        
        piece.run();

        let mut info = info_ptr.write().unwrap();
        (*info).update_state(pid, PieceState::Executed);
    }

    pub fn id(&self) -> &Tid {
        &self.id_
    }

    pub fn name(&self) -> String {
        self.name_.clone()
    }

    pub fn get_next_piece(&mut self) -> Option<Piece> {
        self.all_ps_.pop()
    }

    pub fn add_dep(&mut self, tid : Tid) {
        self.deps_.insert(tid);
    }

    pub fn has_next_piece(&self) -> bool {
        self.all_ps_.is_empty()
    }

    pub fn add_piece(&mut self, piece : Piece) {
        self.all_ps_.push(piece)
    }
    
    pub fn commit(&self) {
        let regis_ptr = TxnRegistry::get_thread_registry();
        let mut regis = regis_ptr.write().unwrap();

        (*regis).checkout(self.name(), self.id().clone()).expect("commit:: info is checkouted");
    }

    pub fn wait_for_dep(&self) {
        let id = self.id();
        loop {
            let regis_ptr = TxnRegistry::get_thread_registry();
            let regis = regis_ptr.read().unwrap();

            if let  None = (*regis).get_info_by_id(id) {
                break;
            }
        }
    }

    pub fn spin_on(&self, piece : &mut Piece, txn_info : Arc<RwLock<TxnInfo>>, pid: Pid) {
        loop {
            let info_g = txn_info.read().unwrap();
            match *info_g.check_state(&pid) {
                PieceState::Executed | PieceState::Persisted =>  {
                    break; 
                },
                _ => {
                    thread::yield_now();
                }
            }
        }
        
        self.execute_piece(piece);
    }

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
    /* Thread local methods */
    pub fn get_thread_registry() -> TxnRegistryPtr {
        TXN_REGISTRY.with( |ref ptr| Arc::clone(ptr))
    }

    pub fn new_thread_registry_names(names : Vec<String>){
        TXN_REGISTRY.with( |ptr| {
            let mut regis = ptr.write().unwrap();
            (*regis).set_names(names);
        })
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
        
        let info_set = self.registry_
            .get_mut(&txn_name)
            .expect(format!("txn name is not found {:?}", txn_name).as_str());
        info_set.insert(tid);
    }

    pub fn checkout(&mut self, txn_name :String, tid: Tid) -> Option<Arc<RwLock<TxnInfo>>>{
        let info_set = self.registry_
            .get_mut(&txn_name)
            .expect(format!("txn name is not found {:?}", txn_name).as_str());

        info_set.remove(&tid);
        
        self.instances_.remove(&tid)
    }
    
    pub fn get_info_by_id(&self, tid : &Tid) -> Option<&Arc<RwLock<TxnInfo>>> {
        self.instances_.get(tid)
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

    pub fn update_state(&mut self, pid : Pid, state : PieceState) {
        self.ps_info_.entry(pid).and_modify(|e| {*e = state.clone()});
    }
}




