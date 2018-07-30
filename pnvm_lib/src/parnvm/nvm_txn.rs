
use txn::{
    self,
    Transaction,
    Tid,
    TxState,
};

use tcore::{self};

use super::piece::*;
use super::dep::*;

use std::{
    collections::{ HashSet, HashMap, VecDeque},
    sync::{Arc},
    cell::{RefCell},
    thread,
};

use parking_lot::RwLock;

use log;
use evmap::{self, ReadHandle, WriteHandle, ShallowCopy};

#[cfg(feature = "profile")]
use flame;



#[derive(Clone, Debug)]
pub struct TransactionParBase {
    conflicts_ : Dep, 
    all_ps_ : VecDeque<Piece>,
    name_ : String,
}

impl TransactionParBase {
    pub fn new(conflicts : Dep, all_ps: VecDeque<Piece>, name: String) -> TransactionParBase {
        TransactionParBase{
            conflicts_: conflicts,
            all_ps_: all_ps,
            name_ : name
        }
    }
}

pub struct TransactionPar
{
    conflicts_ : Dep,
    all_ps_ : VecDeque<Piece>,
    deps_ : HashSet<Tid>,
    id_ : Tid,
    name_ : String ,
    status_: TxState,
    wait_ : Option<Piece>,
}


impl TransactionPar
{

    pub fn new(pieces : VecDeque<Piece>, cfl: Dep, id : Tid, name: String) -> TransactionPar  {
        TransactionPar{
            all_ps_: pieces, 
            conflicts_: cfl,
            deps_ : HashSet::new(), 
            id_ : id,
            name_ : name,
            status_: TxState::EMBRYO,
            wait_ : None
        }
    }


    pub fn new_from_base(txn_base : &TransactionParBase, tid: Tid) -> TransactionPar {
        let txn_base = txn_base.clone();

        TransactionPar {
            all_ps_ : txn_base.all_ps_,
            conflicts_: txn_base.conflicts_,
            name_ : txn_base.name_,
            id_: tid,
            status_ : TxState::EMBRYO,
            deps_: HashSet::new(),
            wait_ : None
        }
    }
    
    #[cfg_attr(feature = "profile", flame)]
    pub fn can_run(&mut self, piece : &Piece) -> Option<(Arc<RwLock<TxnInfo>>, Pid)> {
        let pid = piece.id();
        let conflicts = self.conflicts_.get_conflict_info(pid);
        let me = self.id().clone();


        match conflicts {
            Some(conflicts) => {
                
                
                #[cfg(feature = "profile")]
                {
                    flame::start("has conflict info");
                }

                info!("can_run::{:?} Checking conflicts {:?}", me, pid);
                let regis_ptr = TxnRegistry::get_thread_registry();
                let txn_regis_g = regis_ptr.read();
                let txn_regis = &*txn_regis_g;
                    
                //Each conflict txn
                for conflict in conflicts.iter() {

                    let cfl_name = &conflict.txn_name_;
                    let cfl_pid = &conflict.piece_id_;

                    #[cfg(feature = "profile")]
                    {
                        flame::start(format!("conflict with [{}:{:?}]", cfl_name, cfl_pid));
                    }
                    
                    let cand_tids = txn_regis.registry_
                        .get(cfl_name)
                        .expect(format!("can_run:: txn name not correct : {:}", cfl_name).as_str());
                    
                    //Multiple running instances
                    for cand_tid in cand_tids.iter() {
                        #[cfg(feature = "profile")]
                        {
                            flame::start(format!("with instance [{:?}]", cand_tid));
                        }

                        let info_ptr = txn_regis.instances_
                            .get(cand_tid)
                            .expect(format!("can_run::registry inconsistent data, id{:?}",
                                            cand_tid).as_str());

                        let info_g = info_ptr.read();
                        info!("can_run:: {:?} has conflict instance: {:?}-{:?}", me,info_g.id(), cfl_pid);
                        match (*info_g).check_state(cfl_pid) {
                            PieceState::Ready => {
                                if self.deps_.contains(cand_tid) {
                                    #[cfg(feature = "profile")]
                                    {
                                        flame::end(format!("with instance [{:?}]", cand_tid));
                                        flame::end(format!("conflict with [{}:{:?}]", cfl_name, cfl_pid));
                                        flame::end("has conflict info");
                                    }
                                    return Some((info_ptr.clone(), cfl_pid.clone()));
                                }
                            },
                            PieceState::Running => {
                                    #[cfg(feature = "profile")]
                                    {
                                        flame::end(format!("with instance [{:?}]", cand_tid));
                                        flame::end(format!("conflict with [{}:{:?}]", cfl_name, cfl_pid));
                                        flame::end("has conflict info");
                                    }
                                    self.deps_.insert(cand_tid.clone());
                                    return Some((info_ptr.clone(), cfl_pid.clone()));

                            },
                            PieceState::Executed | PieceState::Persisted => {
                                self.deps_.insert(cand_tid.clone());
                            }
                        }

                        #[cfg(feature = "profile")]
                        {
                            flame::end(format!("with instance [{:?}]", cand_tid));
                        }


                    }

                    #[cfg(feature = "profile")]
                    {
                        flame::end(format!("conflict with [{}:{:?}]", cfl_name, cfl_pid));
                    }

                }


                #[cfg(feature = "profile")]
                {
                    flame::end("has conflict info");
                }

                None
            },
            None => None 
        }

    }
    //TODO:
    //pub fn prepare_log();
    #[cfg_attr(feature = "profile", flame)]
    pub fn register_txn(&mut self) {
        info!("register_txn:: Registering txn : {:?}", self.id());

        let regis_ptr = TxnRegistry::get_thread_registry();
        let mut regis = regis_ptr.write();

        let pids  = self.all_ps_.iter()
            .map(|piece| piece.id().clone())
            .collect();

        let txn_info = TxnInfo::new(pids, self.id().clone());

        (*regis).register(self.name().clone(), self.id().clone(), Arc::new(RwLock::new(txn_info)));
    }
    
    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_txn(&mut self) {
        self.status_ = TxState::ACTIVE;
        while let Some(mut piece) = self.get_next_piece() { 
            //info!("execute:txn :: Got piece - {:?}", piece); 
            let res = self.can_run(&piece);

            match res{
                None => {
                    self.execute_piece(&mut piece); 
                }, 
                Some((info, cfl_pid)) => {
                    self.spin_on(&mut piece, info, cfl_pid);
                    self.add_wait(piece);
                }
            }
        }

        self.wait_for_dep();
        self.commit();
    }
    
    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_piece(&self, piece : &mut Piece) {
        warn!("execute_piece::[{:?}] Running piece - {:?}", self.id(), piece);
        let regis_ptr = TxnRegistry::get_thread_registry();
        {
            let regis = regis_ptr.read();
            let pid = piece.id().clone();

            let info_ptr = (*regis).get_info_by_id(self.id()).expect("execute_piece:: info should not be none");

            let mut info = info_ptr.write();
            (*info).update_state(pid.clone(), PieceState::Running);
        } //unlock registry here

        #[cfg(feature = "profile")]
        flame::start("piece.run");

        piece.run();

        #[cfg(feature = "profile")]
        flame::end("piece.run");

        {
            let regis = regis_ptr.read();
            let pid = piece.id().clone();

            let info_ptr = (*regis).get_info_by_id(self.id()).expect("execute_piece:: info should not be none");
            let mut info = info_ptr.write();
            (*info).update_state(pid, PieceState::Executed);
        }
    }

    pub fn id(&self) -> &Tid {
        &self.id_
    }

    pub fn name(&self) -> String {
        self.name_.clone()
    }

    pub fn status(&self) -> &TxState {
        &self.status_
    }

    pub fn get_next_piece(&mut self) -> Option<Piece> {
        self.wait_.take().or_else(||self.all_ps_.pop_front())
    }

    pub fn add_dep(&mut self, tid : Tid) {
        self.deps_.insert(tid);
    }

    pub fn has_next_piece(&self) -> bool {
        !self.all_ps_.is_empty()
    }

    pub fn add_wait(&mut self, p : Piece) {
        self.wait_ = Some(p)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn add_piece(&mut self, piece : Piece) {
        self.all_ps_.push_back(piece)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn commit(&mut self) {
        tcore::BenchmarkCounter::success();
        let regis_ptr = TxnRegistry::get_thread_registry();
        let mut regis = regis_ptr.write();

        (*regis).checkout(self.name(), self.id().clone()).expect("commit:: info is checkouted");
        self.status_ = TxState::COMMITTED;
    }
    
    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_for_dep(&self) {
        let me = self.id();
        for id in self.deps_.iter(){
            loop {
                debug!("wait_for::{:?} Waiting for {:?}",me, id);
                let regis_ptr = TxnRegistry::get_thread_registry();
                let regis = regis_ptr.read();

                if let  None = (*regis).get_info_by_id(id) {
                    break;
                }
            }
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn spin_on(&self, piece : &mut Piece, txn_info : Arc<RwLock<TxnInfo>>, pid: Pid) {
        let me = self.id();
        loop {
            let info_g = txn_info.read();
            info!("spin_on::{:?} Waiting for {:?}, {:?}", me, info_g.id(), pid);
            match *info_g.check_state(&pid) {
                PieceState::Executed | PieceState::Persisted =>  {
                    break; 
                },
                _ => {
                }
            }
        }
    }

}


pub type TxnRegistryPtr = Arc<RwLock<TxnRegistry>>;
thread_local!{
    pub static TXN_REGISTRY : RefCell<TxnRegistryPtr> = RefCell::new(Arc::new(RwLock::new(TxnRegistry::new())));
}



pub struct NameRegistry {
    inner_read : Vec<ReadHandle<Tid, TxState>>,
    inner_write : Vec<WriteHandle<Tid, TxState>> /* FIXME: SRSW for now*/
}

//Both handlers are Send
unsafe impl Sync for NameRegistry {}

//FIXME: to move to a seperate mod
const NAME_REGISTRY_SIZE : usize = 64;

impl ShallowCopy for TxState {
    unsafe fn shallow_copy(&mut self) -> Self {
        *self
    }
}

impl NameRegistry {
    pub fn new() -> NameRegistry {
        let mut reads = Vec::with_capacity(NAME_REGISTRY_SIZE);
        let mut writes = Vec::with_capacity(NAME_REGISTRY_SIZE);

        for i in 0..NAME_REGISTRY_SIZE {
            let (r, w) = evmap::new();
            reads.push(r);
            writes.push(w);
        }

        NameRegistry {
            inner_read : reads,
            inner_write: writes
        }
    }
    
    /* Converts Txn Name to index into the inner data */
    #[inline]
    fn index(txn_name : String) -> usize {
        //FIXME: Hardcoded format now: TXN_xxx
        let (_, id_str) = txn_name.split_at(4);
        id_str.parse::<usize>().expect("index invalid")
    }

    
    //Register a new transaction instance
    pub fn register(&mut self, txn_name : String, tid : Tid) {

    }

    
    //Checkout an existing transaction instance
    pub fn checkout(&mut self, txn_name : String, tid : Tid ) {

    }


    //Get a read handle of the tids
    pub fn lookup(&self, txn_name: String) -> ReadHandle<Tid, TxState> {
       self.inner_read[Self::index(txn_name)].clone()
    }
    
    //Update the state
    pub fn update(&mut self, txn_name: String, tid : Tid, new_state: TxState) {

    }


}

pub struct TxnRegistry {
    pub registry_ : HashMap<String, HashSet<Tid>>,
    pub registry_x : NameRegistry,
    pub instances_ : HashMap<Tid, Arc<RwLock<TxnInfo>>>
}

impl TxnRegistry {
    /* Thread local methods */
    pub fn get_thread_registry() -> TxnRegistryPtr {
        TXN_REGISTRY.with( |ptr| Arc::clone(&ptr.borrow()))
    }

    pub fn set_thread_registry(p : TxnRegistryPtr) {
        TXN_REGISTRY.with( |ref ptr| ptr.replace(p));
    }

    pub fn thread_count() -> usize {
        TXN_REGISTRY.with( |ptr| {
            let  _ptr = ptr.borrow();
            let mut regis = _ptr.read();
            (*regis).count()
        })
    }

    pub fn new() -> TxnRegistry {
        TxnRegistry {
            registry_: HashMap::new(),
            instances_: HashMap::new(),
            registry_x : NameRegistry::new(),
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
            registry_x : NameRegistry::new(),
        }
    }

    pub fn set_names(&mut self, names: Vec<String>)  {
        for name in names.into_iter() {
            self.registry_.insert(name, HashSet::new());
        }
    }

    pub fn count(&self) -> usize {
        self.instances_.len()     
    }

    pub fn register(&mut self,  txn_name:String, tid: Tid, txn_info : Arc<RwLock<TxnInfo>>) {
        self.instances_.insert(tid, txn_info);

        let info_set = self.registry_
            .get_mut(&txn_name)
            .expect(format!("register::txn name is not found {:?}", txn_name).as_str());
        info_set.insert(tid);
    }

    pub fn checkout(&mut self, txn_name :String, tid: Tid) -> Option<Arc<RwLock<TxnInfo>>>{
        self.registry_.entry(txn_name)
            .and_modify(|set| {
                set.remove(&tid);
            });

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


#[derive(Debug)]
pub struct TxnInfo {
    tid_ : Tid,
    ps_info_ : HashMap<Pid, PieceState>,
}


impl TxnInfo {
    pub fn new( pids : Vec<Pid>, tid : Tid) -> TxnInfo {
        let mut ps_info_ = HashMap::new();

        for pid in pids.into_iter() {
            ps_info_.insert(pid, PieceState::Ready);
        }

        TxnInfo{
            tid_ : tid,
            ps_info_
        }
    }

    pub fn check_state(&self, pid : &Pid) -> &PieceState {
        self.ps_info_.get(pid).expect(format!("check_state:: piece missing, pid={:?}", pid).as_str())
    }

    pub fn update_state(&mut self, pid : Pid, state : PieceState) {
        self.ps_info_.entry(pid).and_modify(|e| {*e = state.clone()});
    }

    pub fn id(&self) -> Tid {
        self.tid_
    }
}





