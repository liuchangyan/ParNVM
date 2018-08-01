
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
    collections::{ HashSet, HashMap},
    sync::{Arc},
    cell::{RefCell},
    thread,
};

use parking_lot::RwLock;
use crossbeam::sync::ArcCell;

use log;
use evmap::{self, ReadHandle, WriteHandle, ShallowCopy};

#[cfg(feature = "profile")]
use flame;



#[derive(Clone, Debug)]
pub struct TransactionParBase {
    conflicts_ : Dep, 
    all_ps_ : Vec<Piece>,
    name_ : String,
}

impl TransactionParBase {
    pub fn new(conflicts : Dep, all_ps: Vec<Piece>, name: String) -> TransactionParBase {
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
    all_ps_ : Vec<Piece>,
    deps_ : HashSet<(String, Tid)>,
    id_ : Tid,
    name_ : String ,
    status_: TxState,
    wait_ : Option<Piece>,
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
    pub fn can_run(&mut self, piece : &Piece) -> Option<(String,Tid, Pid)> {
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
                let txn_regis = &*regis_ptr;
                    
                //Each conflict txn
                for conflict in conflicts.iter() {

                    let cfl_name = &conflict.txn_name_;
                    let cfl_pid = &conflict.piece_id_;
                    
                        
                    let info= txn_regis.lookup(cfl_name);
                    let cfl_tid = info.id();

                    #[cfg(feature = "profile")]
                    {
                        flame::start(format!("conflict with [{}:{:?}:{:?}]", cfl_name,cfl_tid, cfl_pid));
                    }

                    //the instance has checkout out => next conflict
                    if info.is_empty() {
                        #[cfg(feature = "profile")]
                        {
                            flame::end(format!("conflict with [{}:{:?}:{:?}]", cfl_name,cfl_tid, cfl_pid));
                        }

                        continue; 
                    }

                    match info.check_state(cfl_pid) {
                        PieceState::Ready => {
                            if self.deps_.contains(&(cfl_name.clone(), cfl_tid.clone())) {
                                #[cfg(feature = "profile")]
                                {
                                    flame::end(format!("conflict with [{}:{:?}:{:?}]", cfl_name,cfl_tid, cfl_pid));
                                    flame::end("has conflict info");
                                }
                                return Some((cfl_name.clone(), cfl_tid.clone(), cfl_pid.clone()));
                            }
                        },
                        PieceState::Running => {
                            #[cfg(feature = "profile")]
                            {
                                flame::end(format!("conflict with [{}:{:?}:{:?}]", cfl_name,cfl_tid, cfl_pid));
                                flame::end("has conflict info");
                            }

                            self.deps_.insert((cfl_name.clone(),cfl_tid.clone()));
                            return Some((cfl_name.clone(), cfl_tid.clone(), cfl_pid.clone()));

                        },
                        PieceState::Executed | PieceState::Persisted => {
                            self.deps_.insert((cfl_name.clone(), cfl_tid.clone()));
                        }
                    }

                    #[cfg(feature = "profile")]
                    {
                        flame::end(format!("conflict with [{}:{:?}:{:?}]", cfl_name,cfl_tid, cfl_pid));
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
        let me = self.id();
        let regis = TxnRegistry::get_thread_registry();
        (*regis).register(self.name(), me.clone(), self.all_ps_.len());
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
                Some((cfl_name, cfl_tid, cfl_pid)) => {
                    self.spin_on(&mut piece, cfl_name, cfl_tid, cfl_pid); /* FIXME: too many clones */
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
        let regis = TxnRegistry::get_thread_registry();
        let pid = piece.id().clone();
        {
            let info = (*regis).update(self.name(), &pid, PieceState::Running);
        } //unlock registry here

        #[cfg(feature = "profile")]
        flame::start("piece.run");

        piece.run();

        #[cfg(feature = "profile")]
        flame::end("piece.run");

        {
            let info = (*regis).update(self.name(), &pid, PieceState::Executed);
        }
    }

    pub fn id(&self) -> &Tid {
        &self.id_
    }

    pub fn name(&self) -> &String {
        &self.name_
    }

    pub fn status(&self) -> &TxState {
        &self.status_
    }

    pub fn get_next_piece(&mut self) -> Option<Piece> {
        self.wait_.take().or_else(||self.all_ps_.pop())
    }

    pub fn add_dep(&mut self,name: String, tid : Tid) {
        self.deps_.insert((name, tid));
    }

    pub fn has_next_piece(&self) -> bool {
        !self.all_ps_.is_empty()
    }

    pub fn add_wait(&mut self, p : Piece) {
        self.wait_ = Some(p)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn add_piece(&mut self, piece : Piece) {
        self.all_ps_.push(piece)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn commit(&mut self) {
        tcore::BenchmarkCounter::success();
        let regis = TxnRegistry::get_thread_registry();

        (*regis).checkout(self.name());
        self.status_ = TxState::COMMITTED;
    }
    
    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_for_dep(&self) {
        let me = self.id();
        for (name, id) in self.deps_.iter(){
            loop {
                debug!("wait_for::{:?} Waiting for {:?}",me, id);
                let regis = TxnRegistry::get_thread_registry();
                
                let info = regis.lookup(name);
                if info.is_empty() {
                    break;
                }

                if info.id() != id {
                    break;
                }
            }
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn spin_on(&self, piece : &mut Piece, txn_name: String, tid: Tid , pid: Pid) {
        let me = self.id();
        let txn_regis = TxnRegistry::get_thread_registry();
        loop {
            let info = txn_regis.lookup(&txn_name);

            //the instance has checkout out
            if info.is_empty() {
                break;
            }
            info!("spin_on::{:?} Waiting for {:?}, {:?}", me, info.id(), pid);

            //Another transaction instance has started
            if *info.id() != tid {
                break;
            }

            match info.check_state(&pid) {
                PieceState::Executed | PieceState::Persisted =>  {
                    break; 
                },
                _ => {
                }
            }
        }
    }

}


pub type TxnRegistryPtr = Arc<TxnRegistry>;
thread_local!{
    pub static TXN_REGISTRY : RefCell<TxnRegistryPtr> = RefCell::new(Arc::new(TxnRegistry::new()));
}



//FIXME: to move to a seperate mod
const REGISTRY_SIZE : usize = 64;

pub struct TxnRegistry {
    registry_: Vec<ArcCell<TxnInfo>>
}


//Both handlers are Send
//unsafe impl Sync for TxnRegistry {}

impl ShallowCopy for TxState {
    unsafe fn shallow_copy(&mut self) -> Self {
        *self
    }
}


/* Converts Txn Name to index into the inner data */
#[inline]
fn txn_index(txn_name : &String) -> usize {
    //FIXME: Hardcoded format now: TXN_xxx
    let (_, id_str) = txn_name.split_at(4);
    id_str.parse::<usize>().expect("index invalid")
}

#[inline]
fn txn_name(tid : &Tid) -> String {
    let tid :u32 =  (*tid).into();
    format!("TXN_{}", tid).clone()
}




impl TxnRegistry {
    /* Thread local methods */
    pub fn get_thread_registry() -> TxnRegistryPtr {
        TXN_REGISTRY.with( |ptr| Arc::clone(&ptr.borrow()))
    }

    pub fn set_thread_registry(p : TxnRegistryPtr) {
        TXN_REGISTRY.with( |ref ptr| ptr.replace(p));
    }


    pub fn new() -> Self {
        let mut registry_ = Vec::with_capacity(REGISTRY_SIZE);

        for _ in 0..REGISTRY_SIZE {
            registry_.push(ArcCell::new(Arc::new(TxnInfo::empty())));
        }

        TxnRegistry {
            registry_
        }
    }

    
    //Register a new transaction instance
    pub fn register(&self, txn_name : &String, tid: Tid, piece_cnt: usize) {
        let id = txn_index(txn_name);  
        let new_info = TxnInfo::new(tid, piece_cnt);
        
        self.registry_[id].set(Arc::new(new_info));
    }

    
    //Checkout an existing transaction instance
    pub fn checkout(&self, txn_name : &String) {
        let id = txn_index(txn_name);
        self.registry_[id].set(Arc::new(TxnInfo::empty()));
    }


    //Get a read handle of the tids
    pub fn lookup(&self, txn_name: &String) -> Arc<TxnInfo> {
       self.registry_[txn_index(txn_name)].get()
    }
    
    //Update the state
    #[cfg_attr(feature = "profile", flame)]
    pub fn update(&self, txn_name: &String, pid : &Pid, new_state: PieceState) {
        let id = txn_index(&txn_name);
        let old = self.lookup(txn_name);
        
        //ISSUE: if the clone here is too costly, might consider using evmap
        if old.is_empty() {
            panic!("state should not be empty");
        }

        let mut new = (*old).clone();
        new.update_state(pid, new_state);
        self.registry_[id].set(Arc::new(new));
    }

}

//    pub fn thread_count() -> usize {
//        TXN_REGISTRY.with( |ptr| {
//            let  _ptr = ptr.borrow();
//            let mut regis = _ptr.read();
//            (*regis).count()
//        })
//    }
//    pub fn new_with_names(names : Vec<String>) -> TxnRegistry {
//        panic!("deprecated");
//        //TxnRegistry {
//        //    instances_ :HashMap::new(),
//        //    registry_x : NameRegistry::new(),
//        //}
//    }
//
//    pub fn set_names(&mut self, names: Vec<String>)  {
//        panic!("deprecated");
//        //for name in names.into_iter() {
//        //    self.registry_.insert(name, HashSet::new());
//        //}
//    }
//
//    pub fn count(&self) -> usize {
//        panic!("deprecated");
//        1
//    }
//
//    pub fn register(&mut self,  txn_name:String, tid: Tid, txn_info : TxnInfo) {
//        self.instances_.init_instance(txn_name, tid, txn_info);
//        self.registry_x.register(txn_name, tid);
//    }
//
//    pub fn checkout(&mut self, txn_name :String, tid: Tid) {
//        self.registry_x.checkout(txn_name, tid);
//        self.instances_.remove_instance(txn_name, tid);
//    }
//
//    pub fn get_info_by_id(&self, tid : &Tid) -> Option<&Arc<RwLock<TxnInfo>>> {
//        panic!("deprecated");
//    }
//
//    pub fn update_info(&self, txn_name: String, tid: Tid, pid: Pid, new_state: PieceState) {
//        self.instances_.set_info(txn_name, tid, pid,new_state);
//    }


//pub struct InstanceRegistry {
//    inner_read: Vec<ReadHandle<Tid, ArcCell<TxnInfo>>>,
//    inner_write: Vec<WriteHandle<Tid, ArcCell<TxnInfo>>>, //FIXME: do not have multiple writers
//}
//
//impl InstanceRegistry {
//    pub fn new() -> InstanceRegistry {
//        let mut reads = Vec::with_capacity(REGISTRY_SIZE);
//        let mut writes = Vec::with_capacity(REGISTRY_SIZE);
//
//        for i in 0..REGISTRY_SIZE {
//            let (r, w) = evmap::new();
//            reads.push(r);
//            writes.push(w);
//        }
//
//        InstanceRegistry {
//            inner_read : reads,
//            inner_write: writes
//        }
//    }
//    
//    //Called when an instance is added 
//    pub fn init_instance(&self,txn_name: String,  tid : Tid, txn_info : TxnInfo) {
//        let id = index(&txn_name);
//        let w = &mut self.inner_write[id];
//        w.insert(tid, ArcCell::new(txn_info));
//
//        //FIXME: can i not refresh here? => Then i will be visible from the NameRegstr
//        w.refresh();
//    }
//    
//    pub fn remove_instance(&self, txn_name: String, tid: Tid) {
//        let id = index(&txn_name);
//        let w = &mut self.inner_write[id];
//        w.empty(&tid);
//        //No refresh here since it doesn't really matter
//    }
//    
//    //Get an Arc of TxnInfo
//    pub fn get_info(&self, txn_name: &String, tid : &Tid) -> Option<Arc<TxnInfo>> {
//        let id = index(txn_name);
//        let r = &self.inner_read[id];
//        r.get_and(&tid, |info| {
//            info.get() 
//        })
//    }
//
//    
//    //Take a pointer to the TxnInfo
//    pub fn set_info(&self, txn_name: String, tid: Tid, pid: Pid,state: PieceState) {
//        let id = index(&txn_name);
//        let r = &self.inner_read[id];
//
//        r.get_and(&tid, |acell| {
//            let temp_arc = acell.get();
//            let new_arc = Arc::new(TxnInfo::update_state_from_arc(temp_arc, pid, state));
//            acell.set(new_arc);
//        });
//    }
//
//}


#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct TxnName {
    name : String    
}


#[derive(Debug, Clone)]
pub struct TxnInfo {
    tid_ : Tid,
    ps_info_ : HashMap<Pid, PieceState>,
    empty_ : bool,
}


impl TxnInfo {
    pub fn new(tid : Tid, size: usize) -> TxnInfo {
        let mut ps_info_ = HashMap::new();

        for pid in 0..size {
            ps_info_.insert(Pid::new(pid as u32), PieceState::Ready);
        }

        TxnInfo{
            tid_ : tid,
            ps_info_,
            empty_ : false,
        }
    }

    pub fn empty() -> TxnInfo {
        TxnInfo {
            tid_ : Tid::default(),
            ps_info_ : HashMap::new(),
            empty_: true,
        }
    }

    pub fn check_state(&self, pid : &Pid) -> &PieceState {
        self.ps_info_.get(pid).expect(format!("check_state:: piece missing, pid={:?}", pid).as_str())
    }

    pub fn update_state(&mut self, pid : &Pid, state : PieceState) {
        self.ps_info_.entry(pid.clone()).and_modify(|e| {*e = state.clone()});
    }

    pub fn id(&self) -> &Tid {
        &self.tid_
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.empty_
    }
}





