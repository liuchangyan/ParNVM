use txn::{self, Tid, Transaction, TxState};

use tcore;

use super::dep::*;
use super::piece::*;
use plog::{self, PLog};

use std::{
    cell::{RefCell},
    rc::Rc,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    default::Default,
};

#[cfg(feature="pmem")]
use core::alloc::Layout;
extern crate pnvm_sys;


use crossbeam::sync::ArcCell;
use parking_lot::RwLock;

use evmap::{self, ReadHandle, ShallowCopy, WriteHandle};
use log;

#[cfg(feature = "profile")]
use flame;

#[derive(Clone, Debug)]
pub struct TransactionParBase {
    all_ps_:    Vec<Piece>,
    name_:      String,
}

impl TransactionParBase {
    pub fn new(all_ps: Vec<Piece>, name: String) -> TransactionParBase {
        TransactionParBase {
            all_ps_:    all_ps,
            name_:      name,
        }
    }
}

#[derive(Default)]
pub struct TransactionPar {
    all_ps_:    Vec<Piece>,
    deps_:      HashMap<u32, Arc<TxnInfo>>,
    id_:        Tid,
    name_:      String,
    status_:    TxState,
    txn_info_:  Arc<TxnInfo>,
    wait_:      Option<Piece>,

    #[cfg(feature="pmem")]
    records_ :     Vec<(Option<*mut u8>, Layout)>,
}



thread_local!{
    pub static CUR_TXN : Rc<RefCell<TransactionPar>> = Rc::new(RefCell::new(Default::default()));
}

const DEP_DEFAULT_SIZE : usize = 128;
impl TransactionPar {
    pub fn new(pieces: Vec<Piece>, id: Tid, name: String) -> TransactionPar {
        TransactionPar {
            all_ps_:    pieces,
            deps_:      HashMap::with_capacity(DEP_DEFAULT_SIZE),
            id_:        id,
            name_:      name,
            status_:    TxState::EMBRYO,
            wait_:      None,
            txn_info_:  Arc::new(TxnInfo::new(id)),
            #[cfg(feature="pmem")]
            records_ :     Vec::new(),
        }
    }

    pub fn new_from_base(txn_base: &TransactionParBase, tid: Tid) -> TransactionPar {
        let txn_base = txn_base.clone();

        TransactionPar {
            all_ps_:    txn_base.all_ps_,
            name_:      txn_base.name_,
            id_:        tid,
            status_:    TxState::EMBRYO,
            deps_:      HashMap::with_capacity(DEP_DEFAULT_SIZE),
            txn_info_:  Arc::new(TxnInfo::new(tid)),
            wait_:      None,
            #[cfg(feature="pmem")]
            records_ :     Vec::new(),
        }
    }

    pub fn get_thread_txn() -> Rc<RefCell<TransactionPar>> {
        CUR_TXN.with(|txn| {
            txn.clone()
        })
    }

    pub fn set_thread_txn(tx : TransactionPar) {
        CUR_TXN.with(|txn| *txn.borrow_mut() = tx)
    }

    pub fn register(tx : TransactionPar) {
        Self::set_thread_txn(tx)
    }

    pub fn execute() {
        CUR_TXN.with(|txn| txn.borrow_mut().execute_txn());
    }


    //    can_run(piece)
    //    - check all current deps(tx_y) :
    //        - if tx_y still uncommitted && has not ran the rank < tx_x.cur
    //          => false
    //        - else tx_y still uncommitted && has >= rank  || if tx committed
    //          => go on

    //    <<-- old depdencies satisfied, check new deps now -->>
    //    - Write lock all data(variable granularity)
    //      - if any write-locked || read-pinned
    //          => add to deps
    //          => releases all write locks
    //          => false
    //      - else
    //          => move on
    //    - Read pin all
    //      - if any write-locked by other
    //          => add writer to deps
    //          => releases all read pins  && write locks
    //          => false
    //      - else
    //          => move on
    //
    //    <<-- read/write all "locked" -->>
    //    - return true
    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_deps_start(&self) {
        let cur_rank = self.cur_rank();
        for (_, dep) in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_commit() && !dep.has_done(cur_rank) {
                    warn!("waiting waiting for {:?}", dep.id());
                } else {
                    break;
                }
            }
        }
    }
    
    pub fn cur_rank(&self) -> usize {
        self.txn_info_.rank()
    }


    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_txn(&mut self) {
        self.status_ = TxState::ACTIVE;

        while let Some(piece) = self.get_next_piece() {
            self.wait_deps_start();
            self.execute_piece(piece);

            #[cfg(feature = "pmem")]
            self.persist_data();
        }
        

        //Commit
        self.wait_deps_commit();
        self.commit();
    }
    
    #[cfg(feature = "pmem")]
    pub fn add_record(&mut self, ptr: Option<*mut u8>, layout: Layout) {
        self.records_.push((ptr, layout));
    }

    #[cfg(feature = "pmem")]
    #[cfg_attr(feature = "profile", flame)]
    pub fn persist_logs(&self) {
        let id = *(self.id());
        let logs = self.records_.iter().map(|(ptr, layout)| {
            match ptr {
                Some(ptr) => PLog::new(*ptr, layout.clone(), id),
                None => PLog::new_none(layout.clone(), id),
            }
        }).collect();
        plog::persist_log(logs);
    }

    #[cfg(feature="pmem")]
    pub fn persist_data(&mut self) {
       for (ptr, layout) in self.records_.drain(..) {
            if let Some(ptr) = ptr {
                pnvm_sys::flush(ptr, layout.clone());
            }
       }
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_piece(&mut self, mut piece: Piece) {
        info!(
            "execute_piece::[{:?}] Running piece - {:?}",
            self.id(),
            &piece
        );

        piece.run(self);
        self.update_rank(piece.rank());
    }

    pub fn update_rank(&self, rank: usize) {
        self.txn_info_.done(rank);
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

    pub fn txn_info(&self) -> &Arc<TxnInfo> {
        &self.txn_info_
    }

    pub fn get_next_piece(&mut self) -> Option<Piece> {
        self.wait_.take().or_else(|| self.all_ps_.pop())
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn add_dep(&mut self, tid: u32, txn_info: Arc<TxnInfo>) {
        warn!("add_dep::{:?} - {:?}", self.id(), txn_info);
        if !self.deps_.contains_key(&tid) {
            self.deps_.insert(tid, txn_info);
        } 
    }

    pub fn has_next_piece(&self) -> bool {
        !self.all_ps_.is_empty()
    }

    pub fn add_wait(&mut self, p: Piece) {
        self.wait_ = Some(p)
    }

    #[cfg(feature = "pmem")]
    #[cfg_attr(feature = "profile", flame)]
    fn persist_log(&self, records: &Vec<DataRecord>) {
        let id = self.id();
        plog::persist_log(records.iter().map(|ref r| r.as_log(*id)).collect());
    }

    #[cfg(feature = "pmem")]
    #[cfg_attr(feature = "profile", flame)]
    fn persist_txn(&self) {
        pnvm_sys::drain();
        plog::persist_txn(self.id().into());
        self.txn_info_.persist();
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn add_piece(&mut self, piece: Piece) {
        self.all_ps_.push(piece)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn commit(&mut self) {
        self.txn_info_.commit();
        self.status_ = TxState::COMMITTED;
        tcore::BenchmarkCounter::success();

        #[cfg(feature="pmem")]
        {
            self.wait_deps_persist();
            self.persist_txn();
            self.status_ = TxState::PERSIST;
        }
    }

    #[cfg(feature = "pmem")]
    #[cfg_attr(feature = "profile", flame)]
    fn wait_deps_persist(&self) {
        for (_, dep) in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_persist(){
                    warn!("wait_deps_persist::{:?} waiting for {:?} to commit", self.id(),  dep.id());
                } else {
                    break;
                }
            }
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_deps_commit(&self) {
        for (_, dep) in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_commit(){
                    warn!("wait_deps_commit::{:?} waiting for {:?} to commit", self.id(),  dep.id());
                } else {
                    break;
                }
            }
        }

    }
}



#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct TxnName {
    name: String,
}

#[derive(Debug)]
pub struct TxnInfo {
    tid_ : Tid,
    committed_ : AtomicBool,
    rank_ : AtomicUsize,
    #[cfg(feature = "pmem")]
    persist_: AtomicBool,
}

impl Default for TxnInfo {
    fn default() -> Self {
        TxnInfo {
            tid_ : Tid::default(),
            committed_: AtomicBool::new(true),
            rank_ : AtomicUsize::default(),
            #[cfg(feature = "pmem")]
            persist_: AtomicBool::new(true), 
        }
    }
}


impl TxnInfo {
    pub fn new(tid: Tid) -> TxnInfo {
        TxnInfo {
            tid_ : tid,
            committed_: AtomicBool::new(false),
            rank_ : AtomicUsize::new(0),

            #[cfg(feature = "pmem")]
            persist_ : AtomicBool::new(false),
        }
    }

    #[cfg(feature = "pmem")] 
    pub fn has_persist(&self) -> bool {
        self.persist_.load(Ordering::SeqCst)
    }

    pub fn has_commit(&self) -> bool {
        self.committed_.load(Ordering::SeqCst)
    }

    pub fn has_done(&self, rank: usize) -> bool {
        self.rank_.load(Ordering::SeqCst) > rank
    }

    pub fn commit(&self) {
        self.committed_.store(true, Ordering::SeqCst);
    }

    #[cfg(feature = "pmem")]
    pub fn persist(&self) {
        self.persist_.store(true, Ordering::SeqCst);
    }

    pub fn done(&self, rank: usize) {
        self.rank_.store(rank, Ordering::SeqCst);
    }

    pub fn id(&self) -> &Tid {
        &self.tid_
    }

    pub fn rank(&self) -> usize {
        self.rank_.load(Ordering::SeqCst)
    }
}



