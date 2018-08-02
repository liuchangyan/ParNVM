use txn::{self, Tid, Transaction, TxState};

use tcore;

use super::dep::*;
use super::piece::*;
use plog;

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
    deps_:      Vec<Arc<TxnInfo>>,
    id_:        Tid,
    name_:      String,
    status_:    TxState,
    txn_info_:  Arc<TxnInfo>,
    wait_:      Option<Piece>,
}



thread_local!{
    pub static CUR_TXN : Rc<RefCell<TransactionPar>> = Rc::new(RefCell::new(Default::default()));
}

impl TransactionPar {
    pub fn new(pieces: Vec<Piece>, id: Tid, name: String) -> TransactionPar {
        TransactionPar {
            all_ps_:    pieces,
            deps_:      Vec::new(),
            id_:        id,
            name_:      name,
            status_:    TxState::EMBRYO,
            wait_:      None,
            txn_info_:  Arc::new(TxnInfo::new(id))
        }
    }

    pub fn new_from_base(txn_base: &TransactionParBase, tid: Tid) -> TransactionPar {
        let txn_base = txn_base.clone();

        TransactionPar {
            all_ps_:    txn_base.all_ps_,
            name_:      txn_base.name_,
            id_:        tid,
            status_:    TxState::EMBRYO,
            deps_:      Vec::new(),
            txn_info_:  Arc::new(TxnInfo::new(tid)),
            wait_:      None,
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
        for dep in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_commit() && !dep.has_done(cur_rank) {
                    trace!("waiting waiting for {:?}", dep.id());
                } else {
                    break;
                }
            }
        }
    }
    
    pub fn cur_rank(&self) -> usize {
        1
    }



    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_txn(&mut self) {
        self.status_ = TxState::ACTIVE;

        while let Some(piece) = self.get_next_piece() {
            self.execute_piece(piece);
        }

        //Commit
        self.wait_deps_commit();
        self.commit();
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_piece(&self, mut piece: Piece) {
        warn!(
            "execute_piece::[{:?}] Running piece - {:?}",
            self.id(),
            &piece
        );

        piece.run();
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

    pub fn add_dep(&mut self, txn_info: Arc<TxnInfo>) {
        self.deps_.push(txn_info);
    }

    pub fn has_next_piece(&self) -> bool {
        !self.all_ps_.is_empty()
    }

    pub fn add_wait(&mut self, p: Piece) {
        self.wait_ = Some(p)
    }

    #[cfg(feature = "pmem")]
    fn persist_log(&self, records: &Vec<DataRecord>) {
        let id = self.id();
        plog::persist_log(records.iter().map(|ref r| r.as_log(*id)).collect());
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn add_piece(&mut self, piece: Piece) {
        self.all_ps_.push(piece)
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn commit(&mut self) {
        self.txn_info_.commit();
        self.status_ = TxState::COMMITTED;
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_deps_commit(&self) {
        for dep in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_commit(){
                    trace!("wait_deps_commit\t::\twaiting for {:?} to commit", dep.id());
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

#[derive(Debug, Default)]
pub struct TxnInfo {
    tid_ : Tid,
    committed_ : AtomicBool,
    rank_ : AtomicUsize,
}


impl TxnInfo {
    pub fn new(tid: Tid) -> TxnInfo {
        TxnInfo {
            tid_ : tid,
            committed_: AtomicBool::new(false),
            rank_ : AtomicUsize::new(0),
        }
    }


    pub fn has_commit(&self) -> bool {
        self.committed_.load(Ordering::SeqCst)
    }

    pub fn has_done(&self, rank: usize) -> bool {
        self.rank_.load(Ordering::SeqCst) == rank
    }

    pub fn commit(&self) {
        self.committed_.store(true, Ordering::SeqCst);
    }

    pub fn done(&self, rank: usize) {
        self.rank_.store(rank, Ordering::SeqCst);
    }

    pub fn id(&self) -> &Tid {
        &self.tid_
    }
}
