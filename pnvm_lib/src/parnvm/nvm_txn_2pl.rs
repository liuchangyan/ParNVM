/* ********************************************
 * Contains txn methods for the PPNVM
 * that use 2PL locking
 * *************************************
 */


use super::piece::*;
use txn::{self, *};
use tcore::{self, *};

use std::{
    cell::{RefCell},
    rc::Rc,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
    },
    thread,
    default::Default,
    ptr::NonNull,
    any::Any,
};

#[cfg(feature="pmem")]
use {core::alloc::Layout, plog::{self, PLog}};
#[cfg(feature="pmem")]
extern crate pnvm_sys;

use log;

#[cfg(feature = "profile")]
use flame;

const OPERATION_CODE_RW :i8 = 2;
const DEP_DEFAULT_SIZE : usize = 128;


// [Txn with locking as contention management]
// Base struct that contains pieces callbacks. 
// Transaction instances are built on top of this
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
pub struct TransactionPar
{
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

/*
 * Locking Protocol
 *
 */

thread_local!{
    pub static CUR_TXN : Rc<RefCell<TransactionPar>> = Rc::new(RefCell::new(Default::default()));
}


impl TransactionPar
{
    pub fn new(pieces: Vec<Piece>, id: Tid, name: String) -> TransactionPar{
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

    pub fn new_from_base(txn_base: &TransactionParBase, tid: Tid) -> TransactionPar{
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


    #[cfg_attr(feature = "profile", flame)]
    pub fn wait_deps_start(&self) {
        let cur_rank = self.cur_rank();
        for (_, dep) in self.deps_.iter() {
            loop { /* Busy wait here */
                if !dep.has_commit() && !dep.has_done(cur_rank) {
                    warn!("waiting for {:?} start", dep.id());
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

            //#[cfg(feature = "pmem")]
            //self.persist_data();
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

   // #[cfg(feature="pmem")]
   // pub fn persist_data(&mut self) {
   //     for (ptr, layout) in self.records_.drain() {
   //         if let Some(ptr) = ptr {
   //             pnvm_sys::flush(ptr, layout.clone());
   //         }
   //     }
   // }

    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_piece(&mut self, mut piece: Piece) {
        info!(
            "execute_piece::[{:?}] Running piece - {:?}",
            self.id(),
            &piece
        );

        piece.run(self);
        self.commit()
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

