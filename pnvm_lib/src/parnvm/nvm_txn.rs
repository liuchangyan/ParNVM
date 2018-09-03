use txn::{self, Tid, AbortReason, TxState, TxnInfo};
use tcore::{self, ObjectId, TRef, TTag};

use super::dep::*;
use super::piece::*;
use plog::{self, PLog};



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
};

#[cfg(feature="pmem")]
use core::alloc::Layout;
extern crate pnvm_sys;


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

#[derive(Clone, Debug)]
pub struct TransactionParBaseOCC
{
    all_ps_:    Vec<PieceOCC>,
    name_:      String,
}

impl TransactionParBaseOCC
{
    pub fn new(all_ps: Vec<PieceOCC>, name: String) -> TransactionParBaseOCC {
        TransactionParBaseOCC {
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

#[derive(Default)]
pub struct TransactionParOCC
{
    all_ps_:    Vec<PieceOCC>,
    deps_:      HashMap<u32, Arc<TxnInfo>>,
    id_:        Tid,
    name_:      String,
    status_:    TxState,
    txn_info_:  Arc<TxnInfo>,
    wait_:      Option<PieceOCC>,

    #[cfg(feature="pmem")]
    records_ :     Vec<(Option<*mut u8>, Layout)>,
    
    tags_ : HashMap<ObjectId, TTag>,
    locks_ : Vec<*const TTag>,
    
}


impl TransactionParOCC
{
    pub fn new(pieces : Vec<PieceOCC>, id : Tid, name: String) -> TransactionParOCC {
        TransactionParOCC {
            all_ps_:    pieces,
            deps_:      HashMap::with_capacity(DEP_DEFAULT_SIZE),
            id_:        id,
            name_:      name,
            status_:    TxState::EMBRYO,
            wait_:      None,
            txn_info_:  Arc::new(TxnInfo::new(id)),
            #[cfg(feature="pmem")]
            records_ :     Vec::new(),

            tags_: HashMap::with_capacity(16),
            locks_ : Vec::with_capacity(16),
        }
    }

    pub fn new_from_base(txn_base: &TransactionParBaseOCC, tid: Tid) -> TransactionParOCC
    {
        let txn_base = txn_base.clone();
        TransactionParOCC {
            all_ps_:    txn_base.all_ps_,
            name_:      txn_base.name_,
            id_:        tid,
            status_:    TxState::EMBRYO,
            deps_:      HashMap::with_capacity(DEP_DEFAULT_SIZE),
            txn_info_:  Arc::new(TxnInfo::new(tid)),
            wait_:      None,
            #[cfg(feature="pmem")]
            records_ :     Vec::new(),

            tags_: HashMap::with_capacity(16),
            locks_ : Vec::with_capacity(16),
        }
    }




    #[cfg_attr(feature = "profile", flame)]
    pub fn execute_piece(&mut self, mut piece: PieceOCC) {
        info!(
            "execute_piece::[{:?}] Running piece - {:?}",
            self.id(),
            &piece
        );
        
        while {
            //Any states created in the run must be reset
            piece.run(self);
            let res = self.try_commit_piece(piece.rank());
            !res
        } {}
    }

    /* Implement OCC interface */
    pub fn read<'a, T:'static +Clone>(&'a mut self, tobj: &'a dyn TRef) -> &'a T {
        let tag = self.retrieve_tag(tobj.get_id(), tobj.box_clone());
        tag.add_version(tobj.get_version());
        tag.get_data()
    }

    pub fn write<T: 'static + Clone>(&mut self, tobj: &dyn TRef, val : T) {
        let tag = self.retrieve_tag(tobj.get_id(), tobj.box_clone());
        tag.write::<T>(val);
    }

    #[inline(always)]
    fn retrieve_tag(&mut self, id: &ObjectId, tobj_ref : Box<dyn TRef>) -> &mut TTag {
        self.tags_.entry(*id).or_insert(TTag::new(*id, tobj_ref))
    }

    
    fn add_dep(&mut self) {
        for (_, tag) in self.tags_.iter() {
            let txn_info = tag.tobj_ref_.get_writer_info();
            if !txn_info.has_commit() {
                let id : u32= txn_info.id().into();
                if !self.deps_.contains_key(&id) {
                    self.deps_.insert(id, txn_info);
                } 
            }
        }
    }

    pub fn try_commit_piece(&mut self, rank: usize) -> bool {
        if !self.lock() {
            return self.abort_piece(AbortReason::FailedLocking);
        }

        if !self.check() {
            return self.abort_piece(AbortReason::FailedLocking);
        }

        self.add_dep();

        self.commit_piece(rank)
    }

    fn abort_piece(&mut self, _ : AbortReason) -> bool {
        self.clean_up();
        false
    }

    fn commit_piece(&mut self, rank: usize) -> bool {

        #[cfg(feature = "pmem")]
        self.persist_log();


        //Install write sets into the underlying data
        self.install_data();

        //Persist the data
        #[cfg(feature = "pmem")]
        self.persist_data();

        //Persist commit the transaction
        #[cfg(feature = "pmem")]
        self.persist_commit();

        self.update_rank(rank);

        //Clean up local data structures.
        self.clean_up();
        
        true
    }

    fn install_data(&mut self) {
        let id = *self.id();
        let txn_info = self.txn_info().clone();
        for tag in self.tags_.values_mut() {
            tag.commit_data(id);
            tag.tobj_ref_.set_writer_info(txn_info.clone()); 
        }
    }

    fn clean_up(&mut self) {
        for (_, tag) in self.tags_.drain() {
            if tag.has_write() {
                tag.tobj_ref_.unlock();
            }
        }
        self.locks_.clear();
    }


    fn lock(&mut self) -> bool {
        let me = *self.id();
        for tag in self.tags_.values() {
            if !tag.has_write() {
                continue;
            }

            if !tag.lock(me) {
                while let Some(_tag) = self.locks_.pop() {
                    //FIXME: Hacky way use raw pointer to eschew lifetime checker
                    unsafe{ _tag.as_ref().unwrap().unlock()};
                } debug!("{:#?} failed to locked!", tag);
                return false;
            } else {
                self.locks_.push(tag as *const TTag);
            }
            debug!("{:#?} locked!", tag);
        }

        true
    }

    fn check(&mut self) -> bool {
        for tag in self.tags_.values() {
            if !tag.has_read() {
                continue;
            }

            if !tag.check(tag.vers_) {
                return false;
            }
        }

        true
    }


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
       for (ptr, layout) in self.records_.drain() {
            if let Some(ptr) = ptr {
                pnvm_sys::flush(ptr, layout.clone());
            }
       }
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

    pub fn get_next_piece(&mut self) -> Option<PieceOCC> {
        self.wait_.take().or_else(|| self.all_ps_.pop())
    }


    pub fn has_next_piece(&self) -> bool {
        !self.all_ps_.is_empty()
    }

    pub fn add_wait(&mut self, p: PieceOCC) {
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
    pub fn add_piece(&mut self, piece: PieceOCC) {
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



/*
 * Locking Protocol
 *
 */

thread_local!{
    pub static CUR_TXN : Rc<RefCell<TransactionPar>> = Rc::new(RefCell::new(Default::default()));

    //Generic default type 
    //pub static CUR_TXN_OCC_PTR: Rc<RefCell<NonNull<TransactionParOCC<u32>>>> = Rc::new(RefCell::new(NonNull::dangling()));
}

const DEP_DEFAULT_SIZE : usize = 128;

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
       for (ptr, layout) in self.records_.drain() {
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

