#[allow(unused_imports)]
use std::{
    collections::HashMap,
    rc::Rc,
    cell::RefCell,
    sync::{Arc, RwLock},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tcore::{self, ObjectId, TTag};

//lazy_static! {
//    static ref TXN_RUNNING: Arc<RwLock<HashMap<Tid, bool>>> =
//        { Arc::new(RwLock::new(HashMap::new())) };
//}


thread_local! {
    pub static TID_FAC: Rc<RefCell<TidFac>> = Rc::new(RefCell::new(TidFac::new()));
}
//pub fn mark_commit(tid: Tid) {
//    TXN_RUNNING
//        .write()
//        .unwrap()
//        .remove(&tid)
//        .expect("mark_commit : txn not in the map");
//}
//
//pub fn mark_start(tid: Tid) {
//    TXN_RUNNING.write().unwrap().insert(tid, true).is_none();
//}
//
//pub trait Transaction<T>
//where
//    T: Clone,
//{
//    fn try_commit(&mut self) -> bool;
//    fn write(&mut self, tobj: &TObject<T>, val: T);
//    fn read<'b>(&'b mut self, tobj: &'b TObject<T>) -> &'b T;
//    fn notrans_read(tobj: &TObject<T>) -> T;
//    fn notrans_lock(tobj: &TObject<T>, tid: Tid) -> bool;
//}


#[derive(PartialEq, Copy, Clone, Debug, Eq, Hash)]
pub struct Tid(u32);

impl Tid {
    pub fn new(id: u32) -> Tid {
        Tid(id)
    }

    pub fn get_thread_tid() -> Tid {
       TID_FAC.with(|tid_fac| tid_fac.borrow_mut().get_next())
    }
}

impl Into<u32> for Tid {
    fn into(self) -> u32 {
        self.0
    }
}

impl<'a> Into<u32> for &'a Tid {
    fn into(self) -> u32 {
        self.0
    }
}
impl Default for Tid {
    fn default() -> Tid {
        Tid(0)
    }
}

pub struct TidFac {
    mask_: u32,
    next_id_ : u32,
}

impl TidFac {
    pub fn set_thd_mask(mask : u32) {
        TID_FAC.with(|fac| fac.borrow_mut().set_mask(mask))
    }
    
    #[inline(always)]
    pub fn get_thd_next() -> Tid {
        TID_FAC.with(|fac| fac.borrow_mut().get_next())
    }

    pub fn new() -> TidFac {
        TidFac {
            mask_: 0,
            next_id_ : 1,
        }
    }

     fn set_mask(&mut self, mask: u32) {
        self.mask_ = mask;
    }

     fn get_next(&mut self) -> Tid {
        let ret = self.next_id_ | ((self.mask_ ) << 16);
        self.next_id_ +=1; 
        Tid::new(ret)
    }
}


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TxState {
    COMMITTED,
    EMBRYO,
    ACTIVE,
    ABORTED,
    PERSIST,
}

impl Default for TxState {
    fn default() -> Self {
        TxState::EMBRYO
    }
}

pub enum AbortReason {
    Error,
    User,
    FailedLocking,
    IndexErr, 
}

#[derive(Debug)]
pub struct TxnInfo {
    tid_ : Tid,
    locked_ : AtomicBool,
    committed_ : AtomicBool,
    rank_ : AtomicUsize,
    #[cfg(feature = "pmem")]
    persist_: AtomicBool,
}

impl Default for TxnInfo {
    fn default() -> Self {
        TxnInfo {
            tid_ : Tid::default(),
            locked_ : AtomicBool::new(false),
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
            locked_ : AtomicBool::new(false),

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

    pub fn has_lock(&self) -> bool {
        self.locked_.load(Ordering::SeqCst)
    }

    pub fn lock(&self) {
        self.locked_.store(true, Ordering::SeqCst);
    }

    pub fn unlock(&self) {
        self.locked_.store(false, Ordering::SeqCst);
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



