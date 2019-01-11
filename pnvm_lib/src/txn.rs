#[allow(unused_imports)]
use std::{
    collections::HashMap,
    rc::Rc,
    cell::RefCell,
    sync::{Arc, RwLock},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    ptr,
};
use tcore::{ObjectId, TTag, TRef, FieldArray, Operation};

//lazy_static! {
//    static ref TXN_RUNNING: Arc<RwLock<HashMap<Tid, bool>>> =
//        { Arc::new(RwLock::new(HashMap::new())) };
//}


thread_local! {
    pub static TID_FAC: Rc<RefCell<TidFac>> = Rc::new(RefCell::new(TidFac::new()));

#[cfg(all(feature = "pmem"))]
    pub static PMEM_FAC: Rc<RefCell<PmemFac>> = Rc::new(RefCell::new(PmemFac::new()));
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


pub trait Transaction {
    fn try_commit(&mut self) -> bool;
    fn read<'b, T: 'static + Clone>(&'b mut self, Box<dyn TRef>) -> &'b T;
    fn write<T:'static + Clone>(&mut self, Box<dyn TRef>, T);
    fn write_field<T:'static + Clone>(&mut self, Box<dyn TRef>, T, FieldArray);
    fn id(&self) -> Tid ;
    fn txn_info(&self) -> &Arc<TxnInfo>;
    fn should_abort(&mut self);
    fn retrieve_tag(&mut self, &ObjectId, Box<dyn TRef>, Operation) -> &mut TTag;

}


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


#[cfg(all(feature = "pmem"))]
pub struct PmemFac {
    pmem_root_: Vec<*mut u8>,
    pmem_offset_: usize,
    pmem_cap_: usize,
    pmem_len_ : usize,
    pmem_root_idx_: usize,
    pmem_page_size_ : usize,
}

#[cfg(all(feature = "pmem"))]
const PMEM_DIR_ROOT : Option<&str> = option_env!("PMEM_FILE_DIR");

#[cfg(all(feature = "pmem"))]
impl PmemFac {
    pub fn new() -> PmemFac {
        PmemFac {
            pmem_root_: vec![ptr::null_mut();16],
            pmem_offset_ : 0,
            pmem_len_ : 0,
            pmem_root_idx_ : 0,
            pmem_cap_: 1 << 28,
            pmem_page_size_ : 1 << 28,
        }
    }
    
    pub fn init() {
        PMEM_FAC.with(|fac| fac.borrow_mut().init_inner())
    }

    fn init_inner(&mut self) {
        let size =  self.pmem_page_size_;
        let path = String::from(PMEM_DIR_ROOT.expect("PMEM_FILE_DIR must be supplierd at compile time"));
    
        let ret = pnvm_sys::mmap_file(path, size) as *mut u8;
        self.pmem_root_[0] = ret;
    }

    //size is number of bytes requested for alloc
    pub fn alloc(size : usize) -> *mut u8 {
        PMEM_FAC.with(|fac| fac.borrow_mut().alloc_inner(size))
    }

    fn alloc_inner(&mut self, size: usize) -> *mut u8 {
        if self.pmem_len_ + size >= self.pmem_cap_  {
            //TODO:
            //re-alloc 
            panic!("pmem_fac resize not implemented");
        }
        
        let idx = self.pmem_root_idx_;
        let offset = self.pmem_len_;
        let ret = unsafe {self.pmem_root_[idx].offset(offset as isize)};

        self.pmem_len_ += size;

        return ret;
    }
}


//FIXME: self implemented Drop?
//#[cfg(all(feature = "pmem", feature = "pdrain"))]
//impl Drop for PmemFac {
//    fn drop(*mut self) {
//        for idx in 0..=self.pmem_root_idx_ {
//            pnvm_sys::unmap(self.pmem_root_[ 
//        }
//    }
//}

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
        let ret = self.next_id_ | ((self.mask_ ) << 24);
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

#[derive(AsRefStr)]
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
    //#[cfg(any(feature = "pmem", feature = "disk"))]
    persist_: AtomicBool,
}

impl Default for TxnInfo {
    fn default() -> Self {
        TxnInfo {
            tid_ : Tid::default(),
            locked_ : AtomicBool::new(false),
            committed_: AtomicBool::new(true),
            //status_ : AtomicUsize::new(TxnStatus::Active as usize),
            rank_ : AtomicUsize::default(),
            //#[cfg(any(feature = "pmem", feature = "disk"))]
            persist_: AtomicBool::new(true), 
        }
    }
}


pub enum TxnStatus {
    Active = 0,
    Committed, //1 
    Aborted = 2,
}

impl TxnInfo {
    pub fn new(tid: Tid) -> TxnInfo {
        TxnInfo {
            tid_ : tid,
            committed_: AtomicBool::new(false),
            //status_ : AtomicUsize::new(TxnStatus::Active as usize),
            rank_ : AtomicUsize::new(0),
            locked_ : AtomicBool::new(false),

            //#[cfg(any(feature = "pmem", feature = "disk"))]
            persist_ : AtomicBool::new(false),
        }
    }

    //#[cfg(any(feature = "pmem", feature = "disk"))]
    pub fn has_persist(&self) -> bool {
        self.persist_.load(Ordering::Acquire)
    }

    pub fn has_commit(&self) -> bool {
        self.committed_.load(Ordering::Acquire)
    }
    
    //if deps just started rank 3
    //  txn ready to start rank 3 must wait for it to complete
    //  txn ready to start rank 2 can safely go
    //  dep.cur_rank  > txn.rank_to_run
    pub fn has_started(&self, rank: usize) -> bool {
        self.rank_.load(Ordering::Acquire) > rank
    }

    pub fn has_lock(&self) -> bool {
        self.locked_.load(Ordering::Acquire)
    }

    pub fn lock(&self) {
        self.locked_.store(true, Ordering::Release);
    }

    pub fn unlock(&self) {
        self.locked_.store(false, Ordering::Release);
    }

    pub fn commit(&self) {
        self.committed_.store(true, Ordering::Release);
    }

    //#[cfg(any(feature = "pmem", feature = "disk"))]
    pub fn persist(&self) {
        self.persist_.store(true, Ordering::Release);
    }

    pub fn start(&self, rank: usize) {
        self.rank_.store(rank, Ordering::Release);
    }

    pub fn id(&self) -> &Tid {
        &self.tid_
    }

    pub fn rank(&self) -> usize {
        self.rank_.load(Ordering::Acquire)
    }

}



