#[allow(unused_imports)]
use std::{
    sync::{Arc, Mutex, RwLock,
        atomic::{AtomicU32, Ordering}
    },
    ops::Deref,
    any::Any,
};

use crossbeam::sync::ArcCell;

//use std::rc::Rc;
//use std::cell::RefCell;
use tbox::TBox;
use txn::{Tid, TxnInfo};

#[allow(unused_imports)]
use std::{
    self,
    cell::{RefCell,UnsafeCell},
    fmt, mem,
    ptr::Unique,
    rc::Rc,
    sync::{Once, ONCE_INIT},
    time,
};

//use std::alloc::{self, GlobalAlloc};

#[cfg(not(feature = "pmem"))]
use core::alloc::Layout;

#[cfg(feature = "pmem")]
use pnvm_sys::{
    self, Alloc, AllocErr, Layout, MemKind, PMem, PMEM_DEFAULT_SIZE, PMEM_FILE_DIR_BYTES,
};

#[cfg(feature = "profile")]
use flame;

#[cfg(feature = "pmem")]
use plog::PLog;

//#[cfg(benchmark)]
thread_local!{
    pub static COUNTER: RefCell<BenchmarkCounter> = RefCell::new(BenchmarkCounter::new());
}

//#[cfg(benchmark)]
#[derive(Copy, Clone, Debug)]
pub struct BenchmarkCounter {
    pub success_piece_cnt: u32,
    pub abort_piece_cnt:   u32,
    pub success_cnt: u32,
    pub abort_cnt:   u32,
    pub new_order_cnt : u32,
    pub duration:    time::Duration,
    pub start : time::Instant,
}

//#[cfg(benchmark)]
impl BenchmarkCounter {
    pub fn new() -> BenchmarkCounter {
        BenchmarkCounter {
            success_cnt: 0,
            abort_cnt:   0,
            success_piece_cnt: 0, 
            abort_piece_cnt: 0,
            new_order_cnt : 0,
            start:    time::Instant::now(),
            duration: time::Duration::default(),
        }
    }

    #[inline(always)]
    pub fn success() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).success_cnt += 1;
        });
    }

    #[inline(always)]
    pub fn success_piece() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).success_piece_cnt += 1;
        });
    }

    #[inline(always)]
    pub fn abort_piece() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).abort_piece_cnt += 1;
        });
    }

    pub fn new_order_done() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).new_order_cnt +=1;
        });
    }

    #[inline(always)]
    pub fn start() {
        COUNTER.with(|c| c.borrow_mut().start = time::Instant::now())
    }

    #[inline(always)]
    pub fn abort() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).abort_cnt += 1;
        });
    }

    //#[inline(always)]
    //pub fn set_duration(dur: time::Duration) {
    //    COUNTER.with(|c| (*c.borrow_mut()).duration = dur)
    //}

    #[inline(always)]
    pub fn copy() -> BenchmarkCounter {
        COUNTER.with(|c| {
            let mut g = c.borrow_mut();
            let dur = g.start.elapsed();
            g.duration = dur;
            *g
        })
    }

    #[inline(always)]
    pub fn add_time(dur: time::Duration) {
        COUNTER.with(|c| (*c.borrow_mut()).duration += dur)
    }
}

//pub type TObject<T> = Arc<TBox<T>>;


pub trait BoxRef<T> {
    fn into_box_ref(self) -> Box<dyn TRef>;
}


pub trait TRef : fmt::Debug{
    fn get_ptr(&self) -> *mut u8;
    fn get_layout(&self) -> Layout;
    fn install(&self, id: Tid);
    fn box_clone(&self) -> Box<dyn TRef>;
    fn get_id(&self) -> &ObjectId;
    fn get_version(&self) -> u32;
    fn read(&self) -> &Any;
    fn write(&mut self, Box<Any>);
    fn lock(&self, Tid) -> bool;
    fn unlock(&self);
    fn check(&self, u32, u32) -> bool;
    fn get_writer_info(&self) -> Arc<TxnInfo>;
    fn set_writer_info(&mut self, Arc<TxnInfo>);
    fn get_name(&self) -> String;

    #[cfg(feature = "pmem")]
    fn get_pmem_addr(&self) -> *mut u8;
}



//Base trait for all the data structure
//Using trait object cannot be derefed
//when wrapped with Arc
//pub type TObject<T> = Arc<_TObject<T>>;

//pub trait _TObject<T>
//where T: Clone
//    {
//    fn lock(&mut self, Tid) -> bool;
//    fn check(&self, &Option<Tid>) -> bool;
//    fn install(&mut self, T, Tid);
//    fn unlock(&mut self);
//    fn get_id(&self) -> ObjectId;
//    fn get_data(&self) -> T;
//    fn get_version(&self) -> Option<Tid>;
//
//    //For debug
//    fn raw_read(&self) -> T;
//    fn raw_write(&mut self, T) ;
//}

#[derive(PartialEq, Copy, Clone, Debug, Eq, Hash)]
pub struct ObjectId(u64);

//[TODO:]To be optimized later
#[derive(Debug)]
pub struct TVersion {
    pub last_writer_ : AtomicU32,
    //lock_:        Arc<Mutex<bool>>,
    pub lock_owner_: AtomicU32, 
    pub txn_info_ : ArcCell<TxnInfo>, /* Info of the last writer's txn_ info */
    pub count_ : AtomicU32, /* This to allow multiple times of locking */
}


impl TVersion {
    pub fn new_with_info(txn_info: Arc<TxnInfo>) -> TVersion {
        TVersion {
            last_writer_ : AtomicU32::new(txn_info.id().into()),
            lock_owner_ : AtomicU32::new(0),
            txn_info_: ArcCell::new(txn_info),
            count_: AtomicU32::new(0),
        }
    }

    #[inline(always)]
    pub fn lock(&self, tid: Tid) -> bool{
        let tid : u32 = tid.into();
        assert_eq!(tid != 0 , true);
        match self.lock_owner_.compare_exchange(0, tid, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_cur) => {
                assert_eq!(self.count_.load(Ordering::SeqCst), 0);
                self.count_.fetch_add(1, Ordering::SeqCst);
                true
            },
            Err(cur) => {
                if cur == tid {
                    assert_eq!(self.count_.load(Ordering::SeqCst) > 0, true);
                    self.count_.fetch_add(1, Ordering::SeqCst);
                    true
                } else {
                    false /* Lock by others */
                }
            }
        }
    }

    //Caution: whoever has access to self can unlock
    #[inline(always)]
    pub fn unlock(&self) {
        if self.count_.fetch_sub(1, Ordering::SeqCst) == 1 {
            assert_eq!(self.lock_owner_.load(Ordering::SeqCst) != 0,  true);
            assert_eq!(self.count_.load(Ordering::SeqCst), 0);
            self.lock_owner_.store(0, Ordering::SeqCst);
        }
    }

    pub fn get_locker(&self) -> u32 {
        self.lock_owner_.load(Ordering::Relaxed)
    }

    pub fn get_count(&self) -> u32 {
        self.count_.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn check_version(&self, cur: u32, tid: u32) -> bool {
        ((self.lock_owner_.load(Ordering::Acquire) == 0 || 
          self.lock_owner_.load(Ordering::Acquire) == tid)
         && self.last_writer_.load(Ordering::Acquire) == cur)

       // if locker != 0 {
       //     return locker;
       // } else {
       //     let writer = self.last_writer_.load(Ordering::Acquire);
       //     return 
       // }
       // match (tid, self.last_writer_, self.lock_owner_) {
       //     (Some(ref cur_tid), Some(ref tid), None) => {
       //         if *cur_tid == *tid {
       //             true
       //         } else {
       //             false
       //         }
       //     }
       //     (None, None, None) => true,
       //     (_, _, _) => false,
       // }
    }

    //What if the last writer is own? -> Extension
    //#[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn get_version(&self) -> u32 {
        self.last_writer_.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_version(&self, tid: u32) {
        self.last_writer_.store(tid, Ordering::Release)
    }

    #[inline(always)]
    pub fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.txn_info_.get()
    }

    #[inline(always)]
    pub fn set_writer_info(&self, txn_info : Arc<TxnInfo>) {
        self.txn_info_.set(txn_info);
    }
}


impl Default for TVersion {
    fn default() -> Self {
        TVersion{
            last_writer_ : AtomicU32::new(0),
            lock_owner_ : AtomicU32::new(0),
            txn_info_: ArcCell::new(Arc::new(TxnInfo::default())),
            count_: AtomicU32::new(0),
        }
    }
}

#[derive(Debug)]
pub struct TValue<T>
where
    T: Clone,
{
    //ptr_: Unique<T>,
    data_ : UnsafeCell<T>,
}

impl<T> TValue<T>
where
    T: Clone,
{
    pub fn new(val: T) -> TValue<T> {
        TValue {
            data_ : UnsafeCell::new(val),
        }
    }
    pub fn store(&self, data: T) {
        unsafe {*self.data_.get() = data};
        //unsafe { self.ptr_.as_ptr().write(data) };
    }

    pub fn load(&self) -> &T {
        //unsafe { self.ptr_.as_ref() }
        unsafe { &*self.data_.get()}
    }

    pub fn get_ptr(&self) -> *mut T {
        self.data_.get()
    }

    //pub fn get_addr(&self) -> Unique<T> {
    //    self.ptr_
    //}

    //FIXME::This is super dangerous...
    //But it might be a feasible option. Wrapping the underlying data with
    //Rc<RefCell<T>> could be a way to pass the data as a ref all
    //the way up to the user. A main intended advantage is to avoid
    //copying the underlying data.
    //However, there seems to be no direct methods that place
    //data from a pointer to a refcell.
    //
    //pub fn get_ref(&self) -> Rc<T> {
    //    unsafe { Rc::from_raw(self.ptr_.as_ref()) }
    //}
}

//impl<T> Drop for TValue<T>
//where
//    T: Clone,
//{
//    fn drop(&mut self) {
//        unsafe { alloc::dealloc(self.ptr_.as_ptr() as *mut u8, Layout::new::<T>()) }
//    }
//}


//#[derive(PartialEq, Eq, Hash)]
pub struct TTag
{
    //pub tobj_ref_: TObject<T>,
    pub tobj_ref_: Box<dyn TRef>,
    pub oid_:      ObjectId,
    //write_val_:    Option<Box<Any>>,
    pub has_write_: bool,
    is_lock_ : bool,
    pub vers_:     u32, /* 0 means empty */

    //for debug
    pub name_ : String,
}

impl TTag
{
    pub fn new(oid: ObjectId, tobj_ref: Box<dyn TRef>) -> Self {
        TTag {
            oid_:       oid,
            name_ : tobj_ref.get_name(),
            tobj_ref_:  tobj_ref,
            //write_val_: None,
            vers_:      0,
            has_write_: false,
            is_lock_: false,
        }
    }
    
    /* Only called after has_write() true arm */
   // #[inline(always)]
   // #[cfg_attr(feature = "profile", flame)]
   // pub fn write_value(&self) -> &Any {
   //    // match self.write_val_ {
   //    //     Some(ref t) => t,
   //    //     None => panic!("Write Tag Should Have Write Value"),
   //    // }
   //   // match self.write_val_
   //   //     .as_ref()
   //   //     .expect("write non null")
   //   //     .downcast_ref::<T>() {
   //   //         Some(t_ref) => {
   //   //              t_ref
   //   //         }, 
   //   //         None => panic!("wrong type at write_value")
   //   // }
   //     self.tobj_ref_.read()
   // }

   // pub fn write_value_any(&self) -> &Box<Any> {
   //     self.write_val_.as_ref().expect("write non null")
   // }

    pub fn commit_data(&mut self, id: Tid) {
        if !self.has_write() {
            return;
        }

        self.tobj_ref_.install(id);
    }

    pub fn get_data<T:'static+ Clone>(&self) -> &T {
        match self.tobj_ref_.read().downcast_ref::<T>() {
            Some(t_ref) => t_ref, 
            None => panic!("inconsistent data {:?}", self)
        }

    }

    pub fn lock(&mut self, tid: Tid) -> bool {
        if self.tobj_ref_.lock(tid) {
            self.is_lock_ = true;
            true
        } else {
            false
        }
    }

    pub fn is_lock(&self) -> bool {
        self.is_lock_
    }


    pub fn unlock(&mut self) {
        self.tobj_ref_.unlock();
        self.is_lock_ = false;
    }

    pub fn check(&self, vers: u32, tid: u32) -> bool {
        self.tobj_ref_.check(vers, tid)
    }

    pub fn set_write(&mut self) {
        self.has_write_ = true;
    }


    //#[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn has_write(&self) -> bool {
        self.has_write_
    }

    #[inline(always)]
    pub fn has_read(&self) -> bool {
        !self.has_write()
    }

    //#[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn add_version(&mut self, vers: u32) {
        self.vers_ = vers;
    }

    pub fn get_version(&self) -> u32 {
        self.tobj_ref_.get_version()
    }

    #[inline(always)]
    pub fn write<T: 'static>(&mut self, val: T) {
        let val = Box::new(val); 
        self.tobj_ref_.write(val);
        self.has_write_ = true; 
    }
    
    //FIXME: pmem flush
    #[cfg(feature = "pmem")]
    pub fn persist_data(&self, _: Tid) {
        if !self.has_write() {
            return;
        }

        let pmemaddr = self.tobj_ref_.get_pmem_addr();
        pnvm_sys::memcpy_nodrain(pmemaddr,
                                 self.tobj_ref_.get_ptr(),
                                 self.tobj_ref_.get_layout().size());
    }

    #[cfg(feature = "pmem")]
    pub fn make_log(&self, id: Tid) -> PLog {
        PLog::new(
            self.tobj_ref_.get_ptr() as *mut u8,
            self.tobj_ref_.get_layout(),
            id,
        )
    }

    #[cfg(feature= "pmem")]
    pub fn make_record(&self) -> (*mut u8, *mut u8, Layout) {
        (self.tobj_ref_.get_pmem_addr(),
        self.tobj_ref_.get_ptr(),
        self.tobj_ref_.get_layout())
    }
}

impl fmt::Debug for TTag
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TTag {{  Oid: {:?} ,  Vers : {:?}}}",
            self.oid_, self.vers_
        )
    }
}


/* Object ID factory */
static mut OBJECTID: u64 = 1;
pub unsafe fn next_id() -> ObjectId {
    let ret = OBJECTID;
    OBJECTID += 1;
    ObjectId(ret)
}

thread_local! {
    pub static OID_FAC : Rc<RefCell<OidFac>> = Rc::new(RefCell::new(OidFac::new()));
}

pub struct OidFac {
    mask_ : u64,
    next_id_ : u64,
}

impl OidFac {

    /* Thread Local methods */
    pub fn set_obj_mask(mask: u64) {
        OID_FAC.with(|fac| fac.borrow_mut().set_mask(mask))
    }

    /* Thread Local methods */
    pub fn get_obj_next() -> ObjectId {
        OID_FAC.with(|fac| fac.borrow_mut().get_next())
    }

    pub fn new() -> OidFac {
        OidFac {
            mask_ : 0,
            next_id_ : 1,
        }
    }

    fn set_mask(&mut self, mask : u64) {
        self.mask_ = mask;
    }

    fn get_next(&mut self) -> ObjectId {
        let ret = self.next_id_ | ((self.mask_ ) << 52);
        self.next_id_ +=1;
        ObjectId(ret)
    }
}

/*
 * Persistent Memory Allocator
 */
//#[cfg(feature = "pmem")]
//static mut G_PMEM_ALLOCATOR: PMem = PMem {
//    kind: 0 as *mut MemKind,
//    size: 0,
//};
//
//#[cfg(feature = "pmem")]
//fn get_pmem_allocator() -> PMem {
//    unsafe {
//        if G_PMEM_ALLOCATOR.kind as u32 == 0 {
//            G_PMEM_ALLOCATOR =
//                PMem::new_bytes_with_nul_unchecked(PMEM_FILE_DIR_BYTES, PMEM_DEFAULT_SIZE);
//        }
//        G_PMEM_ALLOCATOR
//    }
//}
//
//#[cfg(feature = "pmem")]
//pub struct GPMem;
//
//#[cfg(feature = "pmem")]
//unsafe impl GlobalAlloc for GPMem {
//    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
//        let mut pmem = get_pmem_allocator();
//        pmem.alloc(layout).unwrap()
//    }
//
//    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
//        let mut pmem = get_pmem_allocator();
//        pmem.dealloc(ptr, layout)
//    }
//}



