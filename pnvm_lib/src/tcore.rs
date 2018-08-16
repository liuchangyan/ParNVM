#[allow(unused_imports)]
use std::{
    sync::{Arc, Mutex, RwLock,
        atomic::{AtomicU32, Ordering}
    },
};

//use std::rc::Rc;
//use std::cell::RefCell;
use tbox::TBox;
use txn::Tid;

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

use std::alloc::{self, GlobalAlloc};

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
    pub success_cnt: u32,
    pub abort_cnt:   u32,
    pub duration:    time::Duration,
}

//#[cfg(benchmark)]
impl BenchmarkCounter {
    pub fn new() -> BenchmarkCounter {
        BenchmarkCounter {
            success_cnt: 0,
            abort_cnt:   0,
            duration:    time::Duration::default(),
        }
    }

    #[inline]
    pub fn success() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).success_cnt += 1;
        });
    }

    #[inline]
    pub fn abort() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).abort_cnt += 1;
        });
    }

    #[inline]
    pub fn set_duration(dur: time::Duration) {
        COUNTER.with(|c| (*c.borrow_mut()).duration = dur)
    }

    #[inline]
    pub fn copy() -> BenchmarkCounter {
        COUNTER.with(|c| *c.borrow())
    }

    #[inline]
    pub fn add_time(dur: time::Duration) {
        COUNTER.with(|c| (*c.borrow_mut()).duration += dur)
    }
}

pub type TObject<T> = Arc<TBox<T>>;
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
pub struct ObjectId(u32);

//[TODO:]To be optimized later
#[derive(Debug)]
pub struct TVersion {
    pub last_writer_ : AtomicU32,
    //lock_:        Arc<Mutex<bool>>,
    pub lock_owner_: AtomicU32, //lock_owner_:  Option<Tid>
}

//TTag is attached with each logical segment (identified by key)
//for a TObject.
//TTag is a local object to the thread.

impl TVersion {
    #[inline]
    pub fn lock(&self, tid: Tid) -> bool {
        let tid : u32 = tid.into();
        let cur = self.lock_owner_.load(Ordering::SeqCst);
        cur == 0 || cur == tid
    }

    //Caution: whoever has access to self can unlock
    #[inline]
    pub fn unlock(&self) {
        self.lock_owner_.store(0, Ordering::SeqCst);
    }

    #[inline]
    pub fn check_version(&self, tid: u32) -> bool {
        //let lock_owner = self.lock_owner_.lock().unwrap();
        if self.lock_owner_.load(Ordering::SeqCst) != 0 {
            false 
        } else {
            self.last_writer_.load(Ordering::SeqCst) == tid
        }
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
    #[cfg_attr(feature = "profile", flame)]
    #[inline]
    pub fn get_version(&self) -> u32 {
        self.last_writer_.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_version(&self, tid: Tid) {
        self.last_writer_.store(tid.into(), Ordering::SeqCst)
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
pub struct TTag<T>
where
    T: Clone,
{
    pub tobj_ref_: TObject<T>,
    pub oid_:      ObjectId,
    write_val_:    Option<T>,
    pub has_write_: bool,
    pub vers_:     u32, /* 0 means empty */
}

impl<T> TTag<T>
where
    T: Clone,
{
    pub fn new(oid: ObjectId, tobj_ref: TObject<T>) -> Self {
        TTag {
            oid_:       oid,
            tobj_ref_:  tobj_ref,
            write_val_: None,
            vers_:      0,
            has_write_: false,
        }
    }
    
    /* Only called after has_write() true arm */
    #[cfg_attr(feature = "profile", flame)]
    pub fn write_value(&self) -> &T {
       // match self.write_val_ {
       //     Some(ref t) => t,
       //     None => panic!("Write Tag Should Have Write Value"),
       // }
       self.write_val_.as_ref().unwrap()
    }

    pub fn commit_data(&mut self, id: Tid) {
        if !self.has_write() {
            return;
        }

        let val = self.write_value();
        (*self.tobj_ref_).install(val, id);
    }

    // pub fn consume_value(&mut self) -> T {
    //     match self.write_val_ {
    //         Some(t) => Rc::try_unwrap(t).ok().unwrap(),
    //         None => panic!("Write Tag Should Have Write Value")
    //     }
    // }

    #[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn has_write(&self) -> bool {
        self.has_write_
    }

    #[inline(always)]
    pub fn has_read(&self) -> bool {
        !self.has_write()
    }

    #[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn add_version(&mut self, vers: u32) {
        self.vers_ = vers;
    }

    pub fn write(&mut self, val: T) {
        self.write_val_ = Some(val);
        self.has_write_ = true; 
    }

    #[cfg(feature = "pmem")]
    pub fn persist_data(&self, _: Tid) {
        if !self.has_write() {
            return;
        }
        pnvm_sys::flush((*self.tobj_ref_).get_ptr() as *mut u8, Layout::new::<T>());
    }

    #[cfg(feature = "pmem")]
    pub fn make_log(&self, id: Tid) -> PLog {
        PLog::new(
            self.tobj_ref_.get_ptr() as *mut u8,
            self.tobj_ref_.get_layout(),
            id,
        )
    }
}

impl<T> fmt::Debug for TTag<T>
where
    T: Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TTag {{  Oid: {:?} ,  Vers : {:?}}}",
            self.oid_, self.vers_
        )
    }
}

static mut OBJECTID: u32 = 1;
pub unsafe fn next_id() -> ObjectId {
    let ret = OBJECTID;
    OBJECTID += 1;
    ObjectId(ret)
}

/*
 * Persistent Memory Allocator
 */
#[cfg(feature = "pmem")]
static mut G_PMEM_ALLOCATOR: PMem = PMem {
    kind: 0 as *mut MemKind,
    size: 0,
};

#[cfg(feature = "pmem")]
fn get_pmem_allocator() -> PMem {
    unsafe {
        if G_PMEM_ALLOCATOR.kind as u32 == 0 {
            G_PMEM_ALLOCATOR =
                PMem::new_bytes_with_nul_unchecked(PMEM_FILE_DIR_BYTES, PMEM_DEFAULT_SIZE);
        }
        G_PMEM_ALLOCATOR
    }
}

#[cfg(feature = "pmem")]
pub struct GPMem;

#[cfg(feature = "pmem")]
unsafe impl GlobalAlloc for GPMem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let mut pmem = get_pmem_allocator();
        pmem.alloc(layout).unwrap()
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let mut pmem = get_pmem_allocator();
        pmem.dealloc(ptr, layout)
    }
}
