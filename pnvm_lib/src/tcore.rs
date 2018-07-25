#[allow(unused_imports)]
use std::sync::{ Arc, RwLock,Mutex};
//use std::rc::Rc;
//use std::cell::RefCell;
use tbox::TBox;
use txn::{Tid};

#[allow(unused_imports)]
use std::{
    self,
    fmt,
    ptr::Unique,
    mem,
    rc::Rc,
    time,
    cell::RefCell,
};
use pnvm_sys::{
    self,
    Layout,
};

use plog::PLog;


/* Module Level Exposed Function Calls */

pub fn init() {
    //init the pmem
    pnvm_sys::init(); 
}


//#[cfg(benchmark)]
thread_local!{
    pub static COUNTER: RefCell<BenchmarkCounter> = RefCell::new(BenchmarkCounter::new());
}


//#[cfg(benchmark)]
#[derive(Copy, Clone, Debug)]
pub struct BenchmarkCounter {
    pub success_cnt: u32,
    pub abort_cnt : u32, 
    pub duration: time::Duration
}

//#[cfg(benchmark)]
impl BenchmarkCounter {
    pub fn new() -> BenchmarkCounter {
        BenchmarkCounter{
            success_cnt : 0,
            abort_cnt: 0,
            duration : time::Duration::default(),
        }
    }

    #[inline]
    pub fn success() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).success_cnt+=1;
        });
    }

    #[inline]
    pub fn abort() {
        COUNTER.with(|c| {
            (*c.borrow_mut()).abort_cnt+=1;
        });
    }

    #[inline]
    pub fn set_duration(dur: time::Duration) {
        COUNTER.with(|c| (*c.borrow_mut()).duration = dur)
    }

    #[inline]
    pub fn copy() -> BenchmarkCounter {
        COUNTER.with(|c| {
            *c.borrow()
        })
    }

    #[inline]
    pub fn add_time(dur : time::Duration) {
        COUNTER.with( |c| (*c.borrow_mut()).duration += dur)
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


#[derive(PartialEq,Copy, Clone, Debug, Eq, Hash)]
pub struct ObjectId(u32);


//[TODO:]To be optimized later
#[derive(Debug)]
pub struct TVersion {
    pub last_writer_: Option<Tid>,
    //lock_:        Arc<Mutex<bool>>,
    pub lock_owner_:  Option<Tid>
    //lock_owner_:  Option<Tid>,
}


//TTag is attached with each logical segment (identified by key)
//for a TObject. 
//TTag is a local object to the thread.

impl TVersion {
    pub fn lock(&mut self, tid: Tid) -> bool {
        match self.lock_owner_ {
            Some(ref cur_owner) => {
                if *cur_owner != tid  {
                    false
                } else {
                    true
                }
            },
            None => {
                self.lock_owner_ = Some(tid);
                true
            }
        }
    }
    

    //Caution: whoever has access to self can unlock
    pub fn unlock(&mut self) {
        self.lock_owner_ = None;
    }

    pub fn check_version(&self, tid: &Option<Tid>) -> bool {
        trace!("--- [Checking Version] {:?} <-> {:?}", tid, self.last_writer_);
        //let lock_owner = self.lock_owner_.lock().unwrap();
        match (tid, self.last_writer_, self.lock_owner_) {
            (Some(ref cur_tid), Some(ref tid), None) => {
                if *cur_tid == *tid {
                    true
                } else {
                    false 
                }
            },
            (None, None, None)  => true,
            (_ , _, _) => false
        }
    }

    //What if the last writer is own? -> Extension
    pub fn get_version(&self) -> Option<Tid> {
        self.last_writer_ 
    }

    pub fn set_version(&mut self, tid: Tid) {
        self.last_writer_ = Some(tid);
    }
}

#[derive(Debug)]
pub struct TValue<T>
where T:Clone {
    ptr_: Unique<T>,
}

impl<T> TValue<T> 
where T:Clone
{
    pub fn new(val :T) -> TValue<T> {
        let ptr = pnvm_sys::alloc(Layout::new::<T>());

        match ptr {
            Ok(ptr) => {
                let ptr = unsafe {
                    mem::transmute::<*mut u8, *mut T>(ptr)
                };
                unsafe {ptr.write(val)};
                TValue{ 
                    ptr_ : Unique::new(ptr).expect("Tvalue::new failed"),
                }
            },
            Err(_) => panic!("Tvalue::new failed")
        }
    }
    pub fn store(&mut self, data: T) {
        unsafe {self.ptr_.as_ptr().write(data) };
    }

    pub fn load(&self) -> &T {
        unsafe {self.ptr_.as_ref()}
    }

    pub fn get_ptr(&self) -> *mut T {
        self.ptr_.as_ptr()
    }

      pub fn get_addr(&self) -> Unique<T> {
        self.ptr_
    }
   

    //FIXME::This is super dangerous...
    //But it might be a feasible option. Wrapping the underlying data with 
    //Rc<RefCell<T>> could be a way to pass the data as a ref all
    //the way up to the user. A main intended advantage is to avoid 
    //copying the underlying data. 
    //However, there seems to be no direct methods that place
    //data from a pointer to a refcell. 
    //
    pub fn get_ref(&self) -> Rc<T> {
        unsafe {Rc::from_raw(self.ptr_.as_ref())}        
    }
}

impl<T> Drop for TValue<T>
where T:Clone 
{
    fn drop(&mut self) 
    {
         pnvm_sys::dealloc(self.ptr_.as_ptr() as *mut u8, Layout::new::<T>())
    }
}

//#[derive(PartialEq, Eq, Hash)]
pub struct TTag<T> 
where T:Clone
{
    pub tobj_ref_:  TObject<T>,
    pub oid_:   ObjectId,
    write_val_: Option<T>,
    pub vers_ : Option<Tid>
}

impl<T> TTag<T>
where T:Clone
{
    pub fn new(oid: ObjectId, tobj_ref: TObject<T>) -> Self {
        TTag{
            oid_: oid,
            tobj_ref_ : tobj_ref,
            write_val_: None,
            vers_ : None
        }
    }

    pub fn write_value(&self) -> T {
        match self.write_val_ {
            Some(ref t) => T::clone(t),
            None => panic!("Write Tag Should Have Write Value")
        }
    }

    pub fn commit_data(&mut self, id : Tid) {
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

    pub fn has_write(&self) -> bool {
        match self.write_val_ {
            Some(_) => true,
            None => false
        }
    }

    pub fn has_read(&self) -> bool {
        !self.has_write()
    }

    pub fn add_version(&mut self, vers: Option<Tid>) {
        self.vers_ = vers;
    }

    pub fn write(&mut self, val : T) {
        self.write_val_ = Some(val)
    }

    pub fn persist_data(&self, _: Tid) {
        if !self.has_write() {
            return;
        }
        pnvm_sys::flush((*self.tobj_ref_).get_ptr() as *mut u8, Layout::new::<T>());
    }

    pub fn make_log(&self, id : Tid) -> PLog<T> {
        PLog::new(&Arc::clone(&self.tobj_ref_), id )
    }
}

impl<T> fmt::Debug for TTag<T> 
where T : Clone
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TTag {{  Oid: {:?} ,  Vers : {:?}}}", self.oid_,  self.vers_)
    }
}


static mut OBJECTID: u32 = 1;
pub unsafe fn next_id() -> ObjectId {
    let ret = OBJECTID;
    OBJECTID += 1;
    ObjectId(ret)
}




