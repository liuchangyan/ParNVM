use txn::{Tid, TxnInfo};
//use std::cell::RefCell;
use std::{
    //rc::Rc,
    sync::{Arc, atomic::{
        AtomicU32,
        AtomicPtr
    }
    },
    any::Any,
};

//nightly
use core::alloc::Layout;

#[cfg(feature = "profile")]
use flame;

use tcore;
use tcore::{ObjectId, TValue, TVersion,  TRef, BoxRef};
use crossbeam::sync::ArcCell;

#[derive(Debug)]
pub struct TBox<T>
where
    T: Clone,
{
    tvalue_: TValue<T>,
    vers_:   Arc<TVersion>,
    id_:     ObjectId,
}

//impl<T> _TObject<T> for TBox<T>
impl<T> TBox<T>
where
    T: Clone,
{
    /*Commit callbacks*/
    #[inline(always)]
    pub fn lock(&self, tid: Tid) -> bool {
        self.vers_.lock(tid)
    }

    #[inline(always)]
    pub fn check(&self, cur_ver: u32, tid: u32) -> bool {
        self.vers_.check_version(cur_ver, tid)
    }

    #[inline]
    pub fn install(&self, val: &T, tid: Tid) {
        self.tvalue_.store(T::clone(val));
        self.vers_.set_version(tid.into());
    }

    #[inline(always)]
    pub fn unlock(&self) {
        self.vers_.unlock();
    }
    

    #[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn get_data<'a>(&'a self) -> &'a T {
        self.tvalue_.load()
    }

    #[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn get_id(&self) -> &ObjectId {
        &self.id_
    }
    
    #[inline(always)]
    pub fn get_version(&self) -> u32 {
        self.vers_.get_version()
    }

    #[inline(always)]
    pub fn get_ptr(&self) -> *mut u8 {
        self.tvalue_.get_ptr() as *mut u8
    }

   // pub fn get_addr(&self) -> Unique<T> {
   //     let tvalue = self.tvalue_.read().unwrap();
   //     tvalue.get_addr()
   // }

    pub fn get_layout(&self) -> Layout {
        Layout::new::<T>()
    }

    pub fn get_access_info(&self) -> Arc<TxnInfo> {
        self.vers_.get_access_info()
    }


    pub fn set_access_info(&self, info : Arc<TxnInfo>) {
        self.vers_.set_access_info(info)
    }

    /* No Trans Access method */
    pub fn raw_read(&self) -> T {
        let tvalue = self.tvalue_.load();
        T::clone(tvalue)
    }

    pub fn raw_write(&mut self, val: T) {
        self.tvalue_.store(val);
    }

//    pub fn read<'a>(&self, txn : &mut TransactionOCC) -> &'a T {
//
//    }
}

impl<T> TBox<T>
where
    T: Clone,
{
    pub fn new(val: T) -> Arc<TBox<T>> {
        let id;
        unsafe {
            id = tcore::next_id();
        }
        Arc::new(TBox {
            tvalue_: TValue::new(val),
            id_:     id,
            vers_:   Arc::new(TVersion::default()),
        })
    }


    pub fn new_default(val: T) -> TBox<T> {
        let id ;
        unsafe {
            id = tcore::next_id();
        }

        TBox {
            tvalue_ : TValue::new(val),
            id_ : id,
            vers_: Arc::new(TVersion::default()),
        }
    }
}

unsafe impl<T: Clone> Sync for TBox<T>{}
unsafe impl<T: Clone> Send for TBox<T>{}


/* Concrete Types Instances */
impl BoxRef<u32> for Arc<TBox<u32>> {
    fn into_box_ref(self) -> Box<dyn TRef> {
        Box::new(TInt{
            inner_ : self,
            data_ : None,
        })
    }
}


impl BoxRef<u32> for (u32, Arc<TBox<u32>>) {
    fn into_box_ref(self) -> Box<dyn TRef> {
        let (val, tbox) = self;
        Box::new(TInt {
            inner_ : tbox,
            data_ : Some(Box::new(val))
        })
    }
}


#[derive(Debug)]
pub struct TInt {
    inner_: Arc<TBox<u32>>,
    data_ : Option<Box<u32>>,
}
impl TRef for TInt {
    fn install(&self,id: Tid) {
        match self.data_ {
            Some(ref as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("only write should get installed");
            }
        }
    }
    
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        panic!("TInt::get_pmem_addr(): not implemented");
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(TInt {
            inner_: self.inner_.clone(),
            data_ : self.data_.clone(),
        })
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    //Unused
    fn get_field_ptr(&self, _i: usize) -> *mut u8 {
        panic!("tbox not implemented")
    }
    fn get_field_size(&self, _i : usize) -> usize {
        panic!("tbox not implemented")
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, _i : usize) -> *mut u8 {
        panic!("tbox not implemented")
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }
    
    #[cfg(all(feature = "pmem", feature = "pdrain"))]
    fn write(&mut self, val: *mut u8) {
        panic!("not implemtned write-pdrain for TBox");
    }

    #[cfg(not(all(feature = "pmem", feature = "pdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<u32>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("runtime value should be u32")
        }
    }

    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<u32>() {
            Ok(val) => self.inner_.install(&val, tid),
            Err(_) => panic!("runtime value should be u32 at write_throught")
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32, _tid: u32) -> bool {
        self.inner_.check(vers, _tid)
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("int")
    }


    /* For 2 Phase Locking */
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.write_lock(tid)
    }

    fn write_unlock(&self, tid: u32)  {
        self.inner_.vers_.write_unlock(tid)
    }
}

impl TInt {
    pub fn new(inner : Arc<TBox<u32>>) -> Self {
        TInt{
            inner_ : inner,
            data_ : None
        }
    }
}

