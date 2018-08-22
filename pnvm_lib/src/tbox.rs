use txn::{Tid, TxnInfo};
//use std::cell::RefCell;
use std::{
    ptr::Unique,
    //rc::Rc,
    sync::{Arc, RwLock, atomic::AtomicU32},
};

//nightly
use core::alloc::Layout;

#[cfg(feature = "profile")]
use flame;

use tcore;
use tcore::{ObjectId, TValue, TVersion};
use crossbeam::sync::ArcCell;

#[derive(Debug)]
pub struct TBox<T>
where
    T: Clone,
{
    tvalue_: TValue<T>,
    vers_:   TVersion,
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
    pub fn check(&self, cur_ver: u32) -> bool {
        self.vers_.check_version(cur_ver)
    }

    #[inline]
    pub fn install(&self, val: &T, tid: Tid) {
        self.tvalue_.store(T::clone(val));
        self.vers_.set_version(tid);
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
    pub fn get_ptr(&self) -> *mut T {
        self.tvalue_.get_ptr()
    }

   // pub fn get_addr(&self) -> Unique<T> {
   //     let tvalue = self.tvalue_.read().unwrap();
   //     tvalue.get_addr()
   // }

    pub fn get_layout(&self) -> Layout {
        Layout::new::<T>()
    }

    pub fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.vers_.get_writer_info()
    }

    pub fn set_writer_info(&self, info : Arc<TxnInfo>) {
        self.vers_.set_writer_info(info)
    }

    /* No Trans Access method */
    pub fn raw_read(&self) -> T {
        let tvalue = self.tvalue_.load();
        T::clone(tvalue)
    }

    pub fn raw_write(&mut self, val: T) {
        self.tvalue_.store(val);
    }
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
            vers_:   TVersion {
                last_writer_: AtomicU32::new(0),
                lock_owner_:  AtomicU32::new(0),
                txn_info_: ArcCell::new(Arc::new(TxnInfo::default())),
            },
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
            vers_: TVersion {
                last_writer_ : AtomicU32::new(0),
                lock_owner_: AtomicU32::new(0),
                txn_info_: ArcCell::new(Arc::new(TxnInfo::default())),
            },
        }
    }
}

unsafe impl<T: Clone> Sync for TBox<T>{}
unsafe impl<T: Clone> Send for TBox<T>{}
