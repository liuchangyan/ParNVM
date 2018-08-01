use txn::Tid;
//use std::cell::RefCell;
use std::{
    ptr::Unique,
    //rc::Rc,
    sync::{Arc, RwLock},
};

//nightly
use core::alloc::Layout;

use tcore;
use tcore::{ObjectId, TValue, TVersion};

#[derive(Debug)]
pub struct TBox<T>
where
    T: Clone,
{
    tvalue_: RwLock<TValue<T>>,
    vers_:   RwLock<TVersion>,
    id_:     ObjectId,
}

//impl<T> _TObject<T> for TBox<T>
impl<T> TBox<T>
where
    T: Clone,
{
    /*Commit callbacks*/
    pub fn lock(&self, tid: Tid) -> bool {
        let mut vers = self.vers_.write().unwrap();
        vers.lock(tid)
    }

    pub fn check(&self, tid: &Option<Tid>) -> bool {
        let vers = self.vers_.read().unwrap();
        vers.check_version(tid)
    }

    pub fn install(&self, val: T, tid: Tid) {
        let mut tvalue = self.tvalue_.write().unwrap();
        let mut vers = self.vers_.write().unwrap();
        tvalue.store(val);
        vers.set_version(tid);
    }

    pub fn unlock(&self) {
        let mut vers = self.vers_.write().unwrap();
        vers.unlock();
    }

    pub fn get_data(&self) -> T {
        let tvalue = self.tvalue_.read().unwrap();
        T::clone(tvalue.load())
    }

    pub fn get_id(&self) -> ObjectId {
        self.id_
    }

    pub fn get_version(&self) -> Option<Tid> {
        let vers = self.vers_.read().unwrap();
        vers.get_version()
    }

    pub fn get_ptr(&self) -> *mut T {
        let tvalue = self.tvalue_.read().unwrap();
        tvalue.get_ptr()
    }

    pub fn get_addr(&self) -> Unique<T> {
        let tvalue = self.tvalue_.read().unwrap();
        tvalue.get_addr()
    }

    pub fn get_layout(&self) -> Layout {
        Layout::new::<T>()
    }

    /* No Trans Access method */
    pub fn raw_read(&self) -> T {
        let tvalue = self.tvalue_.read().unwrap();
        T::clone(tvalue.load())
    }

    pub fn raw_write(&mut self, val: T) {
        let mut tvalue = self.tvalue_.write().unwrap();
        tvalue.store(val);
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
            tvalue_: RwLock::new(TValue::new(val)),
            id_:     id,
            vers_:   RwLock::new(TVersion {
                last_writer_: None,
                lock_owner_:  None,
            }),
        })
    }
}
