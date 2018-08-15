use plog;
#[allow(unused_imports)]
use std::{
    collections::HashMap,
    rc::Rc,
    cell::RefCell,
    sync::{Arc, RwLock},
};
use tcore::{self, ObjectId, TObject, TTag};

lazy_static! {
    static ref TXN_RUNNING: Arc<RwLock<HashMap<Tid, bool>>> =
        { Arc::new(RwLock::new(HashMap::new())) };
}


thread_local! {
    pub static TID_FAC: Rc<RefCell<TidFac>> = Rc::new(RefCell::new(TidFac::new()));
}
pub fn mark_commit(tid: Tid) {
    TXN_RUNNING
        .write()
        .unwrap()
        .remove(&tid)
        .expect("mark_commit : txn not in the map");
}

pub fn mark_start(tid: Tid) {
    TXN_RUNNING.write().unwrap().insert(tid, true).is_none();
}

pub trait Transaction<T>
where
    T: Clone,
{
    fn try_commit(&mut self) -> bool;
    fn write(&mut self, tobj: &TObject<T>, val: T);
    fn read(&mut self, tobj: &TObject<T>) -> &T;
    fn notrans_read(tobj: &TObject<T>) -> T;
    fn notrans_lock(tobj: &TObject<T>, tid: Tid) -> bool;
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

pub struct TidFac {
    mask_: u32,
    next_id_ : u32,
}

impl TidFac {
    pub fn set_thd_mask(mask : u32) {
        TID_FAC.with(|fac| fac.borrow_mut().set_mask(mask))
    }

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
}

