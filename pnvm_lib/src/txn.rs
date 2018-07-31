#[allow(unused_imports)]
use std::{
    collections::HashMap,
    sync::{ RwLock, Arc},
    rc::Rc,
};
use tcore::{self, ObjectId, TObject, TTag};
use plog;




lazy_static! {
    static ref TXN_RUNNING : Arc<RwLock<HashMap<Tid, bool>>> = {
        Arc::new(RwLock::new(HashMap::new()))
    };
}
pub fn mark_commit(tid: Tid) {
    TXN_RUNNING.write()
        .unwrap()
        .remove(&tid)
        .expect("mark_commit : txn not in the map");
}


pub fn mark_start(tid : Tid) {
    TXN_RUNNING.write()
        .unwrap()
        .insert(tid, true)
        .is_none();
}

pub trait Transaction<T>
where T : Clone
{
    fn try_commit(&mut self) -> bool;
    fn write(&mut self, tobj: &TObject<T>, val: T);
    fn read(&mut self, tobj: &TObject<T>) -> T;
    fn notrans_read(tobj: &TObject<T>) -> T ;
    fn notrans_lock(tobj: &TObject<T>, tid: Tid) -> bool;
}



#[derive(PartialEq, Copy, Clone, Debug, Eq, Hash)]
pub struct Tid(u32);

impl Tid {
    pub fn new(id: u32) -> Tid {
        Tid(id)
    }
}

impl Into<u32> for Tid {
    fn into(self) -> u32 {
        self.0
    }
}


impl Default for Tid {
    fn default() -> Tid {
        Tid(0)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TxState {
    COMMITTED,
    EMBRYO,
    ACTIVE,
    ABORTED,
}

pub enum AbortReason {
    Error,
    User,
    FailedLocking,
}
