use std::sync::{ Mutex};
use std::rc::Rc;
use std::cell::RefCell;
use txn::{Transaction, Tid};

//Base trait for all the data structure
pub type TObject<T> = Rc<RefCell<_TObject<T>>>;

pub trait _TObject<T> 
where T: Clone
    {
    fn lock(&mut self, Tid) -> bool;
    fn check(&self, TTag<T>, &Transaction<T>) -> bool;
    fn install(&mut self, TTag<T>, &Transaction<T>);
    fn unlock(&mut self);
    fn get_id(&self) -> ObjectId;
    fn get_data(&self) -> T;
}

#[derive(PartialEq,Copy, Clone, Debug, Eq, Hash)]
pub struct ObjectId(u32);


//[TODO:]To be optimized later
pub struct TVersion {
    pub last_writer_: Option<Tid>,
    //lock_:        Arc<Mutex<bool>>,
    pub lock_owner_:  Mutex<Option<Tid>>
    //lock_owner_:  Option<Tid>,
}


//TTag is attached with each logical segment (identified by key)
//for a TObject. 
//TTag is a local object to the thread.

impl TVersion {
    pub fn lock(&mut self, tid: Tid) -> bool {
        let mut lock_owner = self.lock_owner_.lock().unwrap();
        let (success, empty) = match *lock_owner {
            Some(ref cur_owner) => {
                if *cur_owner == tid {
                    (true, false)
                } else {
                    (false, false)
                }
            },
            None => {
                (true, true)
            }
        };

        if empty {
            *lock_owner = Some(tid)
        }
        success
    }
    

    //Caution: whoever has access to self can unlock
    pub fn unlock(&mut self) {
        let mut lock_owner = self.lock_owner_.lock().unwrap();
        *lock_owner = None;
    }

    pub fn check_version(&self, tid: Tid) -> bool {
        match self.last_writer_ {
            Some(ref cur_tid) => *cur_tid == tid,
            None => false,
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

pub struct TValue<T>
where T:Clone
{
    pub data_: T,
}

impl<T> TValue<T> 
where T:Clone
{
    pub fn store(&mut self, data: T) {
        self.data_ = data;
    }

    pub fn load(&self) -> T {
        self.data_.clone()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct TTag<T> {
    //tobj_ref_:  &'a TObject<T>,
    pub oid_:       ObjectId,
    write_val_: Option<T>,
}

impl<T> TTag<T>
where T:Clone
{
    pub fn new(oid: ObjectId, write_value: Option<T>) -> Self {
        TTag{
            oid_: oid,
            write_val_: write_value
        }
    }

    pub fn write_value(&self) -> T {
        match self.write_val_ {
            Some(ref t) => T::clone(t),
            None => panic!("Write Tag Should Have Write Value")
        }
    }

    pub fn has_write(&self) -> bool {
        match self.write_val_ {
            Some(_) => true,
            None => false
        }
    }

    pub fn has_read(&self) -> bool {
        !self.has_write()
    }
}


static mut OBJECTID: u32 = 1;
pub unsafe fn next_id() -> ObjectId {
    let ret = OBJECTID;
    OBJECTID += 1;
    ObjectId(ret)
}
