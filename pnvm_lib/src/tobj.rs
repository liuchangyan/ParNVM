use std::sync::{Arc, Mutex};
use std::result;
use txn::Transaction;

//Base trait for all the data structure
pub trait TObject<T> 
    {
    fn lock(&mut self, TTag<T>, &Transaction) -> bool;
    fn check(&self, TTag<T>, &Transaction) -> bool;
    fn install(&mut self, TTag<T>, &Transaction);
    fn unlock(&mut self);
}

pub struct TValue<T> {
    data_: T,
}

//[TODO:]To be optimized later
pub struct TVersion {
    last_writer_: Option<Tid>,
    lock_:        Arc<Mutex<bool>>,
    //lock_owner_:  Option<Tid>,
}

#[derive(PartialEq,Copy, Clone, Debug)]
pub struct Tid(u32);

//TTag is attached with each logical segment (identified by key)
//for a TObject. 
//TTag is a local object to the thread.
pub struct TTag<'a, T: 'a> {
    tobj_ref_:  &'a TObject<T>,
    key_:       u32,
    write_val_: Option<T>,
    read_val_: Option<Tvalue<T>>,
}

impl TVersion {
    pub fn lock(&mut self) -> bool {
        let mut locked = self.lock_.lock().unwrap();
        if *locked {
            false
        } else {
            *locked = true;
            true  
        }
    }

    pub fn unlock(&mut self) {
        let mut locked = self.lock_.lock().unwrap();
        *locked = false;
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

impl<T> TValue<T> {
    pub fn store(&mut self, data: T) {
        self.data_ = data;
    }
}

impl<'a, T> TTag<'a, T>
where
    T: 'a,
{
    pub fn write_value(self) -> T {
        match self.write_val_ {
            Some(t) => t,
            None => panic!("Write Tag Should Have Write Value")
        }
    }

    pub fn has_read(&self) -> bool {
        match self.write_val_ {
            Some(_) => true,
            None => false
        }
    }

}
