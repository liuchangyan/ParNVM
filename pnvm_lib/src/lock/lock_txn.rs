

use std::{
    collections::HashMap,
    sync::Arc,
};


use txn::{self, AbortReason, Tid, TxState, TxnInfo, Transaction};
use tcore::{
    self,
    ObjectId, 
    TTag, 
    TRef, 
    FieldArray,
    TVersion,
    BenchmarkCounter,
};

#[cfg(any(feature = "pmem", feature = "disk"))]
use pnvm_sys;

#[cfg(any(feature = "pmem", feature = "disk"))]
use plog::{
    PLog,
    self
};



pub struct Transaction2PL {
    tid_ : Tid,
    state_ : TxState,
    locks_ : HashMap<(ObjectId, LockType), Arc<TVersion>>,
    txn_info_ : Arc<TxnInfo>,
    #[cfg(any(feature = "pmem", feature = "disk"))]
    refs_ : Vec<(Box<dyn TRef>, Option<FieldArray>)>,
    //fields_ : HashMap<ObjectId, FieldArray>,
}


impl Transaction2PL {

    pub fn new(id: Tid) -> Transaction2PL {
        Transaction2PL {
            tid_ : id,
            state_ : TxState::EMBRYO,
            locks_ : HashMap::new(),
            txn_info_: Arc::new(TxnInfo::default()),
            #[cfg(any(feature = "pmem", feature = "disk"))]
            refs_ : Vec::new(),
        }
    }

    pub fn add_locks(&mut self, key: (ObjectId, LockType), val: Arc<TVersion>) {
        self.locks_.insert(key, val);
    }

    pub fn has_lock(&mut self, key: &(ObjectId, LockType)) -> bool {
        self.locks_.contains_key(key)
    }

    pub fn lock_tref(&mut self, tref: &Box<dyn TRef>, lock_type: LockType) -> bool {
        let me :u32 = self.id().into();
        let id = self.id();
        let oid = *tref.get_id();

        if !self.locks_.contains_key(&(oid, lock_type)) {
            let ok = match lock_type {
                LockType::Read => tref.read_lock(me),
                LockType::Write => tref.write_lock(me),
            };

            if ok {
                self.locks_.insert((oid, lock_type), tref.get_tvers().clone());
            }
            ok

        } else {
            true
        }

    }

    fn unlock(&mut self) {
        let me : u32 = self.id().into();
        info!("{} is unlocking", me);
        for ((_id, lock_type), vers) in self.locks_.drain() {
            match lock_type {
                LockType::Read => vers.read_unlock(me),
                LockType::Write => vers.write_unlock(me),
            }
        }
    }


   
    //Read the underlying value of the reference
    //Return none when failed locking  
    pub fn read<'a, T:'static+Clone>(&mut self, tref: &'a Box<dyn TRef>) -> Result<&'a T, ()> {
        /* Lock */
        match self.lock_tref(tref, LockType::Read) {
            true => {
                match tref.read().downcast_ref::<T>() {
                    Some(data) => Ok(data),
                    None => panic!("inconsistent type at read"),
                }
            } ,
            false => {
               Err(()) 
            }
        }
    }

    //Write a value into the underlying reference
    //Return Result.Err if failed
   pub fn write<T:'static + Clone>(&mut self, tref: &Box<dyn TRef>, val: T) 
       -> Result<(), ()> 
       {
       match self.lock_tref(tref, LockType::Write) {
           true => {
               tref.write_through(Box::new(val), self.id().clone());
               self.refs_.push((tref.box_clone(), None));
               //Make records for persist later
               Ok(()) 
           },
           false => {
               Err(()) 
           }
       }

   }
    
    pub fn write_field<T:'static + Clone>(&mut self, tref: &Box<dyn TRef>, val: T, fields: FieldArray) -> Result<(), ()> {
       match self.lock_tref(&tref, LockType::Write) {
           true => {
               //Make records for persist later
               tref.write_through(Box::new(val), self.id().clone());
               //Replace current fields
               self.refs_.push((tref.box_clone(), Some(fields)));
               Ok(())
           },
           false => {
               Err(())
           }
       }
    }


    pub fn id(&self) -> Tid {
        self.tid_
    }

    pub fn txn_info(&self) -> &Arc<TxnInfo> {
        &self.txn_info_
    }
    
    //FIXME: should I randomize the input once abort?
    pub fn abort(&mut self) {
        BenchmarkCounter::abort();
        
        #[cfg(any(feature = "pmem", feature = "disk"))]
        self.refs_.clear();

        self.unlock();
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    pub fn add_ref(&mut self, tref: Box<dyn TRef>) {
        self.refs_.push((tref,None));
    }


    pub fn commit(&mut self) {
        //Unlocks
        #[cfg(any(feature = "pmem", feature = "disk"))]
        {
            self.persist_log();
            self.persist_data();
            self.persist_commit();
        }

        BenchmarkCounter::success();
        self.unlock();
    }
    
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn persist_data(&self) {
        #[cfg(feature = "pmem")]
        for (tref, fields) in self.refs_.iter() {
            match fields {
                Some(fields) => {
                    for field in fields.iter() {
                        let pmemaddr = tref.get_pmem_field_addr(*field);
                        let size = tref.get_field_size(*field);
                        let vaddr =tref.get_field_ptr(*field);
                        BenchmarkCounter::flush(size);
                        pnvm_sys::memcpy_nodrain(pmemaddr, vaddr, size);
                    }
                },
                None => {
                    BenchmarkCounter::flush(tref.get_layout().size());
                    pnvm_sys::memcpy_nodrain(
                        tref.get_pmem_addr(),
                        tref.get_ptr(),
                        tref.get_layout().size());
                }
            }
        }


        #[cfg(feature = "disk")]
        panic!("not impelmented for disk");
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn persist_commit(&self) {
        #[cfg(feature = "pmem")]
        pnvm_sys::drain();

        plog::persist_txn(self.id().into());
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn persist_log(&self) {
        let mut logs = vec![];
        for (tref, _field) in self.refs_.iter() {
            logs.push(PLog::new(
                tref.get_ptr() as *mut u8,
                tref.get_layout(),
                self.id()));

        }

        plog::persist_log(logs);
        
    }




}

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub enum LockType {
    Read,
    Write,
}

