

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




pub struct Transaction2PL {
    tid_ : Tid,
    state_ : TxState,
    refs_ : HashMap<(ObjectId, LockType), Arc<TVersion>>,
    fields_ : HashMap<ObjectId, FieldArray>,
    txn_info_ : Arc<TxnInfo>,
}


impl Transaction2PL {

    pub fn new(id: Tid) -> Transaction2PL {
        Transaction2PL {
            tid_ : id,
            state_ : TxState::EMBRYO,
            refs_ : HashMap::new(),
            fields_ : HashMap::new(),
            txn_info_: Arc::new(TxnInfo::default()),
        }
    }

    pub fn lock_tref(&mut self, tref: &Box<dyn TRef>, lock_type: LockType) -> bool {
        let me :u32 = self.id().into();
        let id = self.id();
        let oid = *tref.get_id();

        if !self.refs_.contains_key(&(oid, lock_type)) {
            let ok = match lock_type {
                LockType::Read => tref.read_lock(me),
                LockType::Write => tref.write_lock(me),
            };

            if ok {
                self.refs_.insert((oid, lock_type), tref.get_tvers().clone());
            }
            ok

        } else {
            true
        }

    }

    fn unlock(&mut self) {
        let me : u32 = self.id().into();
        info!("{} is unlocking", me);
        for ((_id, lock_type), vers) in self.refs_.drain() {
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
               self.fields_.insert(*tref.get_id(), fields);
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
        self.unlock();
    }

    pub fn commit(&mut self) {
        //Unlocks
        BenchmarkCounter::success();
        self.unlock();
    }


}

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub enum LockType {
    Read,
    Write,
}

