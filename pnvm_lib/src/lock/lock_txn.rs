

use std::{
    collections::HashMap,
};

use txn::{self, AbortReason, Tid, TxState, TxnInfo, Transaction};
use tcore::{
    self,
    ObjectId, 
    TTag, 
    TRef, 
    FieldArray,
};




pub struct Transaction2PL {
    tid_ : Tid,
    state_ : TxState,
    refs_ : HashMap<(ObjectId, LockType), Box<dyn TRef>>,
}


impl Transaction2PL {

    pub fn new(id: Tid) -> Transaction2PL {
        Transaction2PL {
            tid_ : id,
            state_ : TxState::EMBRYO,
            refs_ : HashMap::new(),
        }
    }

    fn lock(&mut self, tref: &Box<dyn TRef>, lock_type: LockType) -> bool {
        let me :u32 = self.id().into();
        let id = self.id();
        let oid = *tref.get_id();

        if !self.refs_.contains_key(&(oid, lock_type)) {
            let ok = match lock_type {
                LockType::Read => tref.read_lock(me),
                LockType::Write => tref.write_lock(me),
            };
            if ok {
                self.refs_.insert((oid, lock_type), tref.box_clone());
            }
            ok

        } else {
            true
        }

    }

    fn unlock(&mut self) {
        let me : u32 = self.id().into();
        for ((_id, lock_type), tref) in self.refs_.drain() {
            match lock_type {
                LockType::Read => tref.read_unlock(),
                LockType::Write => tref.write_unlock(me),
            }
        }
    }


   
    //Read the underlying value of the reference
    //Return none when failed locking  
    pub fn read<'a, T:'static+Clone>(&mut self, tref: &'a Box<dyn TRef>) -> Option<&'a T> {
        /* Lock */
        match self.lock(&tref, LockType::Read) {
            true => {
                match tref.read().downcast_ref::<T>() {
                    Some(data) => Some(data),
                    None => panic!("inconsistent type at read"),
                }
            } ,
            false => {
                None
            }
        }
    }

    //Write a value into the underlying reference
    //Return Result.Err if failed
   pub fn write<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T) -> bool {
       match self.lock(&tref, LockType::Write) {
           true => {
               tref.write_through(Box::new(val), self.id().clone());
               //Make records for persist later
               true
           },
           false => {
               false
           }
       }

   }
    
    pub fn write_field<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T, fields: FieldArray) -> bool {

       match self.lock(&tref, LockType::Write) {
           true => {
               //Make records for persist later
               tref.write_through(Box::new(val), self.id().clone());
               true
           },
           false => {
               false
           }
       }
    }


    pub fn id(&self) -> Tid {
        self.tid_
    }
    
    //FIXME: should I randomize the input once abort?
    pub fn abort(&mut self) {
        self.unlock();
    }

    pub fn commit(&mut self) {
        //Unlocks
        self.unlock();
    }


}

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
enum LockType {
    Read,
    Write,
}

