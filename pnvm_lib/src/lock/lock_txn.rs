

use std::{

};

use tx::{self, AbortReason, Tid, TxState, TxnInfo, Transaction};
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
    refs_ : Vec<Box<dyn TRef>>,
}


impl Transaction2PL {

    pub new(id: Tid) -> Transaction2PL {
        Transaction2PL {
            tid_ : id,
            state_ : TxState::EMBRYO,
            refs_ : Vec::with_capacity(32),
        }
    }

    fn lock(&mut self, tref: &Box<dyn TRef>, lock_type: LockType) -> bool {
        let me :u32 = self.id().into();
        let id = self.id();

        loop {
            match tref.lock(id, lock_type) {
                me => {
                    return true; 
                },
                other => {
                    if me < other {
                        return false 
                    } else {
                        /* Keep spinning */
                    }
                }
            }
        }
    }
   
    //Read the underlying value of the reference
    //Return none when failed locking  
    pub fn read<T:'static+Clone>(&mut self, tref: Box<dyn TRef>) -> Option<&T> {

        /* Lock */
        match self.lock(&tref, LockType::Read) {
            true => {
                Some(tref.read())
            } ,
            false => {
                None
            }
        }
    }

    //Write a value into the underlying reference
    //Return Result.Err if failed
   // pub fn write<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T) -> Result {

   // }
    
    pub fn write_field<T:'static + Clone)(&mut self, tref: Box<dyn TRef>, val: T, fields: FieldArray) -> Result {

        

    }


    pub fn id(&self) -> Tid {
        self.tid_
    }
    
    //FIXME: should I randomize the input once abort?
    pub fn abort(&mut self) {
        //Unlocks

        //Clean up
    }

    pub fn commit(&mut self) {
        //Unlocks
        

        //Clena up
    }


    fn unlock(&mut self) {

    }
}

enum LockType {
    Read,
    Write,
}

