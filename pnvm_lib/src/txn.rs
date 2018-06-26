//use std::sync::Arc;    
use std::rc::Rc;
//use std::cell::RefCell;
//use std::thread;
//use deps::Dep;
use tobj::{TTag, TObject, ObjectId};
use std::collections::HashMap;

#[derive(PartialEq,Copy, Clone, Debug)]
pub struct Tid(u32);

impl Tid {
    pub fn new(id: u32) -> Tid {
        Tid(id)
    }
}


pub struct Transaction<T>
where T: Clone
   {
        tid_:   Tid,
        state_: TxState,
        deps_:  HashMap<ObjectId, TTag<T>>
    }

    impl<T> Transaction<T>
    where T: Clone
    {
        pub fn new(tid_: Tid) -> Transaction<T> {
            Transaction {
                tid_,
                state_: TxState::EMBRYO,
                deps_ : HashMap::new(),
            }
        }

        pub fn commit_id(&self) -> Tid {
            self.tid_
        }
        

        pub fn try_commit(&mut self) -> bool {
            println!("Tx[{:?}] is commiting", self.tid_);
            self.state_ = TxState::COMMITTED;

            //Stage 1: lock [TODO: Bounded lock or try_lock syntax]

            //Stage 2: Check 
            
            //Stage 3: Commit 
            
            true

        }

        pub fn abort(&mut self, _: AbortReason) -> bool {
            println!("Tx[{:?}] is aborting.", self.tid_);
            self.state_ = TxState::ABORTED;
            true
        }


       // pub fn write<T>(&mut self, tobj: &TObject<T>, val: T) {
       //     let tobj = Rc::clone(tobj);

       //     if !self.try_lock(tobj) {
       //         self.abort();
       //     }
       //     
       // }

        pub fn read(&mut self, tobj: &TObject<T>) -> Result<T, AbortReason> {
            let tobj = Rc::clone(tobj);

            if !self.try_lock(&tobj) {
                return Err(AbortReason::AbortReasonError);
            }

            let _tobj = tobj.borrow();
            let id = _tobj.get_id(); 
            let tag  = self.retrieve_tag(id);
            if tag.has_write() {
                Ok(tag.write_value())
            } else {
                Ok(_tobj.get_data())
            }
        }

        pub fn try_lock(&mut self, tobj : &TObject<T>) -> bool {
            match tobj.try_borrow_mut() {
                Ok(mut _tobj) => {
                    _tobj.lock(self.commit_id()) //TODO
                },
                Err(_) => {
                    false
                }
            }
        }

        pub fn retrieve_tag(&mut self, id: ObjectId) -> &TTag<T> {
            self.deps_.entry(id)
                .or_insert(TTag::new(id, None))
        }



    }

    pub enum TxState {
        COMMITTED,
        EMBRYO,
        ACTIVE,
        ABORTED,
    }


    pub enum AbortReason {
        AbortReasonError,
        AbortReasonUser,
    }



