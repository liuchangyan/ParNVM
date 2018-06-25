use std::sync::Arc;    
use std::cell::RefCell;
use std::thread;
use deps::Dep;
use tobj::{TVersion, TTag, TObject};
use std::collections::HashSet;

#[derive(PartialEq,Copy, Clone, Debug)]
pub struct Tid(u32);

pub struct Transaction<'a, T>
    {
        tid_:   Tid,
        state_: TxState,
        deps_:  HashSet<TTag<'a,T>>,
    }

    impl<'a, T> Transaction<'a, T>
    {
        pub fn new(tid_: Tid) -> Transaction {
            Transaction {
                tid_,
                state_: TxState::EMBRYO,
                deps_ : vec![],
            }
        }

        pub fn commit_id(&self) -> Tid {
            self.tid_
        }
        
        //pub fn add_deps(&mut self, keys: Vec<String>) -> bool {
        //    for key in keys {
        //        if let Some(_) = self.deps_.borrow_mut().add(key, 1) {
        //            return false;
        //        }
        //    }
        //    true
        //}

        //pub fn execute(&mut self) -> bool {
        //    println!("Tx[{:?}] is executing", self.tid_);
        //    self.state_ = TxState::ACTIVE;
        //    (self.fn_)();
        //    true
        //}

        pub fn commit(&mut self) -> bool {
            println!("Tx[{:?}] is commiting", self.tid_);
            self.state_ = TxState::COMMITTED;

            //Stage 1: lock [TODO: Bounded lock or try_lock syntax]
            for tag in self.tags {
                tag.tobj_ref_.lock(tag, &self); 
            }

            //Stage 2: Check 
            
            //Stage 3: Commit 
            
            true

        }

        pub fn abort(&mut self, _: AbortReason) -> bool {
            println!("Tx[{:?}] is aborting.", self.tid_);
            self.state_ = TxState::ABORTED;
            true
        }


        pub fn write<T>(&mut self, tobj: &TObject<T>, val: T) {
            let tobj = Rc::clone(tobj);

            if !self.try_lock(tobj) {
                self.abort();
            }
            //Lock acquired on the object
            let tag = txn::tag(self, tobj);
            tag.write_value(val);
        }

        pub fn read(&mut self, tobj: &TObject<T>) -> T {
            let tobj = Rc::clone(tobj);

            if !self.try_lock(tobj) {
                self.abort();
            }

            let _tobj = tobj.borrow();
            let id = _tobj.get_id();
            if self.has_tag(oid) {
                let tag = self.get_tag(oid);
                tag.read_value()
            } else {
                //Make tag
                //return read value
            }

        }

        pub fn try_lock(&self, tobj : TObject<T>) -> bool {
            match tobj.try_borrow_mut() {
                Ok(_tobj) => {
                    *_tobj.lock(self.commit_id) //TODO
                },
                Err(_) => {
                    false
                }
            }
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


//TODO:
//1.get the transaction from thread_local
//2.check if the TTag exists 
//3.create and add to txn's deps or return accordingly
pub fn tag<'a, T>(txn : &mut Transaction, tobj : &TObject<T>) -> &TTag<'a, T> {


}



