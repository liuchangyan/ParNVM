use std::sync::Arc;    
use std::cell::RefCell;
use std::thread;
use deps::Dep;
use tobj::{ TVersion, Tid};

pub struct Transaction
    {
        tid_:   Tid,
        state_: TxState,
        deps_:  Arc<RefCell<Dep>>,
        fn_:   Box<FnMut()>,
    }

    impl Transaction
    {
        pub fn new(fn_: Box<FnMut()>, tid_: Tid, deps_ : Arc<RefCell<Dep>>) -> Transaction {
            Transaction {
                tid_,
                state_: TxState::EMBRYO,
                deps_,
                fn_,
            }
        }

        pub fn commit_id(&self) -> Tid {
            self.tid_
        }
        
        pub fn add_deps(&mut self, keys: Vec<String>) -> bool {
            for key in keys {
                if let Some(_) = self.deps_.borrow_mut().add(key, 1) {
                    return false;
                }
            }
            true
        }

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


        //For TBox commit protocol 
        
        //try to lock a tag 
        pub fn lock(&self,  vers :&mut TVersion) -> bool {
            while !vers.lock() {
                thread::yield_now();
            }
            true
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


pub fn tag<'a, T>(tobj: &TObject<T>, id: u32) -> TTag<'a, T> {
    //TODO:
    //1.get the transaction from thread_local
    //2.check if the TTag exists 
    //3.create and add to txn's deps or return accordingly
}

