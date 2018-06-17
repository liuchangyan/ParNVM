use std::sync::Arc;    
use std::cell::RefCell;
use super::deps::Dep;

pub struct Transaction<T>
    where
        T: Fn(),
    {
        tid_:   u64,
        state_: TxState,
        deps_:  Arc<RefCell<Dep>>,
        fn_:    T,
    }

    impl<T> Transaction<T>
    where
        T: Fn(),
    {
        pub fn new(fn_: T, tid_: u64, deps_ : Arc<RefCell<Dep>>) -> Transaction<T> {
            Transaction {
                tid_,
                state_: TxState::EMBRYO,
                deps_,
                fn_,
            }
        }
        
        pub fn add_deps(&mut self, keys: Vec<String>) -> bool {
            for key in keys {
                if let Some(_) = self.deps_.borrow_mut().add(key, 1) {
                    return false;
                }
            }
            true
        }

        pub fn execute(&mut self) -> bool {
            println!("Tx[{}] is executing", self.tid_);
            self.state_ = TxState::ACTIVE;

            (self.fn_)();
            true
        }

        pub fn commit(&mut self) -> bool {
            println!("Tx[{}] is commiting", self.tid_);
            self.state_ = TxState::COMMITTED;
            true
        }

        pub fn abort(&mut self, reason: AbortReason) -> bool {
            println!("Tx[{}] is aborting.", self.tid_);
            self.state_ = TxState::ABORTED;
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
