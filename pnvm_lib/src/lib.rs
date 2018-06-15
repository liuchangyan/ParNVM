pub mod txn {
    pub struct Transaction<T>
    where
        T: Fn(),
    {
        tid_:   u64,
        state_: TxState,
        deps_:  Option<Dep>,
        fn_:    T,
    }

    impl<T> Transaction<T>
    where
        T: Fn(),
    {
        pub fn new(fn_: T, tid_: u64) -> Transaction<T> {
            Transaction {
                tid_,
                state_: TxState::EMBRYO,
                deps_: None,
                fn_,
            }
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

        pub fn abort(&self, reason: AbortReason) -> bool {
            println!("Tx[{}] is aborting.", self.tid_);
            true
        }
    }

    pub enum TxState {
        COMMITTED,
        EMBRYO,
        ACTIVE,
        ABORTED,
    }

    pub struct Dep {}

    pub enum AbortReason {
        AbortReasonError,
        AbortReasonUser,
    }
}

pub mod sched {
    use std::thread;
    use std::sync::Arc;
    pub struct Scheduler<T>
    where
        T: Fn() + Send + Sync + 'static
    {
        nthreads_:       u32,
        task_:           &'static T,
    }

    impl<T> Scheduler<T>
    where
        T: Fn() + Send + Sync + 'static
    {
        pub fn new(nthreads_: u32, task_: &'static T) -> Scheduler<T> {
            Scheduler {
                nthreads_,
                task_,
            }
        }

        pub fn run(&self) {
            let task = Arc::new(self.task_);
            let mut handles = vec![];
            for i in 0..self.nthreads_ {
                let task = task.clone();
                handles.push(thread::spawn(move || (task)()));
            }

           
            for handle in handles {
                handle.join().unwrap();
            }

            println!("All done");
        }   
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
