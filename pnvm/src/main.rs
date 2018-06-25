extern crate pnvm_lib;

use pnvm_lib::txn::*;
use std::sync::Arc;
use std::cell::RefCell;

fn main() {
    const NUM_THREADS :u32 = 5;
    let sc = sched::Scheduler::new(NUM_THREADS, &workload);
    sc.run();
}


/******** Utility Functions ********/

fn test_single() {

        let tb = TBox<u32>(1);

        {
            let mut tx = Transaction::new(1);

            tx.write(tb, 2);

            tx.try_commit();
            
            assert!(tb.raw_read() == 2);
        }


       // let dep = Arc::clone(&dep);
       // let mut tx = txn::Transaction::new(say_hi, i, dep);
       // let dep = vec![format!("a{}", i%3),format!("{}", i%3)];
       // tx.add_deps(dep);
       // tx.execute();
       // tx.commit();
    }
}
