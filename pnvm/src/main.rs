extern crate pnvm_lib;

use pnvm_lib::*;
use std::sync::Arc;
use std::cell::RefCell;

fn main() {
    const NUM_THREADS :u32 = 5;
    let sc = sched::Scheduler::new(NUM_THREADS, &workload);
    sc.run();
}


/******** Utility Functions ********/

fn test_single() {

    


       // let dep = Arc::clone(&dep);
       // let mut tx = txn::Transaction::new(say_hi, i, dep);
       // let dep = vec![format!("a{}", i%3),format!("{}", i%3)];
       // tx.add_deps(dep);
       // tx.execute();
       // tx.commit();
    }
}
