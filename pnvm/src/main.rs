extern crate pnvm_lib;

use pnvm_lib::txn::*;
use pnvm_lib::tbox::TBox;
use std::cell::RefCell;
use pnvm_lib::tobj::TObject;

fn main() {
   // const NUM_THREADS :u32 = 5;
   // let sc = sched::Scheduler::new(NUM_THREADS, &workload);
   // sc.run();
   test_single();
}


/******** Utility Functions ********/

fn test_single() {

    let tb = TBox::new(1) as TObject<i32>;

    {
        let tx = &mut Transaction::new(Tid::new(1));

        match tx.read(&tb) {
            Ok(val) => assert!(val == 1),
            Err(err) => panic!("Should not Abort")
        };

        tx.try_commit();
    }


    // let dep = Arc::clone(&dep);
    // let mut tx = txn::Transaction::new(say_hi, i, dep);
    // let dep = vec![format!("a{}", i%3),format!("{}", i%3)];
    // tx.add_deps(dep);
    // tx.execute();
    // tx.commit();
}

