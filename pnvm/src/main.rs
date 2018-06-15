extern crate pnvm_lib;

use pnvm_lib::txn::*;
use pnvm_lib::sched::*;

fn main() {

    let sc = Scheduler::new(5, &workload);
    sc.run();
}

fn say_hi() {
    println!("Hello there!");
}

fn workload() {
    for i in 1..5 {
        let mut tx = Transaction::new(say_hi, i);
        tx.execute();
        tx.commit();
    }
}
