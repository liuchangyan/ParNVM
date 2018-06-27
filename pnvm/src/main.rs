extern crate pnvm_lib;

use pnvm_lib::txn::*;
use pnvm_lib::tbox::TBox;
use std::cell::RefCell;
use pnvm_lib::tobj::TObject;

fn main() {
   test_single();
}


/******** Utility Functions ********/

fn test_single() {

    let tb : TObject<u32> = TBox::new(1);

    {
        let tx = &mut Transaction::new(Tid::new(1));
        let val = tx.read(&tb);
        tx.try_commit();
    }
}

