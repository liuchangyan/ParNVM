//Not used

use txn::Transaction;
use std::cell::RefCell;
use std::rc::Rc;

thread_local!{
    //This init should just be dummy
    pub static TXN : RefCell<Transaction> = Rc::new(RefCell::new(Transaction::new(0))); 
}

pub fn set_thd_txn(txn : Rc<RefCell<Transaction>>) {
    TXN.with(|txn_| txn_ = Rc::clone(&txn));
}

pub fn get_thd_txn() -> Rc<RefCell<Transaction>> {
    TXN.with(|txn_| Rc::clone(&txn_))
}

