//pub mod sched;
#![allow(dead_code)]
#![feature(allocator_api)]
#![feature(libc)]
#![feature(ptr_internals)]
extern crate pnvm_sys;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

extern crate libc;

pub mod tcore;
pub mod plog;
pub mod txn;
pub mod tbox;
pub mod conf;
pub mod piece;

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::tbox::TBox;
    use super::txn::{Transaction, Tid};
    use super::txn;
    use super::tcore::{TObject};

    #[test]
    fn test_single_read() {
        let _ = env_logger::init();
        super::tcore::init();
        let tb : TObject<u32> = TBox::new(1);
        {
            let tx = &mut Transaction::new(Tid::new(1), true);
            let val = tx.read(&tb);
            tx.try_commit();
        }
    }

    #[test]
    fn test_single_write() {
        let _ = env_logger::init();
        super::tcore::init();
        let tb : TObject<u32> = TBox::new(1); 
        {
            let tx = &mut Transaction::new(Tid::new(1), true);
            tx.write(&tb, 2);
            assert_eq!(tx.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb), 2);
        }
    }

    #[test]
    fn test_concurrent_read(){
        super::tcore::init();
        let tb1 : TObject<u32> = TBox::new(1);
        let tb2 : TObject<u32> = TBox::new(2);

        {
            let tx1 = &mut Transaction::new(Tid::new(1), true);
            let tx2 = &mut Transaction::new(Tid::new(2), true);

            assert_eq!(tx1.read(&tb1), 1);
            assert_eq!(tx2.read(&tb1), 1);

            assert_eq!(tx1.read(&tb1), 1);
            assert_eq!(tx2.read(&tb2), 2);
            
            assert_eq!(tx1.try_commit(), true);
            assert_eq!(tx2.try_commit(), true);
        }

    }


    #[test]
    fn test_dirty_read_should_abort(){
        super::tcore::init();
        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1), true);
            let tx2 = &mut Transaction::new(Tid::new(2), true);

            assert_eq!(tx1.read(&tb1), 1);
            tx2.write(&tb1, 2);
            
            assert_eq!(tx2.try_commit(), true);
            assert_eq!(tx1.try_commit(), false);
            
        }
    }
    
    #[test]
    fn test_writes_in_order() {
        super::tcore::init();

        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1), true);
            let tx2 = &mut Transaction::new(Tid::new(2), true);

            tx1.write(&tb1, 10);
            tx2.write(&tb1, 9999);
            
            assert_eq!(tx2.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb1), 9999);
            assert_eq!(tx1.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb1), 10);
        }
        
    }

    #[test]
    fn test_read_own_write() {
        super::tcore::init();
        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1), true);
            assert_eq!(tx1.read(&tb1), 1); 
            tx1.write(&tb1, 10);
            assert_eq!(tx1.read(&tb1), 10); 
            assert_eq!(Transaction::notrans_read(&tb1), 1);

            assert_eq!(tx1.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb1), 10);
        }
    }

    #[test]
    fn test_conflict_write_aborts() {
        
        super::tcore::init();
        let tb : TObject<u32> = TBox::new(1); 
        {
            let tx = &mut Transaction::new(Tid::new(1), true);
            tx.write(&tb, 2);
            assert_eq!(tx.read(&tb), 2); 

            Transaction::notrans_lock(&tb, Tid::new(99));

            assert_eq!(tx.try_commit(), false);
            assert_eq!(Transaction::notrans_read(&tb), 1);
        }
        
    }

    #[test]
    fn test_read_string() {
    
        super::tcore::init();
        let tb : TObject<String> = TBox::new(String::from("hillo"));

        {

            let tx = &mut Transaction::new(Tid::new(1), true);
            assert_eq!(tx.read(&tb), String::from("hillo"));

            tx.write(&tb, String::from("world"));
            assert_eq!(tx.read(&tb), String::from("world"));

            assert_eq!(Transaction::notrans_read(&tb), String::from("hillo"));
            assert_eq!(tx.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb), String::from("world"));
        }

    }
    
    use super::piece::{Pid, Piece};
    use std::{
        rc::Rc,
        cell::RefCell,
    };

    #[test]
    fn test_piece_run(){
        let x = Rc::new(RefCell::new(1));
        let mut piece = Piece::new(Pid::new(1), Tid::new(1), || {
            let mut x = x.borrow_mut();
            *x += 1;
            *x
        });
        
        assert_eq!(*(x.borrow()), 1);
        piece.run();
        assert_eq!(*(x.borrow()), 2);
    }
}
