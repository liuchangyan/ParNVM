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
pub mod occ;
pub mod parnvm;

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::tbox::TBox;
    use super::txn::{Transaction, Tid};
    use super::occ::occ_txn::{TransactionOCC};
    use super::txn;
    use super::tcore::{TObject};

    #[test]
    fn test_single_read() {
        let _ = env_logger::init();
        super::tcore::init();
        let tb : TObject<u32> = TBox::new(1);
        {
            let tx = &mut TransactionOCC::new(Tid::new(1), true);
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
            let tx = &mut TransactionOCC::new(Tid::new(1), true);
            tx.write(&tb, 2);
            assert_eq!(tx.try_commit(), true);
            assert_eq!(TransactionOCC::notrans_read(&tb), 2);
        }
    }

    #[test]
    fn test_concurrent_read(){
        super::tcore::init();
        let tb1 : TObject<u32> = TBox::new(1);
        let tb2 : TObject<u32> = TBox::new(2);

        {
            let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
            let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

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
            
            let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
            let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

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
            
            let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
            let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

            tx1.write(&tb1, 10);
            tx2.write(&tb1, 9999);
            
            assert_eq!(tx2.try_commit(), true);
            assert_eq!(TransactionOCC::notrans_read(&tb1), 9999);
            assert_eq!(tx1.try_commit(), true);
            assert_eq!(TransactionOCC::notrans_read(&tb1), 10);
        }
        
    }

    #[test]
    fn test_read_own_write() {
        super::tcore::init();
        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
            assert_eq!(tx1.read(&tb1), 1); 
            tx1.write(&tb1, 10);
            assert_eq!(tx1.read(&tb1), 10); 
            assert_eq!(TransactionOCC::notrans_read(&tb1), 1);

            assert_eq!(tx1.try_commit(), true);
            assert_eq!(TransactionOCC::notrans_read(&tb1), 10);
        }
    }

    #[test]
    fn test_conflict_write_aborts() {
        
        super::tcore::init();
        let tb : TObject<u32> = TBox::new(1); 
        {
            let tx = &mut TransactionOCC::new(Tid::new(1), true);
            tx.write(&tb, 2);
            assert_eq!(tx.read(&tb), 2); 

            TransactionOCC::notrans_lock(&tb, Tid::new(99));

            assert_eq!(tx.try_commit(), false);
            assert_eq!(TransactionOCC::notrans_read(&tb), 1);
        }
        
    }

    #[test]
    fn test_read_string() {
    
        super::tcore::init();
        let tb : TObject<String> = TBox::new(String::from("hillo"));

        {

            let tx = &mut TransactionOCC::new(Tid::new(1), true);
            assert_eq!(tx.read(&tb), String::from("hillo"));

            tx.write(&tb, String::from("world"));
            assert_eq!(tx.read(&tb), String::from("world"));

            assert_eq!(TransactionOCC::notrans_read(&tb), String::from("hillo"));
            assert_eq!(tx.try_commit(), true);
            assert_eq!(TransactionOCC::notrans_read(&tb), String::from("world"));
        }

    }
    
    use super::parnvm::piece::{Pid, Piece};
    use std::{
        rc::Rc,
        cell::RefCell,
    };

   // #[test]
   // fn test_piece_run(){
   //     let x = Rc::new(RefCell::new(1));
   //     let mut piece = Piece::new(Pid::new(1), Tid::new(1), Box::new(|| {
   //         let mut x = x.borrow_mut();
   //         *x += 1;
   //         *x
   //     }));
   //     
   //     assert_eq!(*(x.borrow()), 1);
   //     piece.run();
   //     assert_eq!(*(x.borrow()), 2);
   // }

    
    use super::parnvm::{dep::*, nvm_txn::*, piece::*};
    use std::{
        sync::{RwLock, Arc},
    };
    #[test]
    fn test_single_piece_run() {
        let x = Arc::new(RwLock::new(1));
        let y = Arc::new(RwLock::new(2));
    

        let x_1 = x.clone();
        let read_x =move  || {
            let v = x_1.read().unwrap();
            println!("Read : {}", *v);
            *v
        };
        
        let y_1 = y.clone();
        let read_y =move  || {
            let v = y_1.read().unwrap();
            println!("Read : {}", *v);
            *v
        };
        
        let y_2 = y.clone();
        let write_y =move  || {
            let mut v = y_2.write().unwrap();
            *v = 999;
            1
        };
    
        let x_2 = x.clone();
        let write_x =move  || {
            let mut v = x_2.write().unwrap();
            *v = 888;
            1
        };
    
        //Prepare TXN1
        let tid1 = Tid::new(1);
        let pid1 = Pid::new(1);
        let pid2 = Pid::new(2);

        let pieces_1 = vec![
            Piece::new(pid1.clone(), tid1.clone(), Box::new(read_x)),
            Piece::new(pid2.clone(), tid1.clone(), Box::new(write_y))
        ];

        let mut dep = Dep::new();
        dep.add(pid1.clone(), ConflictInfo::new(String::from("TXN_WX"), pid2.clone(), ConflictType::ReadWrite));

        let mut tx = TransactionPar::new(pieces_1, dep, tid1, String::from("TXN_RX_WY"));
        //Tx done
        
        //Prepare Registry
        let names  = vec![String::from("TXN_WX"), String::from("TXN_RX_WY")];
        TxnRegistry::new_thread_registry_names(names);

    
        {
            tx.register_txn();
            tx.execute_txn();
        }

        
        assert_eq!(*y.read().unwrap(), 999);
        assert_eq!(TxnRegistry::thread_count(), 0 as usize);
        assert_eq!(*tx.status().clone(), txn::TxState::COMMITTED);
    }
}
