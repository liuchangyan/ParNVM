//pub mod sched;
#![allow(dead_code)]
#![feature(allocator_api)]
#![feature(libc)]
#![feature(test)]
#![feature(ptr_internals)]
#![cfg_attr(feature = "profile", feature(plugin, custom_attribute))]
#![cfg_attr(feature = "profile", plugin(flamer))]
extern crate pnvm_sys;

extern crate core;
#[cfg(feature = "profile")]
extern crate flame;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate crossbeam;
extern crate libc;

extern crate test;

extern crate chashmap;

extern crate evmap;
extern crate parking_lot;

pub mod conf;
pub mod plog;
pub mod tbox;
pub mod tcore;
pub mod txn;

pub mod occ;
pub mod parnvm;

#[cfg(test)]
mod tests {
    extern crate crossbeam;
    extern crate env_logger;

    use super::occ::occ_txn::{TransactionOCC};
    use super::occ::map::TMap;
    use super::tbox::TBox;
    use super::tcore::TObject;
    use super::txn;
    use super::txn::{Tid, Transaction};

    // #[test]
    // fn test_single_read() {
    //     let _ = env_logger::init();
    //     super::tcore::init();
    //     let tb : TObject<u32> = TBox::new(1);
    //     {
    //         let tx = &mut TransactionOCC::new(Tid::new(1), true);
    //         let val = tx.read(&tb);
    //         tx.try_commit();
    //     }
    // }

    // #[test]
    // fn test_single_write() {
    //     let _ = env_logger::init();
    //     super::tcore::init();
    //     let tb : TObject<u32> = TBox::new(1);
    //     {
    //         let tx = &mut TransactionOCC::new(Tid::new(1), true);
    //         tx.write(&tb, 2);
    //         assert_eq!(tx.try_commit(), true);
    //         assert_eq!(TransactionOCC::notrans_read(&tb), 2);
    //     }
    // }

    // #[test]
    // fn test_concurrent_read(){
    //     super::tcore::init();
    //     let tb1 : TObject<u32> = TBox::new(1);
    //     let tb2 : TObject<u32> = TBox::new(2);

    //     {
    //         let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
    //         let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

    //         assert_eq!(tx1.read(&tb1), 1);
    //         assert_eq!(tx2.read(&tb1), 1);

    //         assert_eq!(tx1.read(&tb1), 1);
    //         assert_eq!(tx2.read(&tb2), 2);
    //
    //         assert_eq!(tx1.try_commit(), true);
    //         assert_eq!(tx2.try_commit(), true);
    //     }

    // }

    // #[test]
    // fn test_dirty_read_should_abort(){
    //     super::tcore::init();
    //     let tb1 : TObject<u32> = TBox::new(1);

    //     {
    //
    //         let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
    //         let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

    //         assert_eq!(tx1.read(&tb1), 1);
    //         tx2.write(&tb1, 2);
    //
    //         assert_eq!(tx2.try_commit(), true);
    //         assert_eq!(tx1.try_commit(), false);
    //
    //     }
    // }
    //
    // #[test]
    // fn test_writes_in_order() {
    //     super::tcore::init();

    //     let tb1 : TObject<u32> = TBox::new(1);

    //     {
    //
    //         let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
    //         let tx2 = &mut TransactionOCC::new(Tid::new(2), true);

    //         tx1.write(&tb1, 10);
    //         tx2.write(&tb1, 9999);
    //
    //         assert_eq!(tx2.try_commit(), true);
    //         assert_eq!(TransactionOCC::notrans_read(&tb1), 9999);
    //         assert_eq!(tx1.try_commit(), true);
    //         assert_eq!(TransactionOCC::notrans_read(&tb1), 10);
    //     }
    //
    // }

    // #[test]
    // fn test_read_own_write() {
    //     super::tcore::init();
    //     let tb1 : TObject<u32> = TBox::new(1);

    //     {
    //
    //         let tx1 = &mut TransactionOCC::new(Tid::new(1), true);
    //         assert_eq!(tx1.read(&tb1), 1);
    //         tx1.write(&tb1, 10);
    //         assert_eq!(tx1.read(&tb1), 10);
    //         assert_eq!(TransactionOCC::notrans_read(&tb1), 1);

    //         assert_eq!(tx1.try_commit(), true);
    //         assert_eq!(TransactionOCC::notrans_read(&tb1), 10);
    //     }
    // }

    // #[test]
    // fn test_conflict_write_aborts() {
    //
    //     super::tcore::init();
    //     let tb : TObject<u32> = TBox::new(1);
    //     {
    //         let tx = &mut TransactionOCC::new(Tid::new(1), true);
    //         tx.write(&tb, 2);
    //         assert_eq!(tx.read(&tb), 2);

    //         TransactionOCC::notrans_lock(&tb, Tid::new(99));

    //         assert_eq!(tx.try_commit(), false);
    //         assert_eq!(TransactionOCC::notrans_read(&tb), 1);
    //     }
    //
    // }

    // #[test]
    // fn test_read_string() {
    //
    //     super::tcore::init();
    //     let tb : TObject<String> = TBox::new(String::from("hillo"));

    //     {

    //         let tx = &mut TransactionOCC::new(Tid::new(1), true);
    //         assert_eq!(tx.read(&tb), String::from("hillo"));

    //         tx.write(&tb, String::from("world"));
    //         assert_eq!(tx.read(&tb), String::from("world"));

    //         assert_eq!(TransactionOCC::notrans_read(&tb), String::from("hillo"));
    //         assert_eq!(tx.try_commit(), true);
    //         assert_eq!(TransactionOCC::notrans_read(&tb), String::from("world"));
    //     }

    // }

    // #[test]
    // fn test_read_hashmap() {

    //     super::tcore::init();

    // }
    //
    // use super::parnvm::piece::{Pid, Piece};
    // use std::{
    //     rc::Rc,
    //     cell::RefCell,
    // };

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

    use super::parnvm::{dep::*, nvm_txn::*, piece::*, map::*};
    use std::{
        fs::File,
        sync::{Arc, RwLock, Barrier},
        thread,
    };

    #[test]
    fn test_single_piece_run() {
        
        let data_map = Arc::new(PMap::new());
        let barrier = Arc::new(Barrier::new(1));
        
        let piece = Piece::new(
            Pid::new(1),
            "TXN_1".to_string(),
            Arc::new(Box::new(move|tx : &mut TransactionPar| {
                let val = PValue::new(1 as u32, tx);
                data_map.insert(1,val);

                let g = data_map.get(&1).unwrap();
                println!("Read {}", (*g.read(tx).as_ref().unwrap()));

                let id :u32 = tx.id().into(); 
                {
                    let mut write_g = g.write(tx);
                    let mut i = 0;
                    while i< 1000000 { i+=1;}
                    *write_g.as_mut().unwrap() = id * 100;
                }

                println!("Read {}", (*g.read(tx).as_ref().unwrap()));
                1
            })),
            "insert-read",
            1);
        
        let txn_base = TransactionParBase::new(vec![piece], "TXN_1".to_string()); 

        let tx = TransactionPar::new_from_base(&txn_base, Tid::new(1));
        
        
        crossbeam::scope(|scope| {
            //Prepare TXN1
            let txn_base = txn_base.clone();
            let c = barrier.clone();

            let handler = scope.spawn(move|| {
                c.wait();
                let tx = TransactionPar::new_from_base(&txn_base, Tid::new(2));
                TransactionPar::register(tx);
                TransactionPar::execute();
            });
            
            barrier.wait();
            TransactionPar::register(tx);
            TransactionPar::execute();

            handler.join();
        });

        // Dump the report to disk
    }

    use test::Bencher;

    #[bench]
    fn bench_map_get(b: &mut Bencher) {
        let map : Arc<TMap<u32, u32>> = Arc::new(TMap::new());
        for key in 0..10000 {
            map.insert(key as u32,TBox::new(key as u32));
        }

        let mut tx = TransactionOCC::new(Tid::new(1));
        
        b.iter(|| {
            let g = map.get(&1).unwrap();
            let tag = tx.retrieve_tag(g.get_id(), g.clone());
        });

    }

}
