//pub mod sched;
pub mod tobj;
pub mod txn;
//pub mod deps;
pub mod tbox;


#[cfg(test)]
mod tests {
    use super::tbox::TBox;
    use super::txn::{Transaction, Tid};
    use super::txn;
    use super::tobj::{_TObject, TObject};

    #[test]
    fn test_single_read() {
        let tb : TObject<u32> = TBox::new(1);

        {
            let tx = &mut Transaction::new(Tid::new(1));
            let val = tx.read(&tb);
            tx.try_commit();
        }
    }

    #[test]
    fn test_single_write() {
        let tb : TObject<u32> = TBox::new(1); 
        {
            let tx = &mut Transaction::new(Tid::new(1));
            tx.write(&tb, 2);
            assert_eq!(tx.try_commit(), true);
            assert_eq!(Transaction::notrans_read(&tb), 2);
        }
    }

    #[test]
    fn test_concurrent_read(){
        let tb1 : TObject<u32> = TBox::new(1);
        let tb2 : TObject<u32> = TBox::new(2);

        {
            let tx1 = &mut Transaction::new(Tid::new(1));
            let tx2 = &mut Transaction::new(Tid::new(2));

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
        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1));
            let tx2 = &mut Transaction::new(Tid::new(2));

            assert_eq!(tx1.read(&tb1), 1);
            tx2.write(&tb1, 2);
            
            assert_eq!(tx2.try_commit(), true);
            assert_eq!(tx1.try_commit(), false);
            
        }
    }
    
    #[test]
    fn test_writes_in_order() {

        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1));
            let tx2 = &mut Transaction::new(Tid::new(2));

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
        let tb1 : TObject<u32> = TBox::new(1);

        {
            
            let tx1 = &mut Transaction::new(Tid::new(1));
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
        
        let tb : TObject<u32> = TBox::new(1); 
        {
            let tx = &mut Transaction::new(Tid::new(1));
            tx.write(&tb, 2);
            assert_eq!(tx.read(&tb), 2); 

            Transaction::notrans_lock(&tb, Tid::new(99));

            assert_eq!(tx.try_commit(), false);
            assert_eq!(Transaction::notrans_read(&tb), 1);
        }
        
    }
}
