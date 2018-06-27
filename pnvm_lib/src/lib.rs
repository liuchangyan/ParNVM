pub mod sched;
pub mod tobj;
pub mod txn;
pub mod deps;
pub mod tbox;


#[cfg(test)]
mod tests {
    use super::tbox::TBox;
    use super::txn::{Transaction, Tid};
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
            let val = tx.write(&tb, 2);
            assert_eq!(tx.try_commit(), true);
            assert_eq!(tx.notrans_read(&tb), 2);
        }
    }
}
