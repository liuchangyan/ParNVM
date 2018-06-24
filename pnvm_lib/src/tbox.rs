use tobj::{TValue, TVersion, TTag, TObject};
use txn::Transaction;



pub struct TBox<T> {
    tvalue_: TValue<T>,
    vers_:   TVersion,
}


impl<T> TObject<T> for TBox<T> {
    /*Access callbacks */
    fn read(&self) -> &T {
        let tag = ttag::tag(&self, 0);
        if tag.has_write() {
            tag.write_value()
        } else {
            tag.read_value(self.tvalue_)
        }
    }

    fn write(&self, val: T) {
        let tag = ttag::tag(&self, 0);
        tag.write_value();
    }

    /*Commit callbacks*/
    fn lock(&mut self, _:TTag<T>, tx : &Transaction) -> bool {
        tx.lock(&mut self.vers_)
    }

    fn check(&self, _ : TTag<T>, tx: &Transaction) -> bool {
        self.vers_.check_version(tx.commit_id())
    }
    
     fn install(&mut self, tag : TTag<T>, tx: &Transaction) {
        self.tvalue_.store(tag.write_value());
        self.vers_.set_version(tx.commit_id());
    }

     fn unlock(&mut self) {
        self.vers_.unlock(); 
    }

}
