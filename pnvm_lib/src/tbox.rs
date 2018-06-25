use tobj::{TValue, TVersion, TTag, TObject};
use txn::{Tid,Transaction};
use util;


pub type TObject<T> = Rc<RefCell<_TObject<T>>;

pub struct ObjectId(u32);

pub struct TBox<T> {
    tvalue_: TValue<T>,
    vers_:   TVersion,
    id_ : ObjectId,
}


impl<T> _TObject<T> for TBox<T> {
    fn new(val : T) -> Self {
        let id = util::random_id();
        TBox {
            tvalue_ : val,
            id_ : id,
            vers_: Tversion {
                last_writer_ : None,
                lock_ : Arc::new(Mutex::new(false))
            }
        }
    }

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
    fn lock(&mut self, tid : Tid) -> bool {
         
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

     /* No Trans Access method */
     fn raw_read(&self) -> T {
        self.tvalue_.data_
     }

     fn raw_write(&mut self, val : T){
        self.tvalue_.data_ = val
     }

}
