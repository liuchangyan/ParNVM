use txn::{Tid,Transaction};
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Mutex};
use tobj::{TValue, TVersion, TTag,  ObjectId, _TObject, TObject};
use tobj;


pub struct TBox<T> 
where T : Clone
{
    tvalue_: TValue<T>,
    vers_:   TVersion,
    id_ : ObjectId,
}


impl<T> _TObject<T> for TBox<T>
where T: Clone
    {

    /*Commit callbacks*/
    fn lock(&mut self, tid : Tid) -> bool {
         self.vers_.lock(tid)
    }

    fn check(&self, _ : TTag<T>, tx: &Transaction<T>) -> bool {
        self.vers_.check_version(tx.commit_id())
    }
    
    fn install(&mut self, tag : TTag<T>, tx: &Transaction<T>) {
        self.tvalue_.store(tag.write_value());
        self.vers_.set_version(tx.commit_id());
    }

    fn unlock(&mut self) {
        self.vers_.unlock(); 
    }

    fn get_data(&self) -> T {
        self.raw_read()
    }

    fn get_id(&self) -> ObjectId {
        self.id_
    }

     

}

impl<T> TBox<T> 
where T: Clone
{

    pub fn new(val : T) -> Rc<RefCell<TBox<T>>> {
        let id;
        unsafe {
            id = tobj::next_id();
        }
        Rc::new(RefCell::new(TBox{
            tvalue_ : TValue{
                data_: val
            },
            id_ : id,
            vers_: TVersion {
                last_writer_ : None,
                lock_owner_: Mutex::new(None)
            }
        }))
    }

     /* No Trans Access method */
     fn raw_read(&self) -> T {
        self.tvalue_.load()
     }

     fn raw_write(&mut self, val : T){
        self.tvalue_.store(val);
     }


}
