use txn::{Tid};
//use std::rc::Rc;
//use std::cell::RefCell;
use std::sync::{RwLock, Mutex, Arc};
use tobj::{TValue, TVersion,   ObjectId, _TObject};
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

    fn check(&self, tid : &Option<Tid>) -> bool {
        self.vers_.check_version(tid)
    }
    
    fn install(&mut self, val :T, tid: Tid) {
        self.tvalue_.store(val);
        self.vers_.set_version(tid);
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

    fn get_version(&self) -> Option<Tid> {
       self.vers_.get_version()
    }

     /* No Trans Access method */
     fn raw_read(&self) -> T {
        self.tvalue_.load()
     }

     fn raw_write(&mut self, val : T){
        self.tvalue_.store(val);
     }
}

impl<T> TBox<T> 
where T: Clone
{

    pub fn new(val : T) -> Arc<RwLock<TBox<T>>> {
        let id;
        unsafe {
            id = tobj::next_id();
        }
        Arc::new(RwLock::new(TBox{
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



}
