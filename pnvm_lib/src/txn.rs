use std::{
    collections::HashMap,
    sync::Arc,
    rc::Rc,
};
use tcore::{ObjectId, TObject, TTag};

#[derive(PartialEq, Copy, Clone, Debug)]
pub struct Tid(u32);

impl Tid {
    pub fn new(id: u32) -> Tid {
        Tid(id)
    }
}

pub struct Transaction<T>
where
    T: Clone,
{
    tid_: Tid,
    state_: TxState,
    deps_: HashMap<ObjectId, TTag<T>>,
}

impl<T> Transaction<T>
where
    T: Clone,
{
    pub fn new(tid_: Tid) -> Transaction<T> {
        Transaction {
            tid_,
            state_: TxState::EMBRYO,
            deps_: HashMap::new(),
        }
    }

    pub fn commit_id(&self) -> Tid {
        self.tid_
    }

    pub fn try_commit(&mut self) -> bool {
        println!("Tx[{:?}] is commiting", self.tid_);
        self.state_ = TxState::COMMITTED;

        //Stage 1: lock [TODO: Bounded lock or try_lock syntax]
        if !self.lock() {
            return self.abort(AbortReason::FailedLocking);
        }

        //Stage 2: Check
        if !self.check() {
            return self.abort(AbortReason::FailedLocking);
        }

        //Stage 3: Commit
        self.commit();
        true
    }

    pub fn abort(&mut self, _: AbortReason) -> bool {
        println!("Tx[{:?}] is aborting.", self.tid_);
        self.state_ = TxState::ABORTED;
        self.clean_up();
        false
    }

    pub fn read(&mut self, tobj: &TObject<T>) -> T {
        let tobj = Arc::clone(tobj);

        let id = tobj.get_id();
        let tag = self.retrieve_tag(id, Arc::clone(&tobj));
        tag.add_version(tobj.get_version());
        if tag.has_write() {
            tag.write_value()
        } else {
            tobj.get_data()
        }
    }

    pub fn write(&mut self, tobj: &TObject<T>, val: T) {
        let tobj = Arc::clone(tobj);

        let tag = self.retrieve_tag(tobj.get_id(), Arc::clone(&tobj));
        if !tag.has_read() {
            //persist log 
            //let log = PLog(tobj);
             
        }
        tag.write(val);
    }

    pub fn lock(&mut self) -> bool {
        let mut locks: Vec<&TTag<T>> = Vec::new();

        for tag in self.deps_.values() {
            if !tag.has_write() {
                continue;
            }
            let _tobj = Arc::clone(&tag.tobj_ref_);
            if !_tobj.lock(self.commit_id()) {
                while let Some(_tag) = locks.pop() {
                    _tag.tobj_ref_.unlock();
                }
                println!("{:#?} failed to locked!", tag);
                return false;
            } else {
                locks.push(tag);
            }
            println!("{:#?} locked!", tag);
        }

        true
    }

    fn check(&mut self) -> bool {
        for tag in self.deps_.values() {
            if !tag.has_read() {
                continue;
            }

            if !tag.tobj_ref_.check(&tag.vers_) {
                return false;
            }
        }
        true
    }

    fn commit(&mut self) -> bool {
        let id = self.commit_id();

        //Install write sets into the underlying data
        for tag in self.deps_.values_mut() {
                tag.commit(id); 
                //FIXME: delegating to tag for commiting? 
        }
        
        //Persist the data
        for tag in self.deps_.values() {
            tag.persist(id);  
        }

        //Spinning on checking on depedency
        //self.wait_for_deps();

        //Persist commit the transaction 
        //self.persist_commit();
        
        //Clean up local data structures.
        self.clean_up();
        true
    }

    fn clean_up(&mut self) {
        for tag in self.deps_.values() {
            if tag.has_write() {
                tag.tobj_ref_.unlock();
            }
        }
    }

    pub fn retrieve_tag(&mut self, id: ObjectId, tobj_ref: TObject<T>) -> &mut TTag<T> {
        self.deps_.entry(id).or_insert(TTag::new(id, tobj_ref))
    }

    /*Non Transaction Functions*/
    pub fn notrans_read(tobj: &TObject<T>) -> T {
        //let tobj = Arc::clone(tobj);
        tobj.raw_read()
    }

    pub fn notrans_lock(tobj: &TObject<T>, tid: Tid) -> bool {
        let tobj = Arc::clone(tobj);
        tobj.lock(tid)
    }
}

pub enum TxState {
    COMMITTED,
    EMBRYO,
    ACTIVE,
    ABORTED,
}

pub enum AbortReason {
    Error,
    User,
    FailedLocking,
}
