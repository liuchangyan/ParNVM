use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, RwLock},
};

use txn::{self, AbortReason, Tid,  TxState, TxnInfo};

//use plog;
use tcore::{self, ObjectId, TTag, TRef, BoxRef};
use tbox::TBox;

#[cfg(feature = "profile")]
use flame;

pub struct TransactionOCC
{

    tid_:   Tid,
    state_: TxState,
    deps_:  HashMap<ObjectId, TTag>,
    locks_ : Vec<*const TTag>,
    txn_info_ : Arc<TxnInfo>,

}

impl TransactionOCC
{

    #[cfg_attr(feature = "profile", flame)]
    pub fn try_commit(&mut self) -> bool {
        debug!("Tx[{:?}] is commiting", self.tid_);
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

    #[cfg_attr(feature = "profile", flame)]
    //pub fn read<'b, T:'static>(&'b mut self, tobj: &'b dyn TRef) -> &'b T {
    //    let tag = self.retrieve_tag(tobj.get_id(), tobj.box_clone());
    //    tag.add_version(tobj.get_version());
    //    tag.get_data()
    //}
    pub fn read<'b, T:'static + Clone>(&'b mut self, tref: Box<dyn TRef>) -> &'b T 
    {
        //let tref = tobj.clone().into_box_ref();
        
        let id = *tref.get_id();
        let vers = tref.get_version();
        let tag = self.retrieve_tag(&id, tref);
        tag.add_version(vers);
        tag.get_data()
    }


    #[cfg_attr(feature = "profile", flame)]
    pub fn write<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T) 
    {
        //let tref = tobj.clone().into_box_ref();
        let id = *tref.get_id();
        let mut tag = self.retrieve_tag(&id,tref);
        tag.write::<T>(val);
    }


    /*Non TransactionOCC Functions*/
   // fn notrans_read(tobj: &TObject<T>) -> T {
   //     //let tobj = Arc::clone(tobj);
   //     tobj.raw_read()
   // }

    //fn notrans_lock(tobj: &TObject<T>, tid: Tid) -> bool {
    //    let tobj = Arc::clone(tobj);
    //    tobj.lock(tid)
    //}
}

impl TransactionOCC
{
    pub fn new(tid_: Tid) -> TransactionOCC {
        //txn::mark_start(tid_);
        TransactionOCC {
            tid_,
            state_: TxState::EMBRYO,
            deps_: HashMap::with_capacity(32),
            locks_: Vec::with_capacity(32),
            txn_info_: Arc::new(TxnInfo::new(tid_)),
        }
    }

    pub fn commit_id(&self) -> Tid {
        self.tid_
    }

    pub fn txn_info(&self) -> &Arc<TxnInfo>  {
        &self.txn_info_
    }

    pub fn abort(&mut self, _: AbortReason) -> bool {
        debug!("Tx[{:?}] is aborting.", self.tid_);
        //#[cfg(benchmark)]
        tcore::BenchmarkCounter::abort();
        self.state_ = TxState::ABORTED;
        self.clean_up();
        false
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn lock(&mut self) -> bool {
        let me :u32 = self.commit_id().into();
        for tag in self.deps_.values() {
            if !tag.has_write() {
                continue;
            }
            if !tag.lock(self.commit_id()) {
                while let Some(_tag) = self.locks_.pop() {
                    unsafe{ _tag.as_ref().unwrap().unlock()};
                }
                debug!("{:#?} failed to locked!", tag);
                return false;
            } else {
                self.locks_.push(tag as *const TTag);
            }
            debug!("{:#?} locked!", tag);
        }

        true
    }

    #[cfg_attr(feature = "profile", flame)]
    fn check(&mut self) -> bool {
        for tag in self.deps_.values() {
            if !tag.has_read() {
                continue;
            }
            if !tag.check(tag.vers_)  {
                return false;
            }
        }
        true
    }


    #[cfg_attr(feature = "profile", flame)]
    fn commit(&mut self) -> bool {
        //#[cfg(benchmark)]
        tcore::BenchmarkCounter::success();

        //Persist the write set logs

        #[cfg(feature = "pmem")]
        self.persist_log();


        //Install write sets into the underlying data
        self.install_data();


        //Persist the data
        #[cfg(feature = "pmem")]
        self.persist_data();



        //Persist commit the transaction
        #[cfg(feature = "pmem")]
        self.persist_commit();

        //Clean up local data structures.
        //txn::mark_commit(self.commit_id());
        self.clean_up();
        true
    }

   // #[cfg(feature = "pmem")]
   // #[cfg_attr(feature = "profile", flame)]
   // fn persist_commit(&self) {
   //     //FIXME:: Can it be async?
   //     plog::persist_txn(self.commit_id().into());
   // }

   // #[cfg(feature = "pmem")]
   // #[cfg_attr(feature = "profile", flame)]
   // fn persist_log(&self) {
   //     let mut logs = vec![];
   //     let id = self.commit_id();
   //     for tag in self.deps_.values() {
   //         if tag.has_write() {
   //             logs.push(tag.make_log(id));
   //         }
   //     }

   //     plog::persist_log(logs);
   // }

   // #[cfg(feature = "pmem")]
   // #[cfg_attr(feature = "profile", flame)]
   // fn persist_data(&self) {
   //     for tag in self.deps_.values() {
   //         tag.persist_data(self.commit_id());
   //     }
   // }

    #[cfg_attr(feature = "profile", flame)]
    fn install_data(&mut self) {
        let id = self.commit_id();
        for tag in self.deps_.values_mut() {
            tag.commit_data(id);
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    fn clean_up(&mut self) {
        for tag in self.deps_.values() {
            if tag.has_write() {
                tag.tobj_ref_.unlock();
            }
        }
    }


    #[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
    pub fn retrieve_tag(&mut self,
                        id: &ObjectId, 
                        tobj_ref: Box<dyn TRef>) 
        -> &mut TTag
        {
            self.deps_.entry(*id).or_insert(TTag::new(*id, tobj_ref))
        }
}
