use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc},
};

use txn::{self, AbortReason, Tid,  TxState, TxnInfo, Transaction};


#[cfg(any(feature = "pmem", feature="disk"))]
use {plog, pnvm_sys};
use tcore::{self, ObjectId, TTag, TRef, BoxRef, Operation, FieldArray};

#[cfg(feature = "profile")]
use flame;



pub struct TransactionOCC
{

    tid_:   Tid,
    state_: TxState,
    deps_:  HashMap<(ObjectId, Operation), TTag>,
    locks_ : Vec<*const TTag>,
    txn_info_ : Arc<TxnInfo>,
    should_abort_: bool,

}

impl Transaction for TransactionOCC
{

     #[cfg_attr(feature = "profile", flame)]
     fn try_commit(&mut self) -> bool {
        if self.should_abort_ {
            return self.abort(AbortReason::IndexErr);
        }

        //Stage 1: lock
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

     fn read<'b, T:'static + Clone>(&'b mut self, tref: Box<dyn TRef>) -> &'b T 
    {
        
        //Get the tx id
        let id = *tref.get_id();

        //Get the current object's version 
        let vers = tref.get_version();

        //Insert a tag
        let tag = self.retrieve_tag(&id, tref, Operation::RWrite);
        tag.add_version(vers);

        //Return data
        tag.get_data()
    }


    //#[cfg_attr(feature = "profile", flame)]
     fn write<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T) 
    {
        //Get the object id
        let id = *tref.get_id();

        //Create tag and store the temporary value
        let tag = self.retrieve_tag(&id,tref, Operation::RWrite);
        tag.write::<T>(val);
    }

     fn write_field<T:'static + Clone>(&mut self, tref: Box<dyn TRef>, val: T, fields: FieldArray) 
     {
         let o_id = *tref.get_id(); 
         let tag = self.retrieve_tag(&o_id, tref, Operation::RWrite);
         tag.write::<T>(val);
         tag.set_fields(fields);
     }

     fn id(&self) -> Tid {
        self.tid_
    }

     fn txn_info(&self) -> &Arc<TxnInfo>  {
        &self.txn_info_
    }

     fn should_abort(&mut self) {
        self.should_abort_ = true;
    }

    //#[cfg_attr(feature = "profile", flame)]
    #[inline(always)]
     fn retrieve_tag(&mut self,
                        id: &ObjectId, 
                        tobj_ref: Box<dyn TRef>,
                        ops: Operation
                        ) 
        -> &mut TTag
        {
            self.deps_.entry((*id, ops)).or_insert(TTag::new(*id, tobj_ref))
        }
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
            should_abort_ : false,
        }
    }


    #[cfg_attr(feature = "profile", flame)]
    pub fn abort(&mut self, reason: AbortReason) -> bool {
        warn!("Tx[{:?}] is aborting - {}", self.tid_, reason.as_ref());
        //#[cfg(benchmark)]
        tcore::BenchmarkCounter::abort();
        self.state_ = TxState::ABORTED;
        self.clean_up();
        false
    }

    #[cfg_attr(feature = "profile", flame)]
    pub fn lock(&mut self) -> bool {
        warn!("Tx[{:?}] is LOCKING", self.tid_);
        let me  = self.id();
        for tag in self.deps_.values_mut() {
            if !tag.has_write() {
                continue;
            }
            if !tag.lock(me) {
                warn!("{:?} LOCKED FAILED -----", me);
                return false;
            } 
            debug!("{:#?} locked!", tag);
        }

        warn!("Tx[{:?}] LOCK OK", self.tid_);
        true
    }

    #[cfg_attr(feature = "profile", flame)]
    fn check(&mut self) -> bool {
        warn!("Tx[{:?}] is checking", self.tid_);
        for tag in self.deps_.values() {
            //Only read ops need to be checked
            if !tag.has_read() {
                continue;
            }

            //Check if the versions match
            if !tag.check(tag.vers_, self.id().into())  {
                warn!("{:?} CHECKED FAILED ---- EXPECT: {}, BUT: {}",
                      self.tid_, tag.get_version(), tag.vers_);
                return false;
            }
        }
        warn!("Tx[{:?}] CHECKED OK", self.tid_);
        true
    }


    #[cfg_attr(feature = "profile", flame)]
    fn commit(&mut self) -> bool {
        //#[cfg(benchmark)]
        warn!("Tx[{:?}] is commiting", self.tid_);
        tcore::BenchmarkCounter::success();
        self.state_ = TxState::COMMITTED;


        //Persist the write set logs
        #[cfg(any(feature = "pmem", feature="disk"))]
        self.do_log();

        //Install write sets into the underlying data
        self.install_data();


        //Persist the data
        #[cfg(any(feature = "pmem", feature="disk"))]
        self.persist_data();

        //Persist commit the transaction
        #[cfg(any(feature = "pmem", feature="disk"))]
        self.persist_commit();

        //Clean up local data structures.
        //txn::mark_commit(self.id());
        self.clean_up();
        true
    }

    #[cfg(any(feature = "pmem", feature="disk"))]
    #[cfg_attr(feature = "profile", flame)]
    fn persist_commit(&self) {
        #[cfg(feature = "pmem")]
        pnvm_sys::drain();

        plog::persist_txn(self.id().into());
    }

    #[cfg(any(feature = "pmem", feature="disk"))]
    #[cfg_attr(feature = "profile", flame)]
    fn do_log(&self) {
        let mut logs = vec![];
        let id = self.id();
        for tag in self.deps_.values() {
            if tag.has_write() {
                logs.push(tag.make_log(id));
            }
        }

        plog::persist_log(logs);
    }

    #[cfg(any(feature = "pmem", feature="disk"))]
    #[cfg_attr(feature = "profile", flame)]
    fn persist_data(&self) {
        for tag in self.deps_.values() {
            tag.persist_data(self.id());
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    fn install_data(&mut self) {
        let id = self.id();
        for tag in self.deps_.values_mut() {
            tag.commit_data(id);
        }
    }

    #[cfg_attr(feature = "profile", flame)]
    fn clean_up(&mut self) {
        self.should_abort_ = false;
        for (_, tag) in self.deps_.drain() {
            if tag.has_write() && tag.is_lock() {
                tag.tobj_ref_.unlock();
            }
        }

        debug!("All cleaned up");
    }


}
