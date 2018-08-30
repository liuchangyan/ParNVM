
use alloc::raw_vec::RawVec;
use alloc::alloc::Layout;

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc,RwLock},
    collections::{
        HashMap,
        hash_map::RandomState,
    },
    cell::UnsafeCell,
    ptr,
    hash::{self,Hash, BuildHasher, Hasher},

};

use pnvm_lib::tcore::{TVersion, ObjectId, OidFac, TRef};
use pnvm_lib::txn::{Tid,TxnInfo};
use pnvm_lib::occ::occ_txn::TransactionOCC;
use super::entry::TableRef;



pub trait Key<T> {
    fn primary_key(&self) -> T;
}



pub struct Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone ,
      Index: Eq+Hash + Clone,
{
    buckets : Vec<Bucket<Entry, Index>>,
    bucket_num: usize,
    
    //len :usize,
    hash_builder: RandomState,
    
     //id_ : ObjectId,
    //vers_ : TVersion,
}


impl<Entry, Index> Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone ,
      Index: Eq+Hash+Clone,
{
    pub fn new() -> Table<Entry, Index> {
       Default::default() 
    }

    pub fn new_with_buckets(num: usize) -> Table<Entry, Index> {
        let mut buckets = Vec::with_capacity(num);
        for _ in 0..num {
            buckets.push(Bucket::new());
        }

        Table {
            buckets,
            bucket_num : num,
            hash_builder: Default::default(),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Entry)
    where Arc<Row<Entry, Index>> : TableRef<Entry, Index>
    {
        let bucket_idx = self.make_hash(&entry.primary_key()) % self.bucket_num;

        //Make into row and then make into a RowRef
        let row = Arc::new(Row::new_from_txn(entry, tx.txn_info().clone()));
        let table_ref = row.into_table_ref(Some(bucket_idx), tx.txn_info(), Some(self));
        let _  = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone());
    }

    //pub fn push(&self, entry: Entry) {
    //    let bucket_idx = self.make_hash(&entry.primary_key()) % self.bucket_num;
    //    self.buckets[bucket_idx].push(entry)
    //}

    pub fn retrieve(&self, index: &Index) -> Option<&Row<Entry, Index>> {
        let bucket_idx = self.make_hash(&index) % self.bucket_num;
        self.buckets[bucket_idx].retrieve(index)
    }

    fn make_hash(&self, idx : &Index) -> usize {
        let mut s = self.hash_builder.build_hasher();
        idx.hash(&mut s);
        s.finish() as usize
    }


}

impl<Entry, Index> Default for Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash  + Clone,
{
    fn default() -> Self {
        let mut buckets = Vec::with_capacity(16);

        for _ in 0..16{
            buckets.push(Bucket::new());
        }
        
        Table {
            buckets,
            bucket_num: 16,
            hash_builder : Default::default(), }
    }
}

/* FIXME: can we avoid the copy */
pub struct Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash + Clone,
{
    rows: RwLock<RawVec<Row<Entry, Index>>>,
    index: RwLock<HashMap<Index, usize>>,
    len : AtomicUsize,

    id_ : ObjectId,
    vers_ : TVersion,
}

impl<Entry, Index> Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash+ Clone,
{
    pub fn new() -> Bucket<Entry, Index> {
        Bucket {
            rows: RwLock::new(RawVec::new()),
            len: AtomicUsize::new(0),
            index: RwLock::new(HashMap::new()),

            id_ : OidFac::get_obj_next(),
            vers_ : TVersion::default(),
        }
    }

    pub fn push(&self, row : Row<Entry, Index>) {
        let prev_len = self.len.fetch_add(1, Ordering::Acquire);
        if prev_len == self.cap() {
            let mut rw = self.rows.write().unwrap();
            rw.double(); /* This may OOM */
        } else if prev_len > self.cap() {
            //FIXME: busy wait here maybe
            panic!("hmmm, someone else should have been doubling");
        }
        let idx_elem = row.get_data().primary_key();
        unsafe {
            ptr::write(self.ptr().offset(prev_len as isize), row);
        }
        
        let mut idx_map = self.index.write().unwrap();
        idx_map.insert(idx_elem, prev_len);
    }

    pub fn retrieve(&self, index_elem: &Index) -> Option<&Row<Entry, Index>> {
        //Check out of bound
        let index = self.index.read().unwrap();
        match index.get(index_elem) {
            None => None,
            Some(idx) => {
                let rows = self.rows.read().unwrap();
                unsafe {
                    rows.ptr().offset(*idx as isize).as_ref()
                }
            }
        }
    }

    fn cap(&self) -> usize {
        let rows = self.rows.read().unwrap();
        rows.cap()
    }

    fn ptr(&self) -> *mut Row<Entry, Index> {
        let rows = self.rows.read().unwrap();
        rows.ptr()
    }



}


pub struct Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash + Clone
{
    data_: UnsafeCell<Entry>,
    vers_: TVersion,
    id_ : ObjectId,
    index_ : Index,
}

impl<Entry, Index> Clone for Row<Entry, Index>
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash  + Clone
{
    fn clone(&self) -> Self {
        Row {
            data_ : unsafe {UnsafeCell::new(self.data_.get().as_ref().unwrap().clone())},
            vers_ : self.vers_.clone(),
            id_: self.id_,
            index_ : self.index_.clone()
        }
    }
}

unsafe impl<Entry: Clone, Index> Sync for Row<Entry, Index>
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash  + Clone
{}
unsafe impl<Entry: Clone, Index> Send for Row<Entry, Index>
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash  + Clone
{}

impl<Entry, Index>  Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone,
      Index: Eq+Hash  + Clone
{
    pub fn new(entry: Entry) -> Row<Entry, Index>{
        let key = entry.primary_key();
        Row{
            data_: UnsafeCell::new(entry),
            vers_: TVersion::default(), /* FIXME: this can carry txn info */
            id_ : OidFac::get_obj_next(),
            index_ : key, 
        }
    }

    pub fn new_from_txn(entry : Entry, txn_info: Arc<TxnInfo>) -> Row<Entry, Index> {
        let key = entry.primary_key();
        Row {
            data_ : UnsafeCell::new(entry),
            vers_ : TVersion::new_with_info(txn_info),
            id_ : OidFac::get_obj_next(),
            index_ : key,
        }
    }


    #[inline(always)]
    pub fn get_data(&self) -> &Entry {
        unsafe { self.data_.get().as_ref().unwrap() }
    }

    #[inline(always)]
    pub fn get_ptr(&self) -> *mut u8 {
        unsafe {self.data_.get() as *mut u8}
    }


    #[inline(always)]
    pub fn lock(&self, tid: Tid) -> bool {
        self.vers_.lock(tid)
    }

    #[inline(always)]
    pub fn check(&self, cur_ver: u32) -> bool {
        self.vers_.check_version(cur_ver)
    }

    //FIXME: how to not Clone
    #[inline]
    pub fn install(&self, val: &Entry, tid: Tid) {
        unsafe {ptr::write(self.data_.get(), val.clone())};
        self.vers_.set_version(tid);
    }

    #[inline(always)]
    pub fn unlock(&self) {
        self.vers_.unlock();
    }

    
    #[inline(always)]
    pub fn get_version(&self) -> u32 {
        self.vers_.get_version()
    }
    
    #[inline(always)]
    pub fn get_id(&self) -> &ObjectId {
        &self.id_    
    }

   // pub fn get_addr(&self) -> Unique<T> {
   //     let tvalue = self.tvalue_.read().unwrap();
   //     tvalue.get_addr()
   // }

    pub fn get_layout(&self) -> Layout {
        Layout::new::<Entry>()
    }

    pub fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.vers_.get_writer_info()
    }

    pub fn set_writer_info(&self, info : Arc<TxnInfo>) {
        self.vers_.set_writer_info(info)
    }

    pub fn read(tx : &mut TransactionOCC, self_arc: Arc<Row<Entry, Index>>) -> &Entry
        where Arc<Row<Entry, Index>> : TableRef<Entry, Index>
    {
        let tref = self_arc.into_table_ref(None, tx.txn_info(), None);
        let id = *tref.get_id();
        let old_vers = tref.get_version();

        let tag = tx.retrieve_tag(&id, tref);
        tag.add_version(old_vers);
        tag.get_data()
    }

    pub fn write(tx: &mut TransactionOCC, self_arc: Arc<Row<Entry, Index>>, val: Entry)
        where Arc<Row<Entry, Index>> : TableRef<Entry, Index>
    {
        let tref = self_arc.into_table_ref(None, tx.txn_info(), None);
        let id = *tref.get_id();
        let mut tag = tx.retrieve_tag(&id, tref);
        tag.write(val);
    }

    /* Transaction Methods */
}






