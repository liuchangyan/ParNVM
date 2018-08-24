
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

use tcore::{TVersion};
use txn::{Tid,TxnInfo};


pub trait Key<T> {
    fn primary_key(&self) -> T;
}


pub struct Table<Entry, Index, S = RandomState> 
where Entry: Key<Index> + Clone,
      Index: Eq+Hash 
{
    buckets : Vec<Bucket<Entry, Index>>,
    bucket_num: usize,
    hash_builder: S
}


impl<Entry, Index, S> Table<Entry, Index, S> 
where Entry: Key<Index> + Clone,
      Index: Eq+Hash,
      S: BuildHasher
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

    pub fn push(&self, entry: Entry) {
        let bucket_idx = self.make_hash(&entry.primary_key()) % self.bucket_num;
        self.buckets[bucket_idx].push(entry)
    }

    pub fn retrieve(&self, index: &Index) -> Option<&Row<Entry>> {
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
where Entry: Key<Index> + Clone,
      Index: Eq+Hash 
{
    fn default() -> Self {
        let mut buckets = Vec::with_capacity(16);

        for _ in 0..16{
            buckets.push(Bucket::new());
        }
        
        Table {
            buckets,
            bucket_num: 16,
            hash_builder : Default::default(),
        }
    }
}

pub struct Bucket<Entry, Index> 
where Entry: Key<Index> + Clone,
      Index: Eq+Hash 
{
    rows: RwLock<RawVec<Row<Entry>>>,
    index: RwLock<HashMap<Index, usize>>,
    len : AtomicUsize,
}

impl<Entry, Index> Bucket<Entry, Index> 
where Entry: Key<Index> + Clone,
      Index: Eq+Hash
{
    pub fn new() -> Bucket<Entry, Index> {
        Bucket {
            rows: RwLock::new(RawVec::new()),
            len: AtomicUsize::new(0),
            index: RwLock::new(HashMap::new()),
        }
    }

    pub fn push(&self, entry : Entry) {
        let prev_len = self.len.fetch_add(1, Ordering::Acquire);
        if prev_len == self.cap() {
            let mut rw = self.rows.write().unwrap();
            rw.double(); /* This may OOM */
        } else if prev_len > self.cap() {
            //FIXME: busy wait here maybe
            panic!("hmmm, someone else should have been doubling");
        }
        let idx_elem = entry.primary_key();
        unsafe {
            ptr::write(self.ptr().offset(prev_len as isize), Row::new(entry));
        }
        
        let mut idx_map = self.index.write().unwrap();
        idx_map.insert(idx_elem, prev_len);
    }

    pub fn retrieve(&self, index_elem: &Index) -> Option<&Row<Entry>> {
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

    fn ptr(&self) -> *mut Row<Entry> {
        let rows = self.rows.read().unwrap();
        rows.ptr()
    }
}


pub struct Row<Entry> 
where Entry : Clone
{
    data_: UnsafeCell<Entry>,
    vers_: TVersion,
}


impl<Entry>  Row<Entry> 
where Entry: Clone
{
    pub fn new(entry: Entry) -> Row<Entry>{
        Row{
            data_: UnsafeCell::new(entry),
            vers_: TVersion::default(), /* FIXME: this can carry txn info */
        }
    }

    pub fn new_from_txn(entry : Entry, txn_info: TxnInfo) -> Row<Entry> {
        Row {
            data_ : UnsafeCell::new(entry),
            vers_ : TVersion::new_with_info(txn_info),
        }
    }

    pub fn get_ref(&self) -> &Entry {
        unsafe { self.data_.get().as_ref().unwrap() }
    }

    pub fn get_mut(&self) -> &mut Entry {
        unsafe { self.data_.get().as_mut().unwrap() }
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
    pub fn get_ptr(&self) -> *mut Entry {
        self.data_.get()
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
}



