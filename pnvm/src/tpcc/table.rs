//****************************************
//TPCC tables implementations and index map 
//
//
//Basic Types: 
//- Table<Entry, Index>     a table with many buckets
//- Bucket<Entry, Index>    a single partition 
//- Row<Entry, Index>       a row with transactional implementation
//- SecIndex                secondary index map for range queries 
//- SecIndexBucket          partition for the secondary index
//
//****************************************

use alloc::alloc::Layout;

#[cfg(feature="pmem")]
use pnvm_sys;

use std::{
    sync::atomic::{AtomicUsize, Ordering, AtomicBool, AtomicPtr},
    sync::{Arc,RwLock},
    collections::{
        HashMap,
        hash_map::RandomState,
        VecDeque,
    },
    cell::{RefCell, UnsafeCell},
    ptr::{self, NonNull},
    hash::{self,Hash, BuildHasher, Hasher},
    fmt::{self, Debug},
    mem,
    any::TypeId,    
    str,
    char,
    iter::Iterator,
};

use num::iter::Range;

use pnvm_lib::tcore::{TVersion, ObjectId, OidFac, TRef};
use pnvm_lib::txn::{Tid,TxnInfo, Transaction};
use pnvm_lib::occ::occ_txn::TransactionOCC;
use pnvm_lib::parnvm::nvm_txn_occ::TransactionParOCC;
use super::entry::*;

//FIXME: const
use super::workload_occ::*;
use super::tpcc_tables::*;

const PMEM_DIR_ROOT : &str = "/home/v-xuc/ParNVM/data/";


pub struct SecIndex<K, V>
where K: Hash + Eq + Debug,
      V: Debug,
{
   get_bucket_ : Box<Fn(&K) -> usize>,
   buckets_ : Vec<SecIndexBucket<K, V>>,
}

/* V is not necessary the Primary key */
impl<K, V> SecIndex<K, V>
where K: Hash + Eq + Debug,
      V: Debug,
{
    pub fn new(f: Box<Fn(&K)->usize>) -> SecIndex<K, V> 
    { SecIndex {
            buckets_ : Vec::new(),
            get_bucket_: f,
        }
    }

    pub fn new_with_buckets(bucket_num : usize, f: Box<Fn(&K) -> usize>) -> SecIndex<K, V> 
    {
        let mut buckets = Vec::with_capacity(bucket_num);

        for _ in 0..bucket_num {
            buckets.push(SecIndexBucket::new()); 
        }

        SecIndex {
            buckets_ : buckets,
            get_bucket_: f,
        }
    }

    pub fn insert_index(&self, key: K, val: V) 
    {
        self.buckets_[(self.get_bucket_)(&key)].insert_index(key, val);
    }

    pub fn unlock_bucket(&self, key: &K) 
    {
        self.buckets_[(self.get_bucket_)(key)].unlock();
    }

    pub fn find_one_bucket(&self, key: &K) ->  Option<&VecDeque<V>> 
    {
        self.buckets_[(self.get_bucket_)(key)].find_many(key)
    }

    pub fn find_one_bucket_mut(&self, key: &K) ->  Option<&mut VecDeque<V>> 
    {
        self.buckets_[(self.get_bucket_)(key)].find_many_mut(key)
    }

}



impl<K, V> Debug for SecIndex<K, V>
where K: Hash + Eq + Debug,
      V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.buckets_)
    }
}


struct SecIndexBucket<K, V>
where K: Hash+ Eq+ Debug,
      V: Debug
{
    index_ : UnsafeCell<HashMap<K, VecDeque<V>>>,
    lock_ : AtomicBool,
}

impl<K, V> SecIndexBucket<K, V>
where K: Hash+Eq+Debug,
      V: Debug
{
    pub fn new() -> SecIndexBucket<K, V>
    {
        SecIndexBucket {
            index_ : UnsafeCell::new(HashMap::new()),
            lock_ : AtomicBool::new(false),
        }
    }

    pub fn index(&self) -> &HashMap<K, VecDeque<V>> {
        self.lock(); /* Spin locks */
        unsafe { self.index_.get().as_ref().unwrap() }
    }

    pub fn index_mut(&self) -> &mut HashMap<K, VecDeque<V>> {
        self.lock();
        unsafe { self.index_.get().as_mut().unwrap() }
    }
    
    fn lock(&self) {
        while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
    }

    pub fn unlock(&self) {
        self.lock_.store(false, Ordering::SeqCst);
    }

    fn insert_index(&self, key: K, val : V) {
        let ids = self.index_mut()
            .entry(key)
            .or_insert_with(|| VecDeque::new());

        ids.push_back(val);

        
        /* Delay unlock until the data is pushed */
    }
    
    /* FIXME: Allocating new arrays? */
    fn find_many(&self, key: &K) -> Option<&VecDeque<V>> {
        self.index().get(key)
    }

    fn find_many_mut(&self, key: &K) -> Option<&mut VecDeque<V>> {
        self.index_mut().get_mut(key)
    }
}

impl<K, V> Debug for SecIndexBucket<K, V>
where K: Hash + Eq + Debug,
      V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unsafe {
            write!(f, "{:?}", self.index_.get().as_ref().unwrap())
        }
    }
}


//pub type  OrderLineTable = Table<OrderLine, (i32, i32, i32, i32)>;
pub type  ItemTable = Table<Item, i32>;
pub type  StockTable = Table<Stock, (i32, i32)>;

//FIXME: 
//pub type HistoryTable = NonIndexTable<History>;
pub type HistoryTable = Table<History, (i32, i32)>; /* No primary key in fact */


pub type TablesRef = Arc<Tables>;


pub trait TableRef
{
    fn into_table_ref(self, Option<usize>,Option<Arc<Tables>>) -> Box<dyn TRef>;
}

pub trait BucketDeleteRef {
    fn into_delete_table_ref(self, usize, Arc<Tables>) -> Box<dyn TRef>;
}

pub trait BucketPushRef {
    fn into_push_table_ref(self, usize, Arc<Tables>) -> Box<dyn TRef>;
}


pub trait Key<T> {
    fn primary_key(&self) -> T;

    fn bucket_key(&self) -> usize;
}


#[derive(Debug)]
pub struct Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone + Debug,
      Index: Eq+Hash + Clone+Debug,
{
    buckets : Vec<Bucket<Entry, Index>>,
    bucket_num: usize,
    
    //len :usize,
    hash_builder: RandomState,
    name : String,
    
     //id_ : ObjectId,
    //vers_ : TVersion,
}


impl<Entry, Index> Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash+Clone + Debug,
{
    pub fn new() -> Table<Entry, Index> {
       Default::default() 
    }

    pub fn new_with_buckets(num: usize, bkt_size: usize, name: &str) -> Table<Entry, Index> {
        let mut buckets = Vec::with_capacity(num);
        for _ in 0..num {
            buckets.push(Bucket::with_capacity(bkt_size));
        }

        Table {
            buckets,
            bucket_num : num,
            hash_builder: Default::default(),
            name : String::from(name)
        }
    }
    
    pub fn push_pc(&self, tx: &mut TransactionParOCC, entry: Entry, tables: &Arc<Tables>)
    where Arc<Row<Entry, Index>> :BucketPushRef 
    {
        let bkt_idx = entry.bucket_key() % self.bucket_num;

        //Make into row and then make into a RowRef
        let row = Arc::new(Row::new_from_txn(entry, tx.txn_info().clone()));
        let table_ref = row.into_push_table_ref(bkt_idx, tables.clone());
        
        let tid = tx.id().clone();
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone(), OPERATION_CODE_PUSH);
        tag.set_write();
        debug!("[PUSH TABLE]--[TID:{:?}]--[OID:{:?}]", tid, table_ref.get_id());
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Entry, tables: &Arc<Tables>)
    where Arc<Row<Entry, Index>> :BucketPushRef 
    {
        let bkt_idx = entry.bucket_key() % self.bucket_num;

        //Make into row and then make into a RowRef
        let row = Arc::new(Row::new_from_txn(entry, tx.txn_info().clone()));
        let table_ref = row.into_push_table_ref(bkt_idx, tables.clone());
        
        let tid = tx.id().clone();
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone(), OPERATION_CODE_PUSH);
        tag.set_write();
        debug!("[PUSH TABLE]--[TID:{:?}]--[OID:{:?}]", tid, table_ref.get_id());
    }

    pub fn delete_pc(&self, tx: &mut TransactionParOCC, index: &Index, tables: &Arc<Tables>, bucket_idx: usize) -> bool
        where Arc<Row<Entry, Index>> : BucketDeleteRef
    {
        let bucket_idx = bucket_idx % self.bucket_num;
        let row = match self.buckets[bucket_idx].retrieve(index){
            None => { 
                warn!("tx_delete: no element {:?}", index);
                return false;
            },
            Some(row) => row
        };
        let table_ref = row.into_delete_table_ref(
            bucket_idx,
            tables.clone(),
            );
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone(), OPERATION_CODE_DELETE);
        tag.set_write(); //FIXME: better way?
        true
    }


    pub fn delete(&self, tx: &mut TransactionOCC, index: &Index, tables: &Arc<Tables>, bucket_idx: usize) -> bool
        where Arc<Row<Entry, Index>> : BucketDeleteRef
    {
        let bucket_idx = bucket_idx % self.bucket_num;
        let row = match self.buckets[bucket_idx].retrieve(index){
            None => { 
                warn!("tx_delete: no element {:?}", index);
                return false;
            },
            Some(row) => row
        };
        let table_ref = row.into_delete_table_ref(
            bucket_idx,
            tables.clone(),
            );
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone(), OPERATION_CODE_DELETE);
        tag.set_write(); //FIXME: better way?
        true
    }

    pub fn push_raw(&self, entry: Entry) 
    {
        let bkt_idx = entry.bucket_key() % self.bucket_num;
        self.buckets[bkt_idx].push_raw(entry);
    }

    pub fn retrieve(&self, index: &Index, bucket_idx: usize) -> Option<Arc<Row<Entry, Index>>> {
        self.buckets[bucket_idx % self.bucket_num].retrieve(index)
    }

   // fn make_hash(&self, idx : &Index) -> usize {
   //     let mut s = self.hash_builder.build_hasher();
   //     idx.hash(&mut s);
   //     s.finish() as usize
   // }

   // fn get_bucket_idx(&self, key: &Index) -> usize 
   // {
   //     self.make_hash(key) % self.bucket_num
   // }

    pub fn get_bucket(&self, bkt_idx : usize) -> &Bucket<Entry, Index>{
        info!("------------[TABLE] getting bucket {}-------", bkt_idx);
        &self.buckets[bkt_idx % self.bucket_num]
    }
}

impl<Entry, Index> Default for Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone +Debug,
      Index: Eq+Hash  + Clone + Debug,
{
    fn default() -> Self {
        let mut buckets = Vec::with_capacity(16);

        for _ in 0..16{
            buckets.push(Bucket::with_capacity(1024));
        }
        
        Table {
            buckets,
            bucket_num: 16,
            hash_builder : Default::default(), 
            name: String::from("default")
        }
        
    }
}


//impl<Entry, Index> Drop for Table<Entry, Index> 
//where Entry: 'static + Key<Index> + Clone +Debug,
//      Index: Eq+Hash  + Clone + Debug,
//{
//    fn drop(&mut self) {
//        println!("Dropping table {}", self.name);
//        //if self.name == "stock" {
//        //    println!("{:?}", self.buckets);
//        //}
//        
//    }
//}
//

const PMEM_PAGE_ENTRY_NUM: usize = 1 << 10;

/* FIXME: can we avoid the copy */
pub struct Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone,
{
    rows: UnsafeCell<Vec<Arc<Row<Entry, Index>>>>,
    index: UnsafeCell<HashMap<Index, usize>>,
    id_ : ObjectId,
    vers_ : TVersion,
    #[cfg(feature = "pmem")]
    pmem_root_ : RefCell<Vec<NonNull<Entry>>>,
    pmem_cap_ : AtomicUsize,
    pmem_per_size_ : usize,

}

impl<Entry, Index> Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash+ Clone,
{
   // pub fn new() -> Bucket<Entry, Index> {
   //     Bucket {
   //         rows: UnsafeCell::new(Vec::new()),
   //         index: UnsafeCell::new(HashMap::new()),

   //         id_ : OidFac::get_obj_next(),
   //         vers_ : TVersion::default(),
   //         pmem_root_: 
   //     }
   // }

    pub fn with_capacity(cap: usize) -> Bucket<Entry, Index> 
    {
        let mut bucket = Bucket {
            rows: UnsafeCell::new(Vec::with_capacity(cap)),
            index: UnsafeCell::new(HashMap::with_capacity(cap)),

            id_ : OidFac::get_obj_next(),
            vers_ : TVersion::default(),

            #[cfg(feature="pmem")]
            pmem_root_: RefCell::new(Vec::new()),
            pmem_cap_: AtomicUsize::new(cap),
            pmem_per_size_ : cap,
        };

        /* Get the persistent memory */
        #[cfg(feature = "pmem")]
        {
            let mut path = String::from(PMEM_DIR_ROOT);
            let size =  cap *  mem::size_of::<Entry>();
            //path.push_str(name);
            let pmem_root = pnvm_sys::mmap_file(path, size) as *mut Entry;

            if pmem_root.is_null() {
                panic!("Bucket::with_capacity(): failed, len: {}", size);
            }

            bucket.pmem_root_.borrow_mut().push( NonNull::new(pmem_root).unwrap());
        }
        bucket
    }

    /* Insert a row. 
     * It is guaranteed that no data race is possible by the contention algo
     * */
    pub fn push(&self, row_arc : Arc<Row<Entry, Index>>) {
        debug!("[PUSH ROW] : {:?}", *row_arc);
        assert_eq!(self.vers_.get_count() > 0 , true);
        assert_eq!(self.vers_.get_locker() == 0, false);
        let idx_elem = row_arc.get_data().primary_key();
        unsafe {
            let mut rows = self.rows.get().as_mut().unwrap();
            rows.push(row_arc.clone());
            let mut idx_map = self.index.get().as_mut().unwrap();
            idx_map.insert(idx_elem, self.len() -1);

            #[cfg(feature = "pmem")]
            row_arc.set_pmem_addr(self.get_pmem_addr(self.len() -1));
        }
    }

    pub fn delete(&self, row_arc: Arc<Row<Entry, Index>>) {
        assert_eq!(self.vers_.get_count() > 0 , true);
        assert_eq!(self.vers_.get_locker() == 0, false);
        let idx_elem = row_arc.get_data().primary_key();

        /* FIXME: Leave the data in the rows */
        unsafe {
            let mut idx_map = self.index.get().as_mut().unwrap();
            idx_map.remove(&idx_elem);
        }
    }

    fn push_raw(&self, entry: Entry) {
        let idx_elem = entry.primary_key();
        unsafe {
            let mut rows = self.rows.get().as_mut().unwrap();
            let mut idx_map = self.index.get().as_mut().unwrap();
            let arc = Arc::new(Row::new(entry));
            rows.push(arc.clone());
            idx_map.insert(idx_elem, self.len()-1);
            #[cfg(feature="pmem")]
            arc.set_pmem_addr(self.get_pmem_addr(self.len()-1));
        }
    }

    #[cfg(feature ="pmem")]
    fn get_pmem_addr(&self, idx : usize) -> *mut Entry {
        if idx >= self.pmem_cap_.load(Ordering::SeqCst) {
            //TODO: resize 
            let path = String::from(PMEM_DIR_ROOT);
            let size = self.pmem_per_size_ * mem::size_of::<Entry>();
            let pmem_root = pnvm_sys::mmap_file(path, size) as *mut Entry;

            self.pmem_root_.borrow_mut().push(NonNull::new(pmem_root).unwrap());
            /* Exponential increase the cap here */
            self.pmem_cap_.fetch_add(self.pmem_per_size_, Ordering::SeqCst);
        } 
        
        //Find pmem_page_id
        
        let pmem_page_id = idx / self.pmem_per_size_;
        let roots = self.pmem_root_.borrow();
        unsafe {
            roots[pmem_page_id].as_ptr()
                .offset((idx % self.pmem_per_size_) as isize)
        }

    }


    pub fn retrieve(&self, index_elem: &Index) -> Option<Arc<Row<Entry, Index>>> { 
        //Check out of bound
        let index = unsafe {self.index.get().as_ref().unwrap()};
        match index.get(index_elem) {
            None => None,
            Some(idx) => {
                let rows = unsafe {self.rows.get().as_ref().unwrap()};
                Some(rows.get(*idx).expect("row should not be empty. inconsistent with index").clone())
                //unsafe {
                //    rows.ptr().offset(*idx as isize).as_ref()
                //}
            }
        }
    }

   fn cap(&self) -> usize {
       let rows = unsafe {self.rows.get().as_ref().unwrap()};
       rows.capacity()
   }


    fn len(&self) -> usize {
        let rows = unsafe {self.rows.get().as_ref().unwrap()};
        rows.len()
    }




    #[inline(always)]
    pub fn lock(&self, tid: Tid) -> bool {
        self.vers_.lock(tid)
    }

    #[inline(always)]
    pub fn check(&self, cur_ver: u32, tid: u32) -> bool {
        self.vers_.check_version(cur_ver, tid)
    }

    //FIXME: how to not Clone
   // #[inline]
   // pub fn install(&self, val: &Entry, tid: Tid) {
   //     unsafe {
   //         debug!("\n[TRANSACTION:{:?}]--[INSTALL]\n\t\t[OLD]--{:?}\n\t\t[NEW]--{:?}",
   //                tid, self.data_.get().as_ref().unwrap(), val);

   //         ptr::write(self.data_.get(), val.clone());
   //     }
   //     self.vers_.set_version(tid);
   // }

    #[inline(always)]
    pub fn unlock(&self) {
        self.vers_.unlock();
    }

    
    #[inline(always)]
    pub fn get_version(&self) -> u32 {
        self.vers_.get_version()
    }

    #[inline(always)]
    pub fn set_version(&self, vers: u32) {
        self.vers_.set_version(vers)
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
        Layout::new::<Bucket<Entry, Index>>()
    }

    pub fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.vers_.get_writer_info()
    }

    pub fn set_writer_info(&self, info : Arc<TxnInfo>) {
        self.vers_.set_writer_info(info)
    }


}

unsafe impl<Entry, Index> Sync for Bucket<Entry, Index>
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone, 
{}


unsafe impl<Entry, Index> Send for Bucket<Entry, Index>
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone,
{}

impl<Entry, Index> Debug for Bucket<Entry, Index>
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       //try locks ?
        unsafe {
            let rows = self.rows.get().as_ref().unwrap();
            let map = self.index.get().as_ref().unwrap();
            write!(f, "{:#?}\n{:#?}", rows, map)
        }
    }
}

pub struct Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone
{
    //data_: UnsafeCell<Entry>,
    data_ : NonNull<Entry>,
    vers_: TVersion,
    id_ : ObjectId,
    index_ : Index,

    #[cfg(feature = "pmem")]
    pmem_addr_ : AtomicPtr<Entry>,
}

impl<Entry, Index> Debug for Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
     //   unsafe {write!(f, "[OID: {:?}][VERS: {:?}]\n\t[{:?}]", 
     //                  self.id_, self.vers_,self.data_.get().as_ref().unwrap())}
        unsafe {write!(f, "[OID: {:?}][VERS: {:?}]\n\t[{:?}]", 
                       self.id_, self.vers_,self.data_.as_ref())}
    }
}

impl<Entry, Index> Drop for Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone
{
    fn drop(&mut self) {
        if self.data_.as_ptr().is_null() {
            panic!("freeing null pointers")
        } else {
           // if TypeId::of::<Entry>() == TypeId::of::<Customer>() {
           //     println!("{:?}", self.get_data());
           // }
            let _data = self.get_data();
            unsafe {self.data_.as_ptr().drop_in_place()}
        }

        //println!("{:?}", self);
        //mem::forget(self.vers_);
    }
}
//impl<Entry, Index> Clone for Row<Entry, Index>
//where Entry: 'static + Key<Index> + Clone,
//      Index: Eq+Hash  + Clone
//{
//    fn clone(&self) -> Self {
//        Row {
//            data_ : unsafe {UnsafeCell::new(self.data_.get().as_ref().unwrap().clone())},
//            vers_ : self.vers_.clone(),
//            id_: self.id_,
//            index_ : self.index_.clone()
//        }
//    }
//}

unsafe impl<Entry: Clone, Index> Sync for Row<Entry, Index>
where Entry: 'static + Key<Index> + Clone + Debug,
      Index: Eq+Hash  + Clone
{}
unsafe impl<Entry: Clone, Index> Send for Row<Entry, Index>
where Entry: 'static + Key<Index> + Clone + Debug,
      Index: Eq+Hash  + Clone
{}

impl<Entry, Index>  Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone + Debug,
      Index: Eq+Hash  + Clone
{
    pub fn new(entry: Entry) -> Row<Entry, Index>{
        let key = entry.primary_key();
        Row{
            //data_: UnsafeCell::new(entry),
            data_ : Box::into_raw_non_null(Box::new(entry)),
            vers_: TVersion::default(), /* FIXME: this can carry txn info */
            id_ : OidFac::get_obj_next(),
            index_ : key, 
            
            #[cfg(feature= "pmem")]
            pmem_addr_: AtomicPtr::default(),
        }
    }

    pub fn new_from_txn(entry : Entry, txn_info: Arc<TxnInfo>) -> Row<Entry, Index> {
        let key = entry.primary_key();
        Row {
            //data_ : UnsafeCell::new(entry),
            data_ : Box::into_raw_non_null(Box::new(entry)),
            vers_ : TVersion::new_with_info(txn_info),
            id_ : OidFac::get_obj_next(),
            index_ : key,

            #[cfg(feature= "pmem")]
            pmem_addr_: AtomicPtr::default(),
        }
    }


    #[cfg(feature="pmem")]
    pub fn set_pmem_addr(&self, addr : *mut Entry) {
        self.pmem_addr_.store(addr, Ordering::SeqCst);        
    }
    

    #[cfg(feature="pmem")]
    pub fn get_pmem_addr(&self) -> *mut Entry {
        self.pmem_addr_.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn get_data(&self) -> &Entry {
        //unsafe { self.data_.get().as_ref().unwrap() }
        unsafe { self.data_.as_ref() }
    }

    #[inline(always)]
    pub fn get_ptr(&self) -> *mut u8 {
        //unsafe {self.data_.get() as *mut u8}
        self.data_.as_ptr() as *mut u8
    }


    #[inline(always)]
    pub fn lock(&self, tid: Tid) -> bool {
        self.vers_.lock(tid)
    }

    #[inline(always)]
    pub fn check(&self, cur_ver: u32, tid: u32) -> bool {
        self.vers_.check_version(cur_ver, tid)
    }

    //FIXME: how to not Clone
    #[inline]
    pub fn install(&self, val: &Entry, tid: Tid) {
        unsafe {
            //debug!("\n[TRANSACTION:{:?}]--[INSTALL]\n\t\t[OLD]--{:?}\n\t\t[NEW]--{:?}",
            //      tid, self.data_.get().as_ref().unwrap(), val);

            //ptr::write(self.data_.get(), val.clone());
            let mut data = self.data_.as_ptr() ;
            *data = val.clone();
        }
        self.vers_.set_version(tid.into());
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
}



#[derive(Clone, Debug)]
pub enum Operation {
    RWrite,
    Delete,
    Push,
}

const OPERATION_CODE_PUSH :i8 = 0;
const OPERATION_CODE_DELETE : i8 = 1;
