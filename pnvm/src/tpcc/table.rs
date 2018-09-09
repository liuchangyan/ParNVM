
use alloc::alloc::Layout;

use std::{
    sync::atomic::{AtomicUsize, Ordering, AtomicBool},
    sync::{Arc,RwLock},
    collections::{
        HashMap,
        hash_map::RandomState,
    },
    cell::UnsafeCell,
    ptr::{self, NonNull},
    hash::{self,Hash, BuildHasher, Hasher},
    fmt::{self, Debug},
    mem,
    
};

use pnvm_lib::tcore::{TVersion, ObjectId, OidFac, TRef};
use pnvm_lib::txn::{Tid,TxnInfo};
use pnvm_lib::occ::occ_txn::TransactionOCC;
use super::entry::*;



pub type WarehouseTable = Table<Warehouse, i32>;
pub type DistrictTable = Table<District, (i32, i32)>;

#[derive(Debug)]
pub struct CustomerTable {
    table_ : Table<Customer, (i32, i32, i32)>,
    
    //c_last, c_w_id, c_d_id => c_w_id, c_d_id, c_id
    name_index_ : UnsafeCell<HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>>,  

    lock_ : AtomicBool,
}


impl CustomerTable {
    pub fn new() -> CustomerTable {
        CustomerTable {
            table_ : Table::new(),
            name_index_ : UnsafeCell::new(HashMap::new()),
            lock_: AtomicBool::new(false),
        }
    }

    pub fn new_with_buckets(num : usize , bkt_size : usize) -> CustomerTable {
        CustomerTable {
            table_ : Table::new_with_buckets(num, bkt_size),
            name_index_ : UnsafeCell::new(HashMap::new()),
            lock_ : AtomicBool::new(false),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Customer, tables :&Arc<Tables>)
        where Arc<Row<Customer, (i32, i32, i32)>> : TableRef 
        {
            self.table_.push(tx, entry, tables);
        }


    fn name_index(&self) -> &HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>
    {
        while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
        unsafe { self.name_index_.get().as_ref().unwrap() }
    }


    fn name_index_mut(&self) -> &mut HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>
    {
        while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
        unsafe { self.name_index_.get().as_mut().unwrap() }
    }

    pub fn push_raw(&self, entry: Customer) 
    {
        /* Indexes Updates */
        let p_key = entry.primary_key();
        let name = entry.c_last.clone();
        let mut id_vec = self.name_index_mut()
            .entry((name, entry.c_w_id, entry.c_d_id))
            .or_insert_with(|| Vec::new());

        id_vec.push(p_key);

        //println!("PUSHING CUSTOMER {}, {}, {}", entry.c_id, entry.c_w_id, entry.c_d_id);
        self.table_.push_raw(entry);
        self.lock_.store(false, Ordering::SeqCst);

    }

    //FIXME: deleting an entry needs to be fixed 
    pub fn update_sec_index(&self, arc: &Arc<Row<Customer, (i32, i32, i32)>>) {
        let c  = arc.get_data();

        let mut id_vec = self.name_index_mut()
            .entry((c.c_last.clone(), c.c_w_id, c.c_d_id))
            .or_insert_with(|| Vec::new());
        id_vec.push(c.primary_key());
        self.lock_.store(false, Ordering::SeqCst);
    }

    pub fn retrieve(&self, index :&(i32, i32, i32)) -> Option<Arc<Row<Customer, (i32, i32, i32)>>>
    {
        self.table_.retrieve(index)
    }

    pub fn get_bucket(&self, bkt_idx : usize ) -> &Bucket<Customer, (i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }

    pub fn find_by_name_id(&self, index : &(String, i32, i32))
        -> Vec<Arc<Row<Customer, (i32, i32, i32)>>>
        {
        match self.name_index().get(index) {
            None => {
                Vec::new()
            },
            Some(vecs) => {
                let mut ret = vecs.iter()
                    .filter_map(|id| self.table_.retrieve(id))
                    .collect::<Vec<_>>();
                
                ret.sort_unstable_by(|a, b| a.get_data().c_first.cmp(&b.get_data().c_first));
                ret
            }
        }

    }
}

unsafe impl Sync for CustomerTable {}
unsafe impl Send for CustomerTable {}

pub type NewOrderTable = Table<NewOrder, (i32, i32, i32)>;
//pub type  OrderTable = Table<Order, (i32, i32, i32)>;


pub struct OrderTable {
    table_ : Table<Order, (i32, i32, i32)>;
}

pub type  OrderLineTable = Table<OrderLine, (i32, i32, i32, i32)>;
pub type  ItemTable = Table<Item, i32>;
pub type  StockTable = Table<Stock, (i32, i32)>;

//FIXME: 
//pub type HistoryTable = NonIndexTable<History>;
pub type HistoryTable = Table<History, (i32)>; /* No primary key in fact */

#[derive(Debug)]
pub struct Tables {
   pub warehouse: WarehouseTable,
   pub district: DistrictTable,
   pub customer: CustomerTable,
   pub neworder: NewOrderTable,
   pub order: OrderTable,
   pub orderline: OrderLineTable,
   pub item: ItemTable,
   pub stock: StockTable,
   pub history: HistoryTable,
}

pub type TablesRef = Arc<Tables>;


pub trait TableRef
{
    fn into_table_ref(self, Option<usize>,Option<Arc<Tables>>) -> Box<dyn TRef>;
}


pub trait Key<T> {
    fn primary_key(&self) -> T;
}


#[derive(Debug)]
pub struct Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone + Debug,
      Index: Eq+Hash + Clone,
{
    buckets : Vec<Bucket<Entry, Index>>,
    bucket_num: usize,
    
    //len :usize,
    hash_builder: RandomState,
    
     //id_ : ObjectId,
    //vers_ : TVersion,
}

//impl<Entry, Index> Debug for Table <Entry, Index>
//where Entry: 'static + Key<Index> + Clone ,
//      Index: Eq+Hash+Clone,
//{
//    fn fmt(&self, f : &mut Formatter) -> Result {
//            write!(f, 
//
//    }
//
//}



impl<Entry, Index> Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash+Clone,
{
    pub fn new() -> Table<Entry, Index> {
       Default::default() 
    }

    pub fn new_with_buckets(num: usize, bkt_size: usize) -> Table<Entry, Index> {
        let mut buckets = Vec::with_capacity(num);
        for _ in 0..num {
            buckets.push(Bucket::with_capacity(bkt_size));
        }

        Table {
            buckets,
            bucket_num : num,
            hash_builder: Default::default(),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Entry, tables: &Arc<Tables>)
    where Arc<Row<Entry, Index>> : TableRef
    {
        let bucket_idx = self.make_hash(&entry.primary_key()) % self.bucket_num;
        
        //Make into row and then make into a RowRef
        let row = Arc::new(Row::new_from_txn(entry, tx.txn_info().clone()));
        let table_ref = row.into_table_ref(Some(bucket_idx), Some(tables.clone()));
        
        let tid = tx.commit_id().clone();
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone());
        tag.set_write();
        debug!("[PUSH TABLE]--[TID:{:?}]--[OID:{:?}]", tid, table_ref.get_id());
    }

    pub fn push_raw(&self, entry: Entry) 
    {
        let bucket_idx = self.make_hash(&entry.primary_key()) % self.bucket_num;
        self.buckets[bucket_idx].push_raw(entry);
    }

    pub fn retrieve(&self, index: &Index) -> Option<Arc<Row<Entry, Index>>> {
        let bucket_idx = self.make_hash(&index) % self.bucket_num;
        self.buckets[bucket_idx].retrieve(index)
    }

    fn make_hash(&self, idx : &Index) -> usize {
        let mut s = self.hash_builder.build_hasher();
        idx.hash(&mut s);
        s.finish() as usize
    }

    pub fn get_bucket(&self, bkt_idx : usize) -> &Bucket<Entry, Index>{
        &self.buckets[bkt_idx]
    }
}

impl<Entry, Index> Default for Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone +Debug,
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
#[derive(Debug)]
pub struct Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone,
{
    rows: RwLock<Vec<Arc<Row<Entry, Index>>>>,
    index: RwLock<HashMap<Index, usize>>,
    len : AtomicUsize,

    id_ : ObjectId,
    vers_ : TVersion,
}

impl<Entry, Index> Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash+ Clone,
{
    pub fn new() -> Bucket<Entry, Index> {
        Bucket {
            rows: RwLock::new(Vec::new()),
            len: AtomicUsize::new(0),
            index: RwLock::new(HashMap::new()),

            id_ : OidFac::get_obj_next(),
            vers_ : TVersion::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Bucket<Entry, Index> 
    {
        Bucket {
            rows: RwLock::new(Vec::with_capacity(cap)),
            len: AtomicUsize::new(0),
            index: RwLock::new(HashMap::new()),

            id_ : OidFac::get_obj_next(),
            vers_ : TVersion::default(),
        }

    }

    pub fn push(&self, row_arc : Arc<Row<Entry, Index>>) {
        debug!("[PUSH ROW] : {:?}", *row_arc);
        let idx_elem = row_arc.get_data().primary_key();
        {
            let mut rows = self.rows.write().unwrap();
            rows.push(row_arc);
        }

        let mut idx_map = self.index.write().unwrap();
        idx_map.insert(idx_elem, self.len() -1);
    }

    fn push_raw(&self, entry: Entry) {
        let idx_elem = entry.primary_key();
        {
            let mut rows = self.rows.write().unwrap();
            rows.push(Arc::new(Row::new(entry)));
        }

        let mut idx_map = self.index.write().unwrap();
        idx_map.insert(idx_elem, self.len()-1);
    }

    pub fn retrieve(&self, index_elem: &Index) -> Option<Arc<Row<Entry, Index>>> { 
        //Check out of bound
        let index = self.index.read().unwrap();
        match index.get(index_elem) {
            None => None,
            Some(idx) => {
                let rows = self.rows.read().unwrap();
                Some(rows.get(*idx).expect("row should not be empty. inconsistent with index").clone())
                //unsafe {
                //    rows.ptr().offset(*idx as isize).as_ref()
                //}
            }
        }
    }

   fn cap(&self) -> usize {
       let rows = self.rows.read().unwrap();
       rows.capacity()
   }

   // fn ptr(&self) -> *mut Arc<Row<Entry, Index>> {
   //     let rows = self.rows.read().unwrap();
   //     rows.ptr()
   // }


    fn len(&self) -> usize {
        let rows = self.rows.read().unwrap();
        rows.len()
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

pub struct Row<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone
{
    //data_: UnsafeCell<Entry>,
    data_ : NonNull<Entry>,
    vers_: TVersion,
    id_ : ObjectId,
    index_ : Index,
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
            unsafe {self.data_.as_ptr().drop_in_place()}
        }
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
        }
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
    pub fn check(&self, cur_ver: u32) -> bool {
        self.vers_.check_version(cur_ver)
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

   // pub fn read(&self, tx : &mut TransactionOCC) -> &Entry
   //     where Arc<Row<Entry, Index>> : TableRef
   // {
   //     let tref = self.clone().intotable__ref(None, tx.txn_info(), None);
   //     let id = *tref.get_id();
   //     let old_vers = tref.get_version();

   //     let tag = tx.retrieve_tag(&id, tref);
   //     tag.add_version(old_vers);
   //     tag.get_data()
   // }

   // pub fn write(&self, tx: &mut TransactionOCC, val: Entry)
   //     where Arc<Row<Entry, Index>> : TableRef
   // {
   //     let tref = self.clone().intotable__ref(None, tx.txn_info(), None);
   //     let id = *tref.get_id();
   //     let mut tag = tx.retrieve_tag(&id, tref);
   //     tag.write(val);
   // }

    /* Transaction Methods */
}





