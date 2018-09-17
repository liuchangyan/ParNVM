
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
    any::TypeId,    
    str,
    char,
    iter::Iterator,
};

use num::iter::Range;

use pnvm_lib::tcore::{TVersion, ObjectId, OidFac, TRef};
use pnvm_lib::txn::{Tid,TxnInfo};
use pnvm_lib::occ::occ_txn::TransactionOCC;
use super::entry::*;

//FIXME: const
use super::workload::*;



pub type WarehouseTable = Table<Warehouse, i32>;
pub type DistrictTable = Table<District, (i32, i32)>;
//pub type CustomerTable = Table<Customer, (i32, i32, i32)>;

#[derive(Debug)]
pub struct CustomerTable {
    pub name_index_ : SecIndex<(String, i32, i32), (i32, i32,i32, [u8;16])>,
    table_ : Table<Customer, (i32, i32, i32)>,
    
    //c_last, c_w_id, c_d_id => c_w_id, c_d_id, c_id
    //name_index_ : UnsafeCell<HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>>,  
}


impl CustomerTable {
   // pub fn new() -> CustomerTable {
   //     CustomerTable {
   //         table_ : Table::new(),
   //         name_index_ : SecIndex::new(),
   //     }
   // }

    pub fn new_with_buckets(num : usize , bkt_size : usize, name: &str) -> CustomerTable {
        let total_wd = NUM_WAREHOUSES * NUM_INIT_DISTRICT;
        CustomerTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            name_index_ : SecIndex::new_with_buckets(total_wd as usize, Box::new(move |key| {
                let (_ , w_id, d_id) = key;          
                ((w_id * NUM_WAREHOUSES + d_id)% total_wd) as usize
            })),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Customer, tables :&Arc<Tables>)
        where Arc<Row<Customer, (i32, i32, i32)>> : TableRef 
        {
            self.table_.push(tx, entry, tables);
        }


   // fn name_index(&self) -> &HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>
   // {
   //     while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
   //     unsafe { self.name_index_.get().as_ref().unwrap() }
   // }


   // fn name_index_mut(&self) -> &mut HashMap<(String, i32, i32), Vec<(i32, i32, i32)>>
   // {
   //     while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
   //     unsafe { self.name_index_.get().as_mut().unwrap() }
   // }

    pub fn push_raw(&self, entry: Customer) 
    {
        /* Indexes Updates */
        let (w_id, d_id, c_id) = entry.primary_key();
        let index_val = (w_id, d_id, c_id, entry.c_first);
        let index_key = (
            String::from(str::from_utf8(&entry.c_last).unwrap().trim_right_matches(char::from(0))),
            entry.c_w_id,
            entry.c_d_id,
            );
        self.name_index_.insert_index(index_key.clone(), index_val);

        //println!("PUSHING CUSTOMER {}, {}, {}", entry.c_id, entry.c_w_id, entry.c_d_id);
        self.table_.push_raw(entry);
        self.name_index_.unlock_bucket(&index_key);
    }

    //FIXME: deleting an entry needs to be fixed 
    pub fn update_sec_index(&self, arc: &Arc<Row<Customer, (i32, i32, i32)>>) {
        let c  = arc.get_data();

        let idx_key = (
            String::from(str::from_utf8(&c.c_last).unwrap().trim_right_matches(char::from(0))),
            c.c_w_id,
            c.c_d_id
            );


        let (w_id, d_id, c_id) = c.primary_key();
        let idx_val = (w_id, d_id, c_id, c.c_first);

        self.name_index_.insert_index(idx_key.clone(), idx_val);
        self.name_index_.unlock_bucket(&idx_key);
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
        -> Option<Arc<Row<Customer, (i32, i32, i32)>>>
        {
            match self.name_index_.find_one_bucket_mut(index) {
                None => {
                    self.name_index_.unlock_bucket(index);
                    None
                },
                Some(mut tuples) => {
                    assert_eq!(tuples.len() > 0 , true);
                    tuples.sort_unstable_by(|a, b| a.3.cmp(&b.3));
                    
                    let i = tuples.len()/2;
                    let (w_id, d_id, c_id, _) = tuples[i];
                    let ret = self.table_.retrieve(&(w_id, d_id, c_id));
                    self.name_index_.unlock_bucket(index);
                    ret
                }
            }
    }
}

unsafe impl Sync for CustomerTable {}
unsafe impl Send for CustomerTable {}

//pub type NewOrderTable = Table<NewOrder, (i32, i32, i32)>;
//pub type OrderTable = Table<Order, (i32, i32, i32)>;

#[derive(Debug)]
pub struct NewOrderTable {
    table_ : Table<NewOrder, (i32, i32, i32)>,
    //w_id, d_id
    wd_index_ : SecIndex<(i32, i32), (i32, i32, i32)>,
    
}


impl NewOrderTable {

    pub fn new_with_buckets(num: usize, bkt_size : usize, name: &str) -> NewOrderTable 
    {
        let total_wd = NUM_WAREHOUSES * NUM_INIT_DISTRICT;
        NewOrderTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            wd_index_ : SecIndex::new_with_buckets(
                total_wd as usize, 
                Box::new(move |key| { 
                   ((key.0 * NUM_WAREHOUSES + key.1) % total_wd) as usize
                }))
        }
     }

    pub fn push(&self, tx: &mut TransactionOCC, entry: NewOrder, tables: &Arc<Tables>)
    where Arc<Row<NewOrder, (i32, i32, i32)>>: TableRef 
    {
        self.table_.push(tx, entry, tables);
    }


   // fn wd_index(&self) -> &HashMap<(i32, i32), Vec<(i32, i32,i32)>>
   // {
   //     while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
   //     unsafe { self.wd_index_.get().as_ref().unwrap() }
   // }

   // fn wd_index_mut(&self) -> &mut HashMap<(i32, i32), Vec<(i32, i32, i32)>> 
   // {
   //     while self.lock_.compare_and_swap(false, true, Ordering::SeqCst) {}
   //     unsafe {self.wd_index_.get().as_mut().unwrap() }
   // }

    pub fn push_raw(&self, entry: NewOrder)
    {
        let p_key = entry.primary_key();
        let idx_key = (entry.no_w_id, entry.no_d_id);

        self.wd_index_.insert_index(idx_key.clone(),p_key);
        
        self.table_.push_raw(entry);
        self.wd_index_.unlock_bucket(&idx_key);
    }

    pub fn update_wd_index(&self, arc: &Arc<Row<NewOrder, (i32, i32, i32)>>)
    {
        let no = arc.get_data();
        let idx_key = (no.no_w_id, no.no_d_id);
        
        self.wd_index_.insert_index(idx_key.clone(), no.primary_key());
        self.wd_index_.unlock_bucket(&idx_key);
    }

    pub fn retrieve(&self, index: &(i32, i32, i32)) 
        -> Option<Arc<Row<NewOrder, (i32, i32,i32)>>>
        {
            self.table_.retrieve(index)
        }


    pub fn get_bucket(&self, bkt_idx: usize) -> &Bucket<NewOrder, (i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }


    pub fn retrieve_min_oid(&self, index: &(i32, i32)) 
        -> Option<Arc<Row<NewOrder, (i32,i32,i32)>>>
        {
            match self.wd_index_.find_one_bucket(index) {
                None => {
                    self.wd_index_.unlock_bucket(index);
                    None
                },

                Some(vecs) => {
                    let min_no = vecs.iter()
                        .min_by(|(_xw, _xd, xo), (_yw, _yd, yo) | xo.cmp(yo))
                        .expect("wd_index should not be empty");
                        
                    let ret =self.table_.retrieve(min_no);
                    self.wd_index_.unlock_bucket(index);
                    ret
                }
            }
        }
    
    pub fn delete(&self, tx: &mut TransactionOCC, index: &(i32, i32, i32), tables: &Arc<Tables>)
    {
        self.table_.delete(tx, index, tables);
    }


   // pub fn delete_raw(index: &(i32, i32, i32))
   // {
   //     self.delete_index(*index);
   //     self.delete_entry(*index);
   // }

   // fn delete_entry(index : (i32, i32, i32)) 
   // {
   //     self.table_.delete_raw(&index); 
   // }

    //Holding on bucket lock
    pub fn delete_index(&self, arc : &Arc<Row<NewOrder, (i32, i32,i32)>>)
    {
        let no = arc.get_data();
        let index  = no.primary_key();
        let (w_id, d_id, o_id) = index;
        //update index
        match self.wd_index_.find_one_bucket_mut(&(w_id, d_id)) {
            None => {
                panic!("NewOrderTable::delete_index : missing index");
            },
            Some(mut v) => {
                let idx = v.iter()
                    .position(|&x| x.2==o_id) //w_id, d_id, o_id
                    .expect("delete:no_id should not be empty");

                let removed = v.swap_remove(idx);
                assert_eq!(removed.2 == o_id, true);
                self.wd_index_.unlock_bucket(&(w_id, d_id));
            }
        }
    }
}

unsafe impl Sync for NewOrderTable {}
unsafe impl Send for NewOrderTable {}


#[derive(Debug)]
pub struct OrderLineTable {
    table_ : Table<OrderLine, (i32, i32, i32, i32)>,
    //w_id, d_id, o_id
    order_index_ : SecIndex<(i32, i32, i32), (i32, i32, i32, i32)>,
}

unsafe impl Sync for OrderLineTable {}
unsafe impl Send for OrderLineTable {}

impl OrderLineTable {
    
    pub fn new_with_buckets(num : usize, bkt_size: usize, name: &str) -> OrderLineTable 
    {
        let total_wd = NUM_WAREHOUSES * NUM_INIT_DISTRICT;

        OrderLineTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            order_index_ : SecIndex::new_with_buckets(
                total_wd as usize,
                Box::new(move |key| {
                    let (w_id, d_id, _o_id) = key;
                    ((w_id * NUM_WAREHOUSES + d_id) % total_wd) as usize
                })),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: OrderLine, tables: &Arc<Tables>)
        where Arc<Row<OrderLine, (i32, i32, i32, i32)>> : TableRef
        {
            self.table_.push(tx, entry, tables);
        }


    pub fn push_raw(&self, ol : OrderLine)
    {
        let idx_key = (ol.ol_w_id, ol.ol_d_id, ol.ol_o_id);
        self.order_index_.insert_index(idx_key.clone(), ol.primary_key());

        self.table_.push_raw(ol);
        self.order_index_.unlock_bucket(&idx_key);
    }


    pub fn update_order_index(&self, arc: &Arc<Row<OrderLine, (i32, i32, i32, i32)>>) 
    {
        let ol = arc.get_data();
        let idx_key = (ol.ol_w_id, ol.ol_d_id, ol.ol_o_id);
        self.order_index_.insert_index(idx_key.clone(), ol.primary_key());
        self.order_index_.unlock_bucket(&idx_key);
    }

    pub fn retrieve(&self, index: &(i32, i32, i32, i32)) -> Option<Arc<Row<OrderLine, (i32, i32, i32, i32)>>>
    {
       self.table_.retrieve(index)
    }

    pub fn get_bucket(&self, bkt_idx : usize) -> &Bucket<OrderLine, (i32, i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }
    
    //FIXME: return slice instead
    pub fn find_by_oid(&self, key: &(i32,i32,i32)) ->Vec<Arc<Row<OrderLine, (i32,i32, i32, i32)>>> 
    {
        match self.order_index_.find_one_bucket(key) {
            None => {
                self.order_index_.unlock_bucket(key);
                Vec::new()
            },
            Some(ids) => {
                let ret = ids.iter()
                    .filter_map(|id| self.retrieve(id))
                    .collect::<Vec<_>>();

                self.order_index_.unlock_bucket(key);
                ret
            }
        }
    }

    pub fn find_range(&self, w_id: i32, d_id: i32, o_id_low: i32, o_id_high: i32)
        -> Vec<Arc<Row<OrderLine, (i32, i32, i32, i32)>>>
        {
            let mut ids = Vec::new();
            for o_id in o_id_low..=o_id_high { 
                let key = (w_id, d_id, o_id);
                match self.order_index_.find_one_bucket(&key) {
                    None=> {}, 
                    Some(v) => {
                        ids.append(&mut v.clone());

                    }
                }
                self.order_index_.unlock_bucket(&key);
            }

            let arcs = ids.iter()
                .filter_map(|id| self.table_.retrieve(id))
                .collect::<Vec<_>>();

            assert_eq!(arcs.len(), ids.len());
            arcs
        }

}


#[derive(Debug)]
pub struct OrderTable {
    table_ : Table<Order, (i32, i32, i32)>,
    cus_index_ : SecIndex<(i32, i32, i32), (i32, i32,i32)>,
}


unsafe impl Sync for OrderTable {}
unsafe impl Send for OrderTable {}

impl OrderTable {
   // pub fn new() -> OrderTable {
   //     OrderTable {
   //         table_ : Table::new(),
   //         cus_index_ : SecIndex::new(),
   //     }
   // }

    pub fn new_with_buckets(num : usize, bkt_size : usize, name: &str) -> OrderTable {
        let total_wd = (NUM_WAREHOUSES * NUM_INIT_DISTRICT) as usize;
        OrderTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            cus_index_ : SecIndex::new_with_buckets(
                total_wd,
                Box::new(move |key| {
                    let (w_id, d_id, _o_id) = key;
                    (w_id * NUM_WAREHOUSES + d_id) as usize % total_wd
                })),
        }
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Order, tables: &Arc<Tables>) 
        where Arc<Row<Order, (i32, i32, i32)>> : TableRef
    {
        self.table_.push(tx, entry, tables) 
    }

    pub fn retrieve(&self, index: &(i32, i32, i32)) -> Option<Arc<Row<Order, (i32, i32, i32)>>>
    {
        self.table_.retrieve(index)
    }

    pub fn get_bucket(&self, bkt_idx: usize) -> &Bucket<Order, (i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }

    pub fn push_raw(&self, entry: Order) 
    {
        /* Index Updates */
        let idx_val = entry.primary_key();
        let idx_key = (entry.o_w_id, entry.o_d_id, entry.o_c_id);

        self.cus_index_.insert_index(idx_key.clone(), idx_val);

        self.table_.push_raw(entry);
        self.cus_index_.unlock_bucket(&idx_key);
    }

    pub fn update_cus_index(&self, arc: &Arc<Row<Order, (i32,i32, i32)>>) 
    {
        let o = arc.get_data();
        let idx_key = (o.o_w_id, o.o_d_id, o.o_c_id);

        self.cus_index_.insert_index(idx_key.clone(), o.primary_key());
        self.cus_index_.unlock_bucket(&idx_key);
    }

    //TODO: update index?
    pub fn retrieve_by_cid(&self, key: &(i32, i32, i32))
        -> Option<Arc<Row<Order, (i32, i32, i32)>>> 
        {
            match self.cus_index_.find_one_bucket(key) {
                None => {
                    self.cus_index_.unlock_bucket(key);
                    None
                },
                Some(ids)=> {
                    let max_pos = ids.iter()
                        .max_by(|a, b| a.2.cmp(&b.2))
                        .expect("retrieve_by_cid: empty ids");

                    let ret = self.table_.retrieve(max_pos);
                    self.cus_index_.unlock_bucket(key);
                    ret
                }

            }
        }


}

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
    {
        SecIndex {
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

    pub fn find_one_bucket(&self, key: &K) ->  Option<&Vec<V>> 
    {
        self.buckets_[(self.get_bucket_)(key)].find_many(key)
    }

    pub fn find_one_bucket_mut(&self, key: &K) ->  Option<&mut Vec<V>> 
    {
        self.buckets_[(self.get_bucket_)(key)].find_many_mut(key)
    }

   // pub fn find_many(&self, bucket_idx: usize, ranges: &[K]) -> Option<&Vec<V>>
   // {
   //     let mut ret = Vec::with_capacity(32);
   //     for (key, bucket) in keys.zip(buckets) {
   //         match self.buckets_[bucket].find_many(key) {
   //             None => {},
   //             Some(vecs) => {
   //                 ret.append(&mut vecs.clone());
   //             }
   //         }
   //     }

   //     if ret.len() == 0 {
   //         None
   //     } else {
   //         Some(ret)
   //     }
   // }
}



impl<K, V> Debug for SecIndex<K, V>
where K: Hash + Eq + Debug,
      V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SecIndex not formatted",)
    }
}


struct SecIndexBucket<K, V>
where K: Hash+ Eq+ Debug,
      V: Debug
{
    index_ : UnsafeCell<HashMap<K, Vec<V>>>,
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

    pub fn index(&self) -> &HashMap<K, Vec<V>> {
        self.lock(); /* Spin locks */
        unsafe { self.index_.get().as_ref().unwrap() }
    }

    pub fn index_mut(&self) -> &mut HashMap<K, Vec<V>> {
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
            .or_insert_with(|| Vec::new());

        ids.push(val);
        
        /* Delay unlock until the data is pushed */
    }
    
    /* FIXME: Allocating new arrays? */
    fn find_many(&self, key: &K) -> Option<&Vec<V>> {
        self.index().get(key)
    }

    fn find_many_mut(&self, key: &K) -> Option<&mut Vec<V>> {
        self.index_mut().get_mut(key)
    }
}



//pub type  OrderLineTable = Table<OrderLine, (i32, i32, i32, i32)>;
pub type  ItemTable = Table<Item, i32>;
pub type  StockTable = Table<Stock, (i32, i32)>;

//FIXME: 
//pub type HistoryTable = NonIndexTable<History>;
pub type HistoryTable = Table<History, (i32)>; /* No primary key in fact */

#[derive(Debug)]
pub struct Tables {
   pub stock: StockTable,
   pub orderline: OrderLineTable,
   pub customer: CustomerTable,
   pub warehouse: WarehouseTable,
   pub district: DistrictTable,
   pub neworder: NewOrderTable,
   pub order: OrderTable,
   pub item: ItemTable,
   pub history: HistoryTable,
}

pub type TablesRef = Arc<Tables>;


pub trait TableRef
{
    fn into_table_ref(self, Option<usize>,Option<Arc<Tables>>) -> Box<dyn TRef>;
}

pub trait BucketDeleteRef {
    fn into_delete_table_ref(self, usize, Arc<Tables>) -> Box<dyn TRef>;
}


pub trait Key<T> {
    fn primary_key(&self) -> T;
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

    pub fn delete(&self, tx: &mut TransactionOCC, index: &Index, tables: &Arc<Tables>)
        where Arc<Row<Entry, Index>> : BucketDeleteRef
    {
        let bucket_idx = self.get_bucket_idx(index);
        
        let row = self.buckets[bucket_idx].retrieve(index).expect("delete: no element").clone();
        let table_ref = row.into_delete_table_ref(
            bucket_idx,
            tables.clone(),
            );
        let mut tag = tx.retrieve_tag(table_ref.get_id(), table_ref.box_clone());
        tag.set_write(); //FIXME: better way?
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

    fn get_bucket_idx(&self, key: &Index) -> usize 
    {
        self.make_hash(key) % self.bucket_num
    }

    pub fn get_bucket(&self, bkt_idx : usize) -> &Bucket<Entry, Index>{
        &self.buckets[bkt_idx]
    }
}

impl<Entry, Index> Default for Table<Entry, Index> 
where Entry: 'static + Key<Index> + Clone +Debug,
      Index: Eq+Hash  + Clone + Debug,
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

/* FIXME: can we avoid the copy */
#[derive(Debug)]
pub struct Bucket<Entry, Index> 
where Entry: 'static + Key<Index> + Clone+Debug,
      Index: Eq+Hash + Clone,
{
    rows: RwLock<Vec<Arc<Row<Entry, Index>>>>,
    index: RwLock<HashMap<Index, usize>>,
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
            index: RwLock::new(HashMap::new()),

            id_ : OidFac::get_obj_next(),
            vers_ : TVersion::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Bucket<Entry, Index> 
    {
        Bucket {
            rows: RwLock::new(Vec::with_capacity(cap)),
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

    pub fn delete(&self, row_arc: Arc<Row<Entry, Index>>) {
        let idx_elem = row_arc.get_data().primary_key();

        /* FIXME: Leave the data in the rows */
        let mut idx_map = self.index.write().unwrap();
        idx_map.remove(&idx_elem);
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



#[derive(Clone, Debug)]
pub enum Operation {
    RWrite,
    Delete,
    Push,
}
