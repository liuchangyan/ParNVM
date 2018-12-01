#[allow(unused_imports)]
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
use super::table::*;
use super::entry::*;
use super::workload_common::*;
use pnvm_lib::tcore::{TVersion, ObjectId, OidFac, TRef};
use pnvm_lib::txn::{Tid,TxnInfo, Transaction};
use pnvm_lib::occ::occ_txn::TransactionOCC;
use pnvm_lib::lock::lock_txn::Transaction2PL;
use pnvm_lib::parnvm::nvm_txn_occ::TransactionParOCC;

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

pub type WarehouseTable = Table<Warehouse, i32>;
pub type DistrictTable = Table<District, (i32, i32)>;

#[derive(Debug)]
pub struct CustomerTable {
    pub name_index_ : SecIndex<(String, i32, i32), (i32, i32,i32, [u8;16])>,
    table_ : Table<Customer, (i32, i32, i32)>,
}


impl CustomerTable {

    pub fn new_with_buckets(num : usize , bkt_size : usize, name: &str) -> CustomerTable {
        let num_whs = num_warehouse_get();
        let num_dis = num_district_get();
        let total_wd = num_whs * num_dis;
        CustomerTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            name_index_ : SecIndex::new_with_buckets(total_wd as usize, Box::new(move |key| {
                let (_ , w_id, d_id) = key;          
                ((w_id * num_dis + d_id)% total_wd) as usize
            })),
        }
    }


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
        let dis_num = num_district_get();
        let bucket_idx = index.0 * dis_num + index.1;
        self.table_.retrieve(index, bucket_idx as usize)
    }

    pub fn get_bucket(&self, bkt_idx : usize ) -> &Bucket<Customer, (i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }

    pub fn find_by_name_id(&self, index : &(String, i32, i32))
        -> Option<Arc<Row<Customer, (i32, i32, i32)>>>
        {
            let dis_num = num_district_get();
            match self.name_index_.find_one_bucket_mut(index) {
                None => {
                    self.name_index_.unlock_bucket(index);
                    None
                },
                Some(tuples) => {
                    assert_eq!(tuples.len() > 0 , true);
                    let (front, back) = tuples.as_slices();
                    let mut tuples = [front, back].concat();
                    tuples.sort_unstable_by(|a, b| a.3.cmp(&b.3));
                    
                    let i = tuples.len()/2;
                    let (w_id, d_id, c_id, _) = tuples[i];
                    let ret = self.table_.retrieve(&(w_id, d_id, c_id), (dis_num * w_id + d_id) as usize);
                    self.name_index_.unlock_bucket(index);
                    ret
                }
            }
    }
}

unsafe impl Sync for CustomerTable {}
unsafe impl Send for CustomerTable {}


#[derive(Debug)]
pub struct NewOrderTable {
    table_ : Table<NewOrder, (i32, i32, i32)>,
    //w_id, d_id
    wd_index_ : SecIndex<(i32, i32), (i32, i32, i32)>,
    
}


impl NewOrderTable {

    pub fn new_with_buckets(num: usize, bkt_size : usize, name: &str) -> NewOrderTable 
    {
        let num_whs = num_warehouse_get();
        let num_dis = num_district_get();
        let total_wd = num_whs * num_dis;
        NewOrderTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            wd_index_ : SecIndex::new_with_buckets(
                total_wd as usize, 
                Box::new(move |key| { 
                   ((key.0 * num_dis + key.1) % total_wd) as usize
                }))
        }
     }
    
    pub fn push_pc(&self, tx: &mut TransactionParOCC, entry: NewOrder, tables: &Arc<Tables>)
    where Arc<Row<NewOrder, (i32, i32, i32)>>: TableRef 
    {
        self.table_.push_pc(tx, entry, tables);
    }



    pub fn push(&self, tx: &mut TransactionOCC, entry: NewOrder, tables: &Arc<Tables>)
    where Arc<Row<NewOrder, (i32, i32, i32)>>: TableRef 
    {
        self.table_.push(tx, entry, tables);
    }

    pub fn push_lock(&self, tx: &mut Transaction2PL, entry: NewOrder, tables: &Arc<Tables>)
    {
        self.table_.push_lock(tx, entry, tables)
    }


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
            let dis_num = num_district_get();
            let bucket_idx = index.0 * dis_num + index.1;
            self.table_.retrieve(index, bucket_idx as usize)
        }

    pub fn retrieve_bucket(&self, bucket: &(i32, i32)) -> &Bucket<NewOrder, (i32, i32, i32)> {
        let dis_num = num_district_get();
        let bucket_idx = bucket.0 * dis_num + bucket.1;
        self.get_bucket(bucket_idx as usize)
    }

    pub fn get_bucket(&self, bkt_idx: usize) -> &Bucket<NewOrder, (i32, i32, i32)>
    {
        self.table_.get_bucket(bkt_idx)
    }


    pub fn retrieve_min_oid(&self, index: &(i32, i32)) 
        -> Option<Arc<Row<NewOrder, (i32,i32,i32)>>>
        {
            let dis_num = num_district_get();
            match self.wd_index_.find_one_bucket(index) {
                None => {
                    self.wd_index_.unlock_bucket(index);
                    None
                },

                Some(vecs) => {
                    //assert_eq!(vecs.len()> 0, true);
                    if vecs.len() == 0 {
                        self.wd_index_.unlock_bucket(index);
                        return None;
                    }
                    let min_no = vecs[0];    
                    let ret =self.table_.retrieve(&min_no, (min_no.0 * dis_num + min_no.1) as usize);
                    self.wd_index_.unlock_bucket(index);
                    ret
                }
            }
        }
    
    pub fn delete(&self, tx: &mut TransactionOCC, index: &(i32, i32, i32), tables: &Arc<Tables>) -> bool
    {
        let dis_num = num_district_get();
        let bucket_idx = index.0 * dis_num  + index.1;
        self.table_.delete(tx, index, tables, bucket_idx as usize)
    }

    //pub fn delete_lock(&self, tx: &mut Transaction2PL, index: &(i32, i32, i32), tables: &Arc<Tables>) -> bool
    //{
    //    let dis_num = num_district_get();
    //    let bucket_idx = index.0 * dis_num  + index.1;
    //    self.table_.delete_lock(tx, index, tables, bucket_idx as usize)
    //}

    pub fn delete_pc(&self, tx: &mut TransactionParOCC, index: &(i32, i32, i32), tables: &Arc<Tables>) -> bool
    {
        let dis_num = num_district_get();
        let bucket_idx = index.0 * dis_num  + index.1;
        self.table_.delete_pc(tx, index, tables, bucket_idx as usize)
    }


    //Holding on bucket lock
    pub fn delete_index(&self, arc : &Arc<Row<NewOrder, (i32, i32,i32)>>)
        -> bool
    {
        let no = arc.get_data();
        let index  = no.primary_key();
        let (w_id, d_id, o_id) = index;
        //update index
        match self.wd_index_.find_one_bucket_mut(&(w_id, d_id)) {
            None => {
                panic!("NewOrderTable::delete_index : missing index");
            },
            Some(v) => {
                match v.iter().position(|&x| x.2 == o_id) {
                    None => {
                        warn!("delete_index:: no id {:?}", (w_id, d_id, o_id));
                        self.wd_index_.unlock_bucket(&(w_id, d_id));
                        false
                    },
                    Some(idx) => {
                        let removed = v.remove(idx).unwrap();
                        assert_eq!(removed.2 == o_id, true);
                        self.wd_index_.unlock_bucket(&(w_id, d_id));
                        true
                    }
                }
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
        let num_whs = num_warehouse_get();
        let num_dis = num_district_get();
        let total_wd = num_whs * num_dis;

        OrderLineTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            order_index_ : SecIndex::new_with_buckets(
                total_wd as usize,
                Box::new(move |key| {
                    let (w_id, d_id, _o_id) = key;
                    ((w_id * num_dis + d_id) % total_wd) as usize
                })),
        }
    }


    pub fn push_pc(&self, tx: &mut TransactionParOCC, entry: OrderLine, tables: &Arc<Tables>)
        where Arc<Row<OrderLine, (i32, i32, i32, i32)>> : TableRef
        {
            self.table_.push_pc(tx, entry, tables);
        }

    pub fn push_lock(&self, tx: &mut Transaction2PL, entry: OrderLine, tables: &Arc<Tables>)
    {
        self.table_.push_lock(tx, entry, tables)
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
       // warn!("[-][ORDERLINE-INDEX] Updating Orderline index: {}, {}, {} => {:?}",
       //       ol.ol_w_id, ol.ol_d_id, ol.ol_o_id, ol.primary_key());
        self.order_index_.insert_index(idx_key.clone(), ol.primary_key());
        self.order_index_.unlock_bucket(&idx_key);
    }

    pub fn retrieve(&self, index: &(i32, i32, i32, i32)) -> Option<Arc<Row<OrderLine, (i32, i32, i32, i32)>>>
    {
        let dis_num = num_district_get();
        let bucket_idx = index.0 * dis_num + index.1;
       self.table_.retrieve(index, bucket_idx as usize)
    }

    pub fn retrieve_bucket(&self, bucket: &(i32, i32)) -> &Bucket<OrderLine, (i32, i32, i32, i32)> {
        let dis_num = num_district_get();
        let bucket_idx = bucket.0 * dis_num + bucket.1;
        self.get_bucket(bucket_idx as usize)
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
                        ids.append(&mut v.clone().into());
                    }
                }
                self.order_index_.unlock_bucket(&key);
            }

            let arcs = ids.iter()
                .filter_map(|id| self.retrieve(id))
               // .map(|id| {
               //     match self.retrieve(id) {
               //         None => {
               //            // println!("{:#?}", self.table_.get_bucket((id.0 * wh_num + id.1) as usize));
               //            // println!("{:#?}", self.order_index_);
               //             panic!("find_range: not found {:?}", id);
               //         },
               //         Some(arc) => arc
               //     }
               // })
                .collect::<Vec<_>>();

            //assert_eq!(arcs.len(), ids.len());
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
        let num_whs = num_warehouse_get();
        let num_dis = num_district_get();
        let total_wd = (num_whs * num_dis) as usize;
        OrderTable {
            table_ : Table::new_with_buckets(num, bkt_size, name),
            cus_index_ : SecIndex::new_with_buckets(
                total_wd,
                Box::new(move |key| {
                    let (w_id, d_id, _o_id) = key;
                    (w_id * num_dis + d_id) as usize % total_wd
                })),
        }
    }
    
    pub fn push_pc(&self, tx: &mut TransactionParOCC, entry: Order, tables: &Arc<Tables>) 
        where Arc<Row<Order, (i32, i32, i32)>> : TableRef
    {
        self.table_.push_pc(tx, entry, tables) 
    }

    pub fn push_lock(&self, tx: &mut Transaction2PL, entry: Order, tables: &Arc<Tables>)
    {
        self.table_.push_lock(tx, entry, tables)
    }

    pub fn push(&self, tx: &mut TransactionOCC, entry: Order, tables: &Arc<Tables>) 
        where Arc<Row<Order, (i32, i32, i32)>> : TableRef
    {
        self.table_.push(tx, entry, tables) 
    }

    pub fn retrieve(&self, index: &(i32, i32, i32)) -> Option<Arc<Row<Order, (i32, i32, i32)>>>
    {
        let dis_num = num_district_get();
        let bucket_idx = index.0 * dis_num + index.1;
        self.table_.retrieve(index, bucket_idx as usize)
    }

    pub fn retrieve_bucket(&self, bucket: &(i32, i32)) -> &Bucket<Order, (i32, i32, i32)> {
        let dis_num = num_district_get();
        let bucket_idx = bucket.0 * dis_num + bucket.1;
        self.get_bucket(bucket_idx as usize)
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
            //let wh_num = num_warehouse_get();
            match self.cus_index_.find_one_bucket(key) {
                None => {
                    self.cus_index_.unlock_bucket(key);
                    None
                },
                Some(ids)=> {
                    let max_pos = ids.iter()
                        .max_by(|a, b| a.2.cmp(&b.2))
                        .expect("retrieve_by_cid: empty ids");

                    let ret = self.retrieve(max_pos);
                    if ret.is_none() {
                        warn!("retrieve_by_cid: none {:?}", max_pos);
                    }
                    self.cus_index_.unlock_bucket(key);
                    ret
                }

            }
        }


}
