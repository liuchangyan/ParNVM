
/* FIXME: this should not be needed with GAT implemented */

use super::entry::*;
use super::table::*;
use pnvm_lib:: {
    tcore::*,
    tbox::*,
    txn::*,
};
use std::{
    any::Any,
    sync::Arc,
};

#[cfg(not(feature = "pmem"))]
use core::alloc::Layout;

#[cfg(feature = "pmem")]
use pnvm_sys::{
    Layout
};



pub struct WarehouseRef<'a> {
    inner_ : Arc<Row<Warehouse>>,
    bucket_idx_: usize,
    table_ref_: Option<&'a WarehouseTable>,
    txn_info_ : Arc<TxnInfo>,
    entry_ : Warehouse,
}

pub struct DistrictRef {
    inner_ : Arc<Row<District>>,
    bucket_idx_: usize,
}

pub struct CustomerRef {
    inner_ : Arc<Row<Customer>>,
    bucket_idx_: usize,
    on_table_: bool
}

pub struct NewOrderRef {
    inner_ : Arc<Row<NewOrder>>,
    bucket_idx_: usize,
    on_table_: bool
}

pub struct OrderRef {
    inner_ : Arc<Row<Order>>,
    bucket_idx_: usize,
    on_table_: bool
}

pub struct OrderLineRef {
    inner_ : Arc<Row<OrderLine>>,
    bucket_idx_: usize,
    on_table_: bool
}


pub struct ItemRef {
    inner_ : Arc<Row<Item>>,
    bucket_idx_: usize,
    on_table_: bool
}

pub struct StockRef {
    inner_ : Arc<Row<Stock>>,
    bucket_idx_: usize,
    on_table_: bool
}


impl<'a> TRef for WarehouseRef<'a> {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(table) => {
                table.get_bucket(self.bucket_idx_).push(Row::new_from_txn(as_val));
            },
            None => {
                self.inner_.install(as_wh, id);
            }
        }
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(WarehouseRef {
            inner_: self.inner_.clone(),

        })
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}



impl TRef for DistrictRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<District>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to district")
            }
        }
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(DistrictRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}



impl TRef for CustomerRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<Customer>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to customer")
            }
        }
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(CustomerRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}


impl TRef for NewOrderRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<NewOrder>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to new order")
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(NewOrderRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl TRef for OrderRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<Order>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to order")
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(OrderRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl TRef for OrderLineRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<OrderLine>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to order line")
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(OrderLineRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl TRef for ItemRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<Item>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to item")
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(ItemRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl TRef for StockRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<Stock>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to stock")
            }
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(StockRef {
            inner_: self.inner_.clone()
        })
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32) -> bool {
        self.inner_.check(vers)
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}


impl TableRef<Warehouse, i32> for Warehouse {
    type Table = WarehouseTable;

    fn into_table_ref(
        self,
        bucket_idx: usize,
        txn_info : &Arc<TxnInfo>,
        table_ref : &WarehouseTable
    ) 
        -> Box<dyn TRef> 
        {
            Box::new(
                WarehouseRef {
                    inner_ : Arc::new(row),
                    bucket_idx_: bucket_idx,
                    table_ref_ : Some(table_ref),
                    txn_info_ : txn_info.clone(),
                    entry_ : self
                })
        }
}




