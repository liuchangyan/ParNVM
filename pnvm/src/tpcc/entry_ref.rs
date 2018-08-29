
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



pub struct WarehouseRef {
    inner_ : Arc<Row<Warehouse>>,
}

pub struct DistrictRef {
    inner_ : Arc<Row<District>>,
}

pub struct CustomerRef {
    inner_ : Arc<Row<Customer>>,
}

pub struct NewOrderRef {
    inner_ : Arc<Row<NewOrder>>,
}

pub struct OrderRef {
    inner_ : Arc<Row<Order>>,
}

pub struct OrderLineRef {
    inner_ : Arc<Row<OrderLine>>,
}


pub struct ItemRef {
    inner_ : Arc<Row<Item>>,
}

pub struct StockRef {
    inner_ : Arc<Row<Stock>>,
}


impl TRef for WarehouseRef {
    fn install(&self, val: &Box<Any>, id: Tid) {
        match (**val).downcast_ref::<Warehouse>() {
            Some(as_u32) => {
                self.inner_.install(as_u32, id)
            },
            None => {
                panic!("failed to convert to warehouse")
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

