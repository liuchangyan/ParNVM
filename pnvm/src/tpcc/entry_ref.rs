//************************************************
//This module has concrete implementation for the 
//TRef trait.
//
//Types: 
//- WarehouseRef
//- DistrictRef
//- XXXXRef
//
//Impl: 
//- BucketDeleteRef
//- TableRef
//- BucketPushRef
//************************************************

/* FIXME: this should not be needed with GAT implemented */
/* FIXME: this should be much shorter with procedures macros, 
 * but copy-paste is easier for now... */


use super::entry::*;
use super::table::*;
use super::tpcc_tables::*;
use pnvm_lib:: {
    tcore::*,
    tbox::*,
    txn::*,
};
use std::{
    any::Any,
    sync::Arc,
    ptr,
};

#[cfg(not(any(feature = "pmem", feature = "disk")))]
use core::alloc::Layout;

#[cfg(any(feature = "pmem", feature = "disk"))]
use pnvm_sys::{
    Layout
};



#[derive(Clone , Debug)]
pub struct WarehouseRef  {
    inner_ : Arc<Row<Warehouse, i32>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Warehouse>>,
    ops_ : Operation,

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut Warehouse,
}


#[derive(Clone , Debug)]
pub struct DistrictRef  {
    inner_ : Arc<Row<District, (i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<District>>,
    ops_ : Operation,

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut District,
}

#[derive(Clone , Debug)]
pub struct CustomerRef  {
    inner_ : Arc<Row<Customer, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Customer>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut Customer,
}

#[derive(Clone , Debug)]
pub struct NewOrderRef  {
    inner_ : Arc<Row<NewOrder, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<NewOrder>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut NewOrder,
}

#[derive(Clone , Debug)]
pub struct OrderRef  {
    inner_ : Arc<Row<Order, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Order>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut Order,
}

#[derive(Clone , Debug)]
pub struct OrderLineRef  {
    inner_ : Arc<Row<OrderLine, (i32, i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<OrderLine>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut OrderLine,
}


#[derive(Clone , Debug)]
pub struct ItemRef  {
    inner_ : Arc<Row<Item, i32>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Item>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut Item,
}

#[derive(Clone , Debug)]
pub struct StockRef  {
    inner_ : Arc<Row<Stock, (i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Stock>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut Stock,
}

#[derive(Clone , Debug)]
pub struct HistoryRef  {
    inner_ : Arc<Row<History, (i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<History>>,
    ops_ : Operation,
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut History,
}

impl  TRef for WarehouseRef {
   // fn install(&self, id: Tid) {
   //     match self.table_ref_ {
   //         Some(ref table) => {
   //         },
   //         None => {
   //         }
   //     }
   // }


    fn install(&self, id: Tid) {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().expect("Warehouse_ref: no table ref for push");
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.warehouse.get_bucket(bucket_idx).set_version(row.get_version());
                table.warehouse.get_bucket(bucket_idx).push(row);
            },
            Operation::RWrite => {
                
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            },
            _ =>  panic!("Unknown Operations")
        }
    }
    

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    //TODO:
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }
    
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut Warehouse;
    }

    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Warehouse>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("warehosuref::write value should be Box<Warehouse>")
        }
    }

   // fn replace_filed(&mut self, vals: &[(usize, &Any)], val_cnt: usize) {
   //     //TODO:
   //     // 1. store the vals
   // }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.warehouse.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.warehouse.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("warehouse")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<Warehouse>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be warehouse")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.warehouse.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.warehouse.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.warehouse.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.warehouse.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }

}



impl  TRef for DistrictRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.district.get_bucket(bucket_idx).set_version(row.get_version());
                table.district.get_bucket(bucket_idx).push(row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }

    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }
    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut District;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<District>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.district.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.district.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()

    }
    fn get_name(&self) -> String {
        String::from("district")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<District>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be Distric")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.district.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.district.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.district.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.district.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}



impl  TRef for CustomerRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.customer.get_bucket(bucket_idx).set_version(row.get_version());
                table.customer.get_bucket(bucket_idx).push(row.clone());
                table.customer.update_sec_index(&row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }
    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut Customer;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Customer>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.customer.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.customer.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }


    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("customer")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<Customer>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be Distric")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.customer.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.customer.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.customer.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.customer.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}


impl  TRef for NewOrderRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                match self.ops_ {
                    Operation::Push => {
                        let row = self.inner_.clone();
                        let bucket_idx = self.bucket_idx_.expect("no bucketidx");
                        table.neworder.get_bucket(bucket_idx).set_version(row.get_version());
                        table.neworder.get_bucket(bucket_idx).push(row.clone());
                        table.neworder.update_wd_index(&row);
                    }, 
                    Operation::Delete => {
                        let row = self.inner_.clone();
                        let bucket_idx = self.bucket_idx_.expect("no bucket indx");
                        table.neworder.get_bucket(bucket_idx).set_version(row.get_version());
                        //FIXME: hack so double delete allowed
                        if table.neworder.delete_index(&row) {
                            table.neworder.get_bucket(bucket_idx).delete(row);
                        }
                    }
                    _ => panic!("NewOrderRef::install: RWrite has table ref")
                }
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().expect("no data"), id);
            }
        }
    }


    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                match self.ops_ {
                    Operation::Delete => {
                        //Lock both the deleted entry and the bucket
                        table.neworder.get_bucket(self.bucket_idx_.unwrap()).lock(tid) && self.inner_.lock(tid)
                    },
                    Operation::Push => {
                        table.neworder.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
                    },
                    _ => panic!("NewOrderRef::lock : RWrite has table ref")
                }
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                match self.ops_ {
                    Operation::Delete => {
                        //Lock both the deleted entry and the bucket
                        self.inner_.unlock();
                        table.neworder.get_bucket(self.bucket_idx_.unwrap()).unlock();
                    },
                    Operation::Push => {
                        table.neworder.get_bucket(self.bucket_idx_.unwrap()).unlock();
                    },
                    _ => panic!("NewOrderRef::unlock : RWrite has table ref")
                }
            }
        }
    }
    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    
    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
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

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }
    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut NewOrder;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<NewOrder>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }


    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }


    fn get_name(&self) -> String {
        String::from("neworder")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<NewOrder>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be NewOrder")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.neworder.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.neworder.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.neworder.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.neworder.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}

impl  TRef for OrderRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.order.get_bucket(bucket_idx).set_version(row.get_version());
                table.order.get_bucket(bucket_idx).push(row.clone());
                table.order.update_cus_index(&row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.order.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.order.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }
    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut Order;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Order>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }


    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("order")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<Order>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be NewOrder")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.order.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.order.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.order.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.order.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}

impl  TRef for OrderLineRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.orderline.get_bucket(bucket_idx).set_version(row.get_version());
                table.orderline.get_bucket(bucket_idx).push(row.clone());
                 info!("[{:?}] [ORDERLINE-INDEX] Inserting for {:?}", id, row);
                table.orderline.update_order_index(&row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.orderline.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.orderline.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true //FIXME: what if deleted
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }
    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }
    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut OrderLine;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<OrderLine>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }
    fn get_name(&self) -> String {
        String::from("orderline")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<OrderLine>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be NewOrder")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.orderline.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.orderline.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.orderline.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.orderline.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}

impl  TRef for ItemRef  {
    fn install(&self, id: Tid) {
        panic!("Item is read only");
    }


    fn lock(&self, tid: Tid) -> bool {
        panic!("Item is read only");
    }

    fn unlock(&self) {
        panic!("Item is read only");
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        true
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }
    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut Item;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        panic!("Item is read only")
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }


    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        //Noop for Item
        //self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("item")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        panic!("not implemented")
    }
    fn read_lock(&self, tid: u32) -> bool {
        true
    }

    fn read_unlock(&self, tid: u32) {
    }

    fn write_lock(&self, tid: u32) -> bool {
        true
    }

    fn write_unlock(&self, tid: u32)  {
        panic!("not implemented")
    }
}

impl  TRef for HistoryRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.history.get_bucket(bucket_idx).set_version(row.get_version());
                table.history.get_bucket(bucket_idx).push(row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.history.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.history.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }
    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }


    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }

    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }
    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut History;
    }
    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<History>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("Stock::write value should be Box<Warehouse>")
        }
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }
    fn get_name(&self) -> String {
        String::from("history")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<History>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be NewOrder")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.history.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.history.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.history.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.history.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}
impl  TRef for StockRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.stock.get_bucket(bucket_idx).set_version(row.get_version());
                table.stock.get_bucket(bucket_idx).push(row);
            },
            None => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner_.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }

                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }
        }
    }


    fn lock(&self, tid: Tid) -> bool {
        match self.table_ref_ {
            None => self.inner_.lock(tid),
            Some(ref table) => {
                table.stock.get_bucket(self.bucket_idx_.unwrap()).lock(tid)
            }
        }
    }

    fn unlock(&self) {
        match self.table_ref_ {
            None => self.inner_.unlock(),
            Some(ref table) => {
                table.stock.get_bucket(self.bucket_idx_.unwrap()).unlock();
            }
        }
    }
    fn check(&self, vers: u32, tid: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers, tid)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_addr(&self) -> *mut u8 {
        self.inner_.get_pmem_addr() as *mut u8
    }
    fn get_ptr(&self) -> *mut u8 {
        self.inner_.get_ptr()
    }

    fn get_layout(&self) -> Layout {
        self.inner_.get_layout()
    }

    #[cfg(any(feature = "pmem", feature = "disk"))]
    fn get_pmem_field_addr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_pmem_field_addr(field_idx) as *mut u8
    }

    //TODO:
    fn get_field_ptr(&self, field_idx: usize) -> *mut u8 {
        self.inner_.get_field_ptr(field_idx)
    }

    fn get_field_size(&self, field_idx: usize) -> usize {
        self.inner_.get_field_size(field_idx)
    }

    fn get_id(&self) -> &ObjectId {
        self.inner_.get_id()
    }
    fn get_tvers(&self) -> &Arc<TVersion> {
        &self.inner_.vers_
    }

    fn get_version(&self) -> u32 {
        self.inner_.get_version()
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }

    #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Stock>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("Stock::write value should be Box<Warehouse>")
        }
    }
    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    fn write(&mut self, ptr: *mut u8) {
        self.pd_ptr = ptr as *mut Stock;
    }


    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }
    fn get_name(&self) -> String {
        String::from("stock")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<Stock>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be NewOrder")
        }
    }
    fn read_lock(&self, tid: u32) -> bool {
        self.inner_.vers_.read_lock(tid)
    }

    fn read_unlock(&self, tid: u32) {
        self.inner_.vers_.read_unlock(tid)
    }

    fn write_lock(&self, tid: u32) -> bool {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.stock.get_bucket(bkt_idx).vers_.write_lock(tid)
            },
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.stock.get_bucket(bkt_idx).vers_.write_lock(tid)
                    && self.inner_.vers_.write_lock(tid)
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::Push => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                table.stock.get_bucket(bkt_idx).vers_.write_unlock(tid);
            },
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            Operation::Delete => {
                let table = self.table_ref_.as_ref().unwrap();
                let bkt_idx = self.bucket_idx_.unwrap();
                self.inner_.vers_.write_unlock(tid);
                table.stock.get_bucket(bkt_idx).vers_.write_unlock(tid);
            }
        }
    }
}



impl TableRef for Arc<Row<Warehouse, i32>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    ) 
        -> Box<dyn TRef> 
        {
            Box::new(
                WarehouseRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,

                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl TableRef for Arc<Row<Customer, (i32, i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                CustomerRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}



impl TableRef for Arc<Row<District, (i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                DistrictRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}


impl TableRef for Arc<Row<NewOrder, (i32, i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                NewOrderRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None, 
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}


impl TableRef for Arc<Row<Order, (i32, i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                OrderRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl TableRef for Arc<Row<OrderLine, (i32, i32, i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                OrderLineRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}


impl TableRef for Arc<Row<Item, i32>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                ItemRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl TableRef for Arc<Row<Stock, (i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                StockRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None ,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl TableRef for Arc<Row<History,(i32, i32)>> {
    fn into_table_ref(
        self,
        bucket_idx: Option<usize>,
        //txn_info : Option<Arc<TxnInfo>>,
        table_ref : Option<Arc<Tables>>
    )
        -> Box<dyn TRef> 
        {
            Box::new(
                HistoryRef {
                    inner_ : self,
                    bucket_idx_: bucket_idx,
                    table_ref_: table_ref,
                    //txn_info_ : txn_info,
                    data_ : None,
                    ops_ : Operation::RWrite,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl BucketDeleteRef for Arc<Row<NewOrder, (i32, i32, i32)>> {
    fn into_delete_table_ref(
        self,
        bucket_idx: usize,
        table_ref : Arc<Tables>
    )
        -> Box<dyn TRef>
        {
            Box::new(
                NewOrderRef {
                    inner_ : self,
                    bucket_idx_: Some(bucket_idx),
                    table_ref_: Some(table_ref),
                    data_ : None,
                    ops_ : Operation::Delete,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }

}

impl BucketPushRef for Arc<Row<Order, (i32, i32, i32)>> {
    fn into_push_table_ref(
        self,
        bucket_idx: usize,
        table_ref : Arc<Tables>
    ) 
        -> Box<dyn TRef>
        {
            Box::new(
                OrderRef {
                    inner_ : self,
                    bucket_idx_: Some(bucket_idx),
                    table_ref_: Some(table_ref),
                    data_ : None,
                    ops_: Operation::Push,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}


impl BucketPushRef for Arc<Row<NewOrder, (i32, i32, i32)>>
{
    fn into_push_table_ref(
        self,
        bucket_idx: usize,
        table_ref: Arc<Tables>
    )
        -> Box<dyn TRef>
        {
            Box::new(
                NewOrderRef {
                    inner_ : self,
                    bucket_idx_: Some(bucket_idx),
                    table_ref_: Some(table_ref),
                    data_ : None,
                    ops_ : Operation::Push,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}


impl BucketPushRef for Arc<Row<OrderLine, (i32, i32, i32, i32)>>
{
    fn into_push_table_ref(
        self,
        bucket_idx: usize,
        table_ref: Arc<Tables>
    )
        -> Box<dyn TRef>
        {
            Box::new(
                OrderLineRef {
                    inner_ : self,
                    bucket_idx_: Some(bucket_idx),
                    table_ref_: Some(table_ref),
                    data_ : None,
                    ops_ : Operation::Push,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}

impl BucketPushRef for Arc<Row<History, (i32, i32)>>
{
    fn into_push_table_ref(
        self,
        bucket_idx: usize,
        table_ref: Arc<Tables>
    )
        -> Box<dyn TRef>
        {
            Box::new(
                HistoryRef {
                    inner_ : self,
                    bucket_idx_: Some(bucket_idx),
                    table_ref_: Some(table_ref),
                    data_ : None,
                    ops_ : Operation::Push,
                    #[cfg(all(feature = "pmem", feature = "wdrain"))]
                    pd_ptr: ptr::null_mut(),
                })
        }
}





//impl BucketPushRef for Arc<Row<NewOrder, (i32, i32, i32)>> {
//    fn into_push_bucket_ref
//
//}
//

