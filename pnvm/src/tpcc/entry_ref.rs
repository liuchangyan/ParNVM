
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



#[derive(Clone , Debug)]
pub struct WarehouseRef  {
    inner_ : Arc<Row<Warehouse, i32>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Warehouse>>,
}


#[derive(Clone , Debug)]
pub struct DistrictRef  {
    inner_ : Arc<Row<District, (i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<District>>,
}

#[derive(Clone , Debug)]
pub struct CustomerRef  {
    inner_ : Arc<Row<Customer, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Customer>>,
}

#[derive(Clone , Debug)]
pub struct NewOrderRef  {
    inner_ : Arc<Row<NewOrder, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<NewOrder>>,
}

#[derive(Clone , Debug)]
pub struct OrderRef  {
    inner_ : Arc<Row<Order, (i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Order>>,
}

#[derive(Clone , Debug)]
pub struct OrderLineRef  {
    inner_ : Arc<Row<OrderLine, (i32, i32, i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<OrderLine>>,
}


#[derive(Clone , Debug)]
pub struct ItemRef  {
    inner_ : Arc<Row<Item, i32>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Item>>,
}

#[derive(Clone , Debug)]
pub struct StockRef  {
    inner_ : Arc<Row<Stock, (i32, i32)>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<Stock>>,
}

#[derive(Clone , Debug)]
pub struct HistoryRef  {
    inner_ : Arc<Row<History, i32>>,
    bucket_idx_: Option<usize>,
    table_ref_: Option<Arc<Tables>>,
    //txn_info_ : Option<Arc<TxnInfo>>,
    data_ : Option<Box<History>>,
}

impl  TRef for WarehouseRef {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.warehouse.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
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
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Warehouse>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("warehosuref::write value should be Box<Warehouse>")
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}



impl  TRef for DistrictRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.district.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
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
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<District>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }
    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}



impl  TRef for CustomerRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.customer.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
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
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Customer>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }
    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }


    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}


impl  TRef for NewOrderRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.neworder.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<NewOrder>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }


    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl  TRef for OrderRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.order.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Order>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }


    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl  TRef for OrderLineRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.orderline.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<OrderLine>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("DistrictRef::write value should be Box<Warehouse>")
        }
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl  TRef for ItemRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.item.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }
    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }

    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Item>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("ItemRef::write value should be Box<Warehouse>")
        }
    }

    fn read(&self) -> &Any {
        self.inner_.get_data()
    }


    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}

impl  TRef for HistoryRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.history.get_bucket(bucket_idx).push(row);

            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<History>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("Stock::write value should be Box<Warehouse>")
        }
    }

    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
    }
}
impl  TRef for StockRef  {
    fn install(&self, id: Tid) {
        match self.table_ref_ {
            Some(ref table) => {
                let row = self.inner_.clone();
                let bucket_idx = self.bucket_idx_.unwrap();
                table.stock.get_bucket(bucket_idx).push(row);
            },
            None => {
                self.inner_.install(self.data_.as_ref().unwrap(), id);
            }
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.lock(tid)
        } else {
            true
        }
    }

    fn unlock(&self) {
        if self.table_ref_.is_none() {
            self.inner_.unlock()
        }
    }

    fn check(&self, vers: u32) -> bool {
        if self.table_ref_.is_none() {
            self.inner_.check(vers)
        } else {
            true
        }
    }
    fn box_clone(&self) -> Box<dyn TRef> {
        Box::new(self.clone())
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

    fn write(&mut self, val: Box<Any>) {
        match val.downcast::<Stock>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("Stock::write value should be Box<Warehouse>")
        }
    }


    fn set_writer_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_writer_info(txn_info);
    }

    fn get_writer_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_writer_info()
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
                })
        }
}

impl TableRef for Arc<Row<History,i32>> {
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
                    table_ref_ : table_ref,
                    //txn_info_ : txn_info,
                    data_ : None 
                })
        }
}


