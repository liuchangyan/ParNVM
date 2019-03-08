
use std::{
    sync::Arc,
    fmt::{Debug,Formatter, Result},
    ptr,
    any::Any,
};

use pnvm_lib::tcore::{
    TVersion,
    TRef, 
    Operation,
    ObjectId,
};

use pnvm_lib::{
    occ::occ_txn::TransactionOCC,
    txn::{Tid,TxnInfo},
};

#[cfg(not(any(feature = "pmem", feature = "disk")))]
use core::alloc::Layout;

#[cfg(any(feature = "pmem", feature = "disk"))]
use pnvm_sys::Layout;



//FIXME: reusing code from TPCC
use tpcc::table::{Key, Row};


#[derive(Clone)]
pub struct YCSBEntry {
    idx_ : isize,
    fields_ : Vec<Field>,
}

impl Key<isize> for YCSBEntry {
    fn primary_key(&self) -> isize {
        self.idx_
    }

    fn bucket_key(&self) -> usize {
        panic!("bucket_key not implemented for YCSBEntry");
    }

    fn field_offset(&self) -> [isize;32] {
        panic!("field_offset not implemented for YCSBEntry");
    }
}

impl Debug for YCSBEntry {
    fn fmt(&self, f: &mut Formatter) -> Result {
        for x in self.fields_.iter() {
            write!(f, "[{:?}]", x);
        }
        write!(f, "\n")
    }
}



type YCSBRow = Row<YCSBEntry, isize>;

pub struct YCSBTable {
    rows_ : Vec<Arc<YCSBRow>>,
}


#[derive(Clone)]
pub struct Field {
    data_: [u8;100],
}

impl Debug for Field {
    fn fmt(&self, f: &mut Formatter) -> Result {
        self.data_[..].fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct YCSBRef{
    inner_ : Arc<YCSBRow>,
    ops_ : Operation,
    data_ : Option<Box<YCSBEntry>>,

    #[cfg(all(feature = "pmem", feature = "wdrain"))]
    pd_ptr: *mut YCSBEntry,
}


impl TRef for YCSBRef {
    fn install(&self,  id: Tid) {
        match self.ops_ {
            Operation::Push => {
                panic!("Ops::Push not implemented for YCSBRef");
            },

            Operation::RWrite => {
                #[cfg(all(feature = "pmem", feature = "wdrain"))]
                {
                    if !self.pd_ptr.is_null() {
                        self.inner.install_ptr(self.pd_ptr, id);
                    } else {
                        panic!("pd_ptr should not be null at write");
                    }
                }


                #[cfg(not(all(feature = "pmem", feature = "wdrain")))]
                self.inner_.install_val(self.data_.as_ref().unwrap(), id);
            }, 
            _ => panic!("not impelented operation")
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
        match val.downcast::<YCSBEntry>() {
            Ok(val) => self.data_ = Some(val),
            Err(_) => panic!("YCSBRef::write value should be Box<YCSBEntry>")
        }
    }

    fn lock(&self, tid: Tid) -> bool {
        self.inner_.lock(tid)
    }

    fn unlock(&self) {
        self.inner_.unlock()
    }

    fn check(&self, vers: u32, tid: u32) -> bool {
        self.inner_.check(vers, tid)
    }

    fn set_access_info(&mut self, txn_info : Arc<TxnInfo> ) {
        self.inner_.set_access_info(txn_info);
    }

    fn get_access_info(&self) -> Arc<TxnInfo> {
        self.inner_.get_access_info()
    }

    fn get_name(&self) -> String {
        String::from("YCSB ")
    }

    /* For 2 Phase Locking */
    fn write_through(&self, val: Box<Any>, tid: Tid) {
        match val.downcast::<YCSBEntry>() {
            Ok(val) => self.inner_.install_val(&val, tid),
            Err(_) => panic!("runtime value should be YCSBEntry")
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
            Operation::RWrite => {
                self.inner_.vers_.write_lock(tid)
            },
            _ => {
                panic!("write_lock not implemented other than RWrite")
            }
        }
    }

    fn write_unlock(&self, tid: u32)  {
        match self.ops_ {
            Operation::RWrite => {
                self.inner_.vers_.write_unlock(tid);
            },
            _ => {
                panic!("write_unlock not implemented other than RWrite")
            }
        }
    }

}



impl YCSBTable {
    pub fn new() -> YCSBTable {
        YCSBTable {
            rows_: Vec::new(),
        }
    }

    pub fn new_with_rows(rows : Vec<Arc<YCSBRow>>) -> YCSBTable {
        YCSBTable {
            rows_: rows,
        }
    }


    pub fn insert_raw(&mut self, row: Arc<YCSBRow>){
        self.rows_.push(row)
    }

    pub fn retrieve_tref(&self, idx: usize) -> Option<Box<dyn TRef>> {
        if idx < self.len() {
            let row = &self.rows_[idx];
            let tref = Box::new(
                    YCSBRef {
                        inner_ : row.clone(),
                        ops_: Operation::RWrite,
                        data_ : None,
                        #[cfg(all(feature = "pmem", feature = "wdrain"))]
                        pd_ptr: ptr::null_mut(),
                    }
                );
            Some(tref)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.rows_.len()
    }
}


