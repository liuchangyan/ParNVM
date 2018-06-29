extern crate libc;
//extern crate nvml_sys;

use libc::*;
use std::ffi::CString;
use std::fs::File;
use std::mem::*;

#[link(name = "pmem")]
extern "C" {
    pub fn pmem_check_version(major_required: c_uint, minor_required: c_uint) -> *const c_char;
    pub fn pmem_deep_drain(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_deep_flush(addr: *const c_void, len: usize);
    pub fn pmem_deep_persist(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_drain();
    pub fn pmem_errormsg() -> *const c_char;
    pub fn pmem_flush(addr: *const c_void, len: usize);
    pub fn pmem_has_hw_drain() -> c_int;
    pub fn pmem_is_pmem(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_map_file(
        path: *const c_char,
        len: usize,
        flags: c_int,
        mode: mode_t,
        mapped_lenp: *mut usize,
        is_pmemp: *mut c_int,
    ) -> *mut c_void;
    pub fn pmem_msync(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_persist(addr: *const c_void, len: usize);
    pub fn pmem_unmap(addr: *mut c_void, len: usize) -> c_int;

}

pub const PMEM_FILE_CREATE: c_int = 1;
pub const PMEM_FILE_EXCL: c_int = 2;
pub const PMEM_FILE_SPARSE: c_int = 4;
pub const PMEM_FILE_TMPFILE: c_int = 8;

const PMEM_LEN: usize = 4096;

fn main() -> std::io::Result<()> {
    let mut mapped_len: usize = unsafe { uninitialized() };
    let mapped_lenp = &mut mapped_len as *mut _;
    let mut is_pmem: i32 = unsafe { uninitialized() };
    let is_pmemp = &mut is_pmem as *mut _;
    let filepath = CString::new("../../data/myfile").unwrap();
    let filepath_ptr = filepath.as_ptr();
    let res = unsafe {
        pmem_map_file(
            filepath_ptr,
            PMEM_LEN,
            PMEM_FILE_CREATE,
            0666,
            mapped_lenp,
            is_pmemp,
        )
    };

    if res.is_null() {
        println!("Null pointer...");
    } else if is_pmem == 0 {
        println!("No pesistent memory..");
    } else if is_pmem == 1 {
        println!("WOWOWYOOHOH!");
    } else {
        panic!("pmem_map_file returns crap");
    }

    Ok(())
}
