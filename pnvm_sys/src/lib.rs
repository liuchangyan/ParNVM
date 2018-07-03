#![feature(alloc, allocator_api)]
extern crate libc;
extern crate alloc;

use libc::*;
use alloc::allocator::{
    AllocErr,
    Layout
};

use std::{
    fmt, 
    str, 
    ptr, 
    mem::*, 
    string::String,
    ffi::CString
};


#[link(name = "pmem")]
extern "C" {
    pub fn pmem_check_version(major_required: c_uint, minor_required: c_uint) -> *const c_char;
    pub fn pmem_deep_drain(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_deep_flush(addr: *const c_void, len: usize);
    pub fn pmem_deep_persist(addr: *const c_void, len: usize) -> c_int;
    pub fn pmem_drain(); pub fn pmem_errormsg() -> *const c_char;
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

#[link(name = "memkind")]
extern "C" {
    //Memkind Wrappers
    pub fn memkind_create_pmem(
        dir : *const c_char,
        max_size : size_t,
        kind : *mut *mut MemKind
    ) -> c_int;

    pub fn memkind_malloc(kind : &mut MemKind, size: size_t) -> *mut u8;
    pub fn memkind_free(kind : &mut MemKind, ptr : *mut u8);
    pub fn memkind_check_available(kind :&mut MemKind) -> c_int;
}

pub const PMEM_MIN_SIZE : usize = 1024 * 1024 * 16;
const PMEM_ERROR_OK : c_int = 0;

#[repr(C)]
pub struct MemKind {
    ops_ptr : *mut c_void,
    partitions : c_uint,
    name : [u8; 64],
    init_once : c_int, //No matching type in libc tho
    arena_map_len : c_uint,
    arena_map : *mut c_uint,
    arena_key : pthread_key_t,
    _priv : *mut c_void,
    arena_map_mask: c_uint,
    arena_zero : c_uint
}


#[derive(Debug)]
pub struct PMem<'a> {
    kind : &'a mut MemKind,
    size : usize,
    dir : String
}


//FIXME::Potentially could implement Alloc Trait from rust
impl<'a>  PMem<'a>  {
    //Allocate max_size pmem and returns the memory allocator
    pub fn new(dir: String, max_size : usize) -> Option<PMem<'a>> {
        let _dir = String::clone(&dir);
        let dir = CString::new(dir).unwrap();
        let dir_ptr = dir.as_ptr();
        let mut kind_ptr : *mut MemKind = ptr::null_mut();
        let kind_ptr_ptr = (&mut kind_ptr) as *mut _  as *mut *mut MemKind;

        if max_size < PMEM_MIN_SIZE {
            panic!("pmem size too small");
            //return None;
        }

        let err = unsafe { memkind_create_pmem(dir_ptr, max_size, kind_ptr_ptr)};
        if err != PMEM_ERROR_OK {
            panic!("pemem failed create {}", err);
            //return None;
        }

        Some(PMem{
            kind: unsafe { &mut *(kind_ptr) },
            size : max_size,
            dir: _dir
        })
    }


    pub fn alloc(&mut self,  layout : Layout) -> Result<*mut u8, AllocErr> {
        debug_assert!(layout.size() > 0, "alloc: size of layout must be non-zero");
        let res = unsafe { memkind_malloc(self.kind, layout.size()) }; 

        if res.is_null() {
            return Err(AllocErr::Exhausted{request :layout});
        } else {
            return Ok(res);                                
        }
    }

    pub fn dealloc(&mut self, ptr : *mut u8, layout : Layout) {
        debug_assert!(
            layout.size() > 0,
            "dealloc: size of layout must be non-zero"
        );

        unsafe { memkind_free(self.kind, ptr) };
    }

    pub fn check(&mut self) -> bool {
        let res = unsafe { memkind_check_available(self.kind)};
        if res != 0 {
            false
        } else {
            true
        }
    }

}


impl fmt::Debug for MemKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MemKind {{
             ops_ptr : {:p}
             partitions : {:?}
             name : {:?}
             init_once : {:?}
             arena_map_len : {:?}
             arena_map : {:p}
             arena_key: {:?}
             _priv: {:p}
             arena_map_mask : {:}
             arena_zero: {:?}
         }}",
         self.ops_ptr,
         self.partitions,
         unsafe { str::from_utf8_unchecked(&(self.name))},
         self.init_once,
         self.arena_map_len,
         self.arena_map,
         self.arena_key,
         self._priv,
         self.arena_map_mask,
         self.arena_zero)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    const PMEM_TEST_PATH_ABS : &str = "/home/v-xuc/ParNVM/data";
    const PMEM_TEST_PATH_WRONG : &str = "/home/v-xuc";

    #[test]
    fn test_create_ok() {
        let pmem = PMem::new(String::from(PMEM_TEST_PATH_ABS), 16*super::PMEM_MIN_SIZE);
        assert_eq!(pmem.is_some(), true);
        assert_eq!(pmem.unwrap().check(), true);

        let pmem = PMem::new(String::from("../data"), 16*super::PMEM_MIN_SIZE);
        assert_eq!(pmem.is_some(), true);
        assert_eq!(pmem.unwrap().check(), true);
    }
    
    #[test]
    #[should_panic]
    fn test_create_non_exist() {
        let pmem = PMem::new(String::from("../../data"), 16*super::PMEM_MIN_SIZE);
    }

    #[test]
    #[should_panic]
    fn test_size_too_small() {
        let pmem = PMem::new(String::from("../data"),  super::PMEM_MIN_SIZE / 2);
    }

    #[test]
    fn test_malloc_ok() {
        let mut pmem = PMem::new(String::from("../data"),  super::PMEM_MIN_SIZE *4).unwrap();
        let res =  pmem.alloc(Layout::new::<u32>());
        assert_eq!(res.is_ok(), true);
    }

    #[test]
    fn test_malloc_fail() {
        let mut pmem = PMem::new(String::from("../data"),  super::PMEM_MIN_SIZE *4).unwrap();
        let res =  pmem.alloc(Layout::from_size_align(PMEM_MIN_SIZE * 5, 4).unwrap());
        assert_eq!(res.is_err(), true);
    }
    
    #[test]
    fn test_dealloc_ok() {
        let mut pmem = PMem::new(String::from("../data"),  super::PMEM_MIN_SIZE *4).unwrap();
        let res =  pmem.alloc(Layout::new::<u32>());
        pmem.dealloc(res.unwrap(), Layout::new::<u32>());
    }
}










