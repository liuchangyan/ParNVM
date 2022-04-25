#![allow(dead_code)]
// #![feature(alloc)]
// #![feature(ptr_internals)]
// #![feature(box_into_raw_non_null)]
extern crate libc;

#[cfg(not(any(feature = "profile", feature = "unstable")))]
extern crate alloc;
#[cfg(any(feature = "profile", feature = "unstable"))]
extern crate core;

#[macro_use]
extern crate log;

extern crate errno;
use errno::errno;

use libc::*;

#[cfg(any(feature = "profile", feature = "unstable"))]
pub use core::alloc::{Alloc, AllocErr, GlobalAlloc, Layout};

#[cfg(not(any(feature = "profile", feature = "unstable")))]
pub use alloc::alloc::{alloc, Layout};
// pub use core::alloc::{Alloc, AllocErr, Layout};

extern crate rand;

use std::{cell::RefCell, ffi::CString, ptr::NonNull, rc::Rc, str, string::String, thread};

const LPREFIX: &'static str = "pnvm_sys::";
const PMEM_FILE_CREATE: c_int = 1 << 0;
const PMEM_FILE_EXCL: c_int = 1 << 1;
const PMEM_FILE_SPARSE: c_int = 1 << 2;
const PMEM_FILE_TMPFILE: c_int = 1 << 3;

/* *************
 * Exposed APIS
 * **************/
//pub fn alloc(layout: Layout) -> Result<*mut u8, AllocErr> {
//    panic!("not used anymore");
//    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().alloc(layout))
//}
//
//pub fn dealloc(ptr: *mut u8, layout: Layout) {
//    panic!("not used anymore");
//    warn!("freeing pointer {:p}", ptr);
//    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().dealloc(ptr, layout))
//}

pub fn flush(ptr: *mut u8, size: usize) {
    trace!("flush {:p} , {}", ptr, size);
    unsafe { pmem_flush(ptr as *const c_void, size) };
}

pub fn drain() {
    unsafe { pmem_drain() };
}

/* Disk Operations*/
pub fn disk_memcpy(dest: *mut u8, src: *mut u8, n: size_t) -> *mut u8 {
    unsafe { memcpy(dest as *mut c_void, src as *const c_void, n) as *mut u8 }
}

pub fn disk_msync(addr: *mut u8, len: size_t) -> c_int {
    unsafe { msync(addr as *mut c_void, len, MS_ASYNC) }
}

pub fn disk_persist_log(iovecs: &Vec<iovec>) {
    DISK_LOGGER.with(|disk_log| {
        disk_log
            .borrow_mut()
            .append_many(iovecs, iovecs.len() as i32)
    });
}

pub fn persist_log(iovecs: &Vec<iovec>) {
    PMEM_LOGGER.with(|pmem_log| pmem_log.borrow_mut().append_many(iovecs, iovecs.len()));
}

pub fn walk(
    chunksize: usize,
    callback: extern "C" fn(buf: *const c_void, len: size_t, arg: *mut c_void) -> c_int,
) {
    trace!("walk : chunksize = {}", chunksize);
    PMEM_LOGGER.with(|pmem_log| pmem_log.borrow_mut().walk(chunksize, callback));
}

pub fn init() {
    //    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().check());
    PMEM_LOGGER.with(|pmem_log| pmem_log.borrow_mut().check());
}

pub fn mmap_file(path: String, len: usize) -> *mut u8 {
    let path = CString::new(path).unwrap();
    let pathp = path.as_ptr();

    let mapped_len: NonNull<usize> = NonNull::<usize>::dangling();
    let is_pmem: NonNull<c_int> = NonNull::<c_int>::dangling();

    let ret = unsafe {
         pmem_map_file(
            pathp,
            len,
            PMEM_FILE_CREATE | PMEM_FILE_TMPFILE,
            0777,
            mapped_len.as_ptr(),
            is_pmem.as_ptr(),
        )
    };

    if ret.is_null() {
        panic!("[pmem_map_file] failed {}", errno());
    }

    if !mapped_len.as_ptr().is_null() {
        //unsafe {assert_eq!(mapped_len.as_ref(), &len)};
        //unsafe{ debug!("[pmem_map_file]: mapped_len is {}", mapped_len.as_ref())};
    } else {
        panic!("[pmem_map_file]:mapped_len is null");
    }

    if !is_pmem.as_ptr().is_null() {
        unsafe {
            debug!("[pmem_map_file]: is_pmem: {}", is_pmem.as_ref());
            //    IS_PMEM.with(|is_pmem_ref| is_pmem_ref.borrow_mut() = is_pmem);
        }
    //unsafe { debug!("[pmem_map_file] is_pmem: {}", is_pmem.as_ref())};
    } else {
        panic!("[pmem_map_file]: is_pmeme is null");
    }

    debug!("mmap_file(): {:p}", ret);
    ret as *mut u8
}

pub fn memcpy_persist(pmemaddr: *mut u8, src: *mut u8, len: usize) {
    unsafe { pmem_memcpy_persist(pmemaddr as *mut c_void, src as *mut c_void, len) };
}

pub fn memcpy_nodrain(pmemaddr: *mut u8, src: *mut u8, len: usize) {
    unsafe { pmem_memcpy_nodrain(pmemaddr as *mut c_void, src as *mut c_void, len) };
}

pub fn memset_persist(pmemaddr: *mut u8, c: i32, len: usize) {
    unsafe { pmem_memset_persist(pmemaddr as *mut c_void, c as c_int, len) };
}

pub fn unmap(pmemaddr: *mut u8, len: usize) {
    unsafe { pmem_unmap(pmemaddr as *mut c_void, len) };
}

pub fn has_hw_drain() -> c_int {
    unsafe { pmem_has_hw_drain() }
}

pub fn has_auto_flush() -> c_int {
    unsafe { pmem_has_auto_flush() }
}

/* *****************
 *   Mappings
 * ****************/

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
    pub fn pmem_has_auto_flush() -> c_int;
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
    pub fn pmem_memset_persist(pmemdest: *mut c_void, c: c_int, len: usize) -> *mut c_void;
    pub fn pmem_memcpy_persist(
        pmemdest: *mut c_void,
        src: *const c_void,
        len: usize,
    ) -> *mut c_void;
    pub fn pmem_memcpy_nodrain(
        pmemdest: *mut c_void,
        src: *const c_void,
        len: usize,
    ) -> *mut c_void;

}

#[link(name = "pmemlog")]
extern "C" {
    pub fn pmemlog_create(path: *const c_char, poolsize: usize, mode: mode_t) -> *mut LogPool;
    pub fn pmemlog_open(path: *const c_char) -> *mut LogPool;
    pub fn pmemlog_close(plp: *mut LogPool);

    pub fn pmemlog_append(plp: *mut LogPool, buf: *const c_void, count: usize) -> c_int;
    pub fn pmemlog_appendv(plp: *mut LogPool, iov: *const iovec, iovecnt: usize) -> c_int;
    pub fn pmemlog_tell(plp: *mut LogPool) -> c_longlong;
    pub fn pmemlog_walk(
        plp: *mut LogPool,
        chunksize: usize,
        process_chunk: extern "C" fn(buf: *const c_void, len: size_t, arg: *mut c_void) -> c_int,
        arg: *mut c_void,
    );
}

pub const PMEM_MIN_SIZE: usize = 1024 * 1024 * 16;
pub const PMEM_DEFAULT_SIZE: usize = 48 * PMEM_MIN_SIZE;
const PMEM_ERROR_OK: c_int = 0;
//pub const PMEM_FILE_DIR: &'static str = "/home/v-xuc/ParNVM/data";
pub const PMEM_FILE_DIR: Option<&'static str> = option_env!("PMEM_FILE_DIR");
pub const PMEM_FILE_DIR_BYTES: &'static [u8] = b"/home/v-xuc/ParNVM/data\0";
//FIXME:
pub const PLOG_FILE_PATH: Option<&'static str> = option_env!("PLOG_FILE_PATH");
const DISK_LOG_FILE: &'static str = "/home/v-xuc/ParNVM/v-data/log";
const PLOG_MIN_SIZE: usize = 1024 * 1024 * 2;
const PLOG_DEFAULT_SIZE: usize = 2 * PLOG_MIN_SIZE;

#[repr(C)]
pub struct LogPool {
    hdr: LogHeader,

    start_offset: uint64_t,
    end_offset: uint64_t,
    write_offset: uint64_t,

    addr: *const c_void,
    size: usize,
    is_pmem: c_int,
    rdonly: c_int,
    rwlockp: *mut c_void, //FIXME: casting assumed
    is_dev_dax: c_int,
    set: *mut c_void, //FIXME: casting assumed
}

const POOL_HDR_SIG_LEN: usize = 8;
#[repr(C)]
pub struct LogHeader {
    signature: [c_char; POOL_HDR_SIG_LEN],
    major: uint32_t,
    compat_feat: uint32_t,
    incompat_feat: uint32_t,
    ro_compat_feat: uint32_t,
    poolset_uuid: Uuid,
    uuid: Uuid,
    prev_part_uuid: Uuid,
    next_part_uuid: Uuid,
    prev_repl_uuid: Uuid,
    next_repl_uuid: Uuid,

    crtime: uint64_t,
    arch_flags: ArchFlags,
    unused: [c_uchar; 1888],

    unused2: [c_uchar; 1992],

    sds: ShutdownState,
    checksum: uint64_t,
}

type Uuid = [c_uchar; 16];

#[repr(C)]
pub struct ArchFlags {
    align_desc: uint64_t,
    machine_class: uint8_t,
    data: uint8_t,
    reserved: [uint8_t; 4],
    machine: uint16_t,
}

#[repr(C)]
pub struct ShutdownState {
    usc: uint64_t,
    uuid: uint64_t,
    dirty: uint8_t,
    reserved: [uint8_t; 39],
    checksum: uint64_t,
}

thread_local!{

    pub static PMEM_LOGGER : Rc<RefCell<PLog>> = Rc::new(RefCell::new(PLog::new(String::from(PLOG_FILE_PATH.expect("plog_file_path should be set at compile time")), PLOG_DEFAULT_SIZE, !std::env::var("DEBUG").unwrap_or("false".to_string()).parse::<bool>().unwrap())));

    pub static DISK_LOGGER: Rc<RefCell<DLogger>>= Rc::new(RefCell::new(DLogger::new(String::from(DISK_LOG_FILE))));

}

//FIXME::Potentially could implement Alloc Trait from rust
//impl PMem {
//    //Allocate max_size pmem and returns the memory allocator
//
//    pub fn new_bytes_with_nul_unchecked(dir: &[u8], max_size: usize) -> PMem {
//        let dir_str = unsafe { CStr::from_bytes_with_nul_unchecked(dir) };
//        let dir_ptr = dir_str.as_ptr();
//
//        let mut kind_ptr: *mut MemKind = ptr::null_mut();
//        let kind_ptr_ptr = (&mut kind_ptr) as *mut _ as *mut *mut MemKind;
//
//        if max_size < PMEM_MIN_SIZE {
//            panic!("pmem size too small");
//            //return None;
//        }
//
//        //println!("pemem  create @ {:} {:} {:p} ",  _dir, max_size, kind_ptr_ptr);
//        let err = unsafe { memkind_create_pmem(dir_ptr, max_size, kind_ptr_ptr) };
//        if err != PMEM_ERROR_OK {
//            panic!(
//                "pemem failed create {} @ {:?} {:} {:p}\n",
//                err,
//                dir,
//                max_size,
//                unsafe { *kind_ptr_ptr }
//            );
//            //return None;
//        }
//
//        PMem {
//            kind: unsafe { &mut *(kind_ptr) },
//            size: max_size,
//        }
//    }
//
//    pub fn new(dir: String, max_size: usize) -> PMem {
//        trace!("{:}new(dir: {:}, max_size:{:})", LPREFIX, dir, max_size);
//        let _dir = String::clone(&dir);
//        let dir = CString::new(dir).unwrap();
//        //let dir_ptr = dir.as_ptr();
//        let dir_ptr = dir.into_raw();
//        let mut kind_ptr: *mut MemKind = ptr::null_mut();
//        let kind_ptr_ptr = (&mut kind_ptr) as *mut _ as *mut *mut MemKind;
//
//        if max_size < PMEM_MIN_SIZE {
//            panic!("pmem size too small");
//            //return None;
//        }
//
//        //println!("pemem  create @ {:} {:} {:p} ",  _dir, max_size, kind_ptr_ptr);
//        let err = unsafe { memkind_create_pmem(dir_ptr, max_size, kind_ptr_ptr) };
//        let _ = unsafe { CString::from_raw(dir_ptr) };
//        if err != PMEM_ERROR_OK {
//            panic!(
//                "pemem failed create {} @ {:} {:} {:p}\n",
//                err,
//                _dir,
//                max_size,
//                unsafe { *kind_ptr_ptr }
//            );
//            //return None;
//        }
//
//        PMem {
//            kind: unsafe { &mut *(kind_ptr) },
//            size: max_size,
//        }
//    }
//
//    pub fn alloc(&mut self, layout: Layout) -> Result<*mut u8, AllocErr> {
//        debug_assert!(layout.size() > 0, "alloc: size of layout must be non-zero");
//        let res = unsafe { memkind_malloc(self.kind, layout.size()) };
//
//        if res.is_null() {
//            #[cfg(not(any(feature = "profile", feature = "unstable")))]
//            return Err(AllocErr::Exhausted { request: layout });
//
//            #[cfg(any(feature = "profile", feature = "unstable"))]
//            return Err(AllocErr);
//        } else {
//            return Ok(res);
//        }
//    }
//
//    pub fn dealloc(&mut self, ptr: *mut u8, layout: Layout) {
//        debug_assert!(
//            layout.size() > 0,
//            "dealloc: size of layout must be non-zero"
//        );
//
//        unsafe { memkind_free(self.kind, ptr) };
//    }
//
//    pub fn check(&mut self) {
//        let res = unsafe { memkind_check_available(self.kind) };
//        if res != 0 {
//            panic!("memkeind check failed");
//        }
//    }
//
//    pub fn is_pmem(ptr: *mut u8, size: usize) -> bool {
//        let res = unsafe { pmem_is_pmem(ptr as *const c_void, size) };
//        println!("result {}", res);
//        if res == 1 {
//            true
//        } else {
//            false
//        }
//    }
//}

#[derive(Debug)]
pub struct PLog {
    plp: *mut LogPool,
    size: usize,
    path: String,
}

pub struct DLogger {
    fd: c_int,
    path: String,
}

//Logger for Disk
impl DLogger {
    fn new(path: String) -> DLogger {
        //Open a disk file
        let mut path_cpy = String::clone(&path);
        path_cpy.push_str(
            thread::current()
                .name()
                .expect("thrad local needs to have named threads"),
        );
        let path_cstr = CString::new(path_cpy).unwrap();
        let pathp = path_cstr.as_ptr();

        let mode = String::from("a+");
        let mode_cstr = CString::new(mode).unwrap();
        let modep = mode_cstr.as_ptr();

        let file = unsafe { fopen(pathp, modep) };
        let fd = unsafe { fileno(file) };

        DLogger { fd: fd, path: path }
    }

    fn append_many(&self, iovecs: &Vec<iovec>, size: c_int) {
        warn!("writev : {} items", size);
        unsafe { writev(self.fd, iovecs.as_ptr() as *const iovec, size) };
    }
}

impl Drop for DLogger {
    fn drop(&mut self) {
        unsafe { close(self.fd) };
    }
}

impl PLog {
    fn new(path: String, size: usize, thread_local: bool) -> PLog {
        trace!("{:}Plog::new(path: {:}, size:{:})", LPREFIX, path, size);
        let mut _path = String::clone(&path);
        if thread_local {
            _path.push_str(
                thread::current()
                    .name()
                    .expect("thrad local needs to have named threads"),
            );
        }
        let path = CString::new(String::clone(&_path)).unwrap();
        let pathp = path.as_ptr();

        let mut plp: *mut LogPool = unsafe { pmemlog_create(pathp, size, S_IWUSR | S_IRUSR) };
        if plp.is_null() {
            plp = unsafe { pmemlog_open(pathp) };
            if plp.is_null() {
                panic!("pmemlog_created failed ");
            }
        }

        PLog {
            plp: plp,
            size: size,
            path: _path,
        }
    }

    fn append_single(&self, addr: *const c_void, size: usize) {
        unsafe { pmemlog_append(self.plp, addr, size) };
    }

    fn tell(&self) -> i64 {
        unsafe { pmemlog_tell(self.plp) }
    }

    fn append_many(&self, iovecs: &Vec<iovec>, size: usize) {
        warn!("appendv : {} items", size);
        unsafe { pmemlog_appendv(self.plp, iovecs.as_ptr() as *const iovec, size) };
    }

    fn walk(
        &self,
        chunk_size: usize,
        callback: extern "C" fn(buf: *const c_void, len: size_t, arg: *mut c_void) -> c_int,
    ) {
        unsafe {
            let arg = &1 as *const _ as *mut c_void;
            pmemlog_walk(self.plp, chunk_size, callback, arg)
        };
    }

    fn check(&self) {
        if self.plp.is_null() {
            panic!("pmemlog check failed");
        }
    }
}

impl Drop for PLog {
    fn drop(&mut self) {
        unsafe { pmemlog_close(self.plp) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate env_logger;
    extern crate rand;

    const PMEM_TEST_PATH_ABS: &str = "/home/v-xuc/ParNVM/data";

    #[derive(Clone)]
    pub struct Customer {
        pub c_id: i32,
        pub c_d_id: i32,
        pub c_w_id: i32,
        pub c_first: [u8; 16],
        pub c_middle: [u8; 2],
        pub c_last: [u8; 16],
        pub c_street_1: [u8; 20],
        pub c_street_2: [u8; 20],
        pub c_city: [u8; 20],
        pub c_state: [u8; 2],
        pub c_zip: [u8; 9],
        pub c_phone: [u8; 16],
        pub c_since: i32, // Timestamp
        pub c_credit: [u8; 2],
        pub c_credit_lim: i32,   // i32(12,2)
        pub c_discount: i32,     // i32(4, 4)
        pub c_balance: i32,      // i32(12,2)
        pub c_ytd_payment: i32,  // i32(12,2)
        pub c_payment_cnt: i32,  // i32(4,0)
        pub c_delivery_cnt: i32, // i32(4,0)
        pub c_data: [u8; 500],
    }

    impl Customer {
        pub fn new() -> Self {
            let c_first: [u8; 16] = Default::default();
            let c_middle: [u8; 2] = Default::default();
            let c_last: [u8; 16] = Default::default();
            let c_street_1: [u8; 20] = Default::default();
            let c_street_2: [u8; 20] = Default::default();
            let c_city: [u8; 20] = Default::default();
            let c_state: [u8; 2] = Default::default();
            let c_zip: [u8; 9] = Default::default();
            let c_phone: [u8; 16] = Default::default();
            let c_credit: [u8; 2] = Default::default();
            let c_data: [u8; 500] = [1; 500];

            let c_id = 0;
            let c_d_id = 1;
            let c_w_id = 1;
            let c_since = 1;
            let c_credit_lim = 1;
            let c_discount = 1;
            let c_balance = 1;
            let c_ytd_payment = 1;
            let c_payment_cnt = 1;
            let c_delivery_cnt = 1;

            Customer {
                c_id,
                c_d_id,
                c_w_id,
                c_first,
                c_middle,
                c_last,
                c_street_1,
                c_street_2,
                c_city,
                c_state,
                c_zip,
                c_phone,
                c_since, // Timestamp
                c_credit,
                c_credit_lim,   // i32(12,2)
                c_discount,     // i32(4, 4)
                c_balance,      // i32(12,2)
                c_ytd_payment,  // i32(12,2)
                c_payment_cnt,  // i32(4,0)
                c_delivery_cnt, // i32(4,0)
                c_data,
            }
        }
    }

    use std::mem;
    use std::time::{Duration, Instant};
    #[test]
    fn single_write_dram() {
        let mut counter = 0;
        let size = 1 << 30;
        let pmem = mmap_file(String::from(PMEM_TEST_PATH_ABS), size);
        let dram_data = Box::into_raw(Box::new(Customer::new()));
        let offset_max = size / mem::size_of::<Customer>();
        let start = Instant::now();
        let duration = Duration::new(10, 0); //10 seconds
        let cus_size = mem::size_of::<Customer>();
        //let offset = rand::random::<usize>() % offset_max;
        let mut prev = 0;
        unsafe {
            while start.elapsed() < duration {
                let paddr = pmem.offset((((prev + 1000) % offset_max) * cus_size) as isize);
                prev = (prev + 100) % offset_max;
                memcpy_persist(paddr, dram_data as *mut u8, cus_size);
                counter += 1;
            }
        }
        println!("write:counter : {}, time: {:?}", counter, duration);
    }

    #[test]
    fn single_write_drain() {
        let mut counter = 0;
        let size = 1 << 30;
        let pmem = mmap_file(String::from(PMEM_TEST_PATH_ABS), size);
        let dram_data = Box::into_raw(Box::new(Customer::new()));
        let offset_max = size / mem::size_of::<Customer>();
        let start = Instant::now();
        let duration = Duration::new(10, 0); //10 seconds
        let cus_size = mem::size_of::<Customer>();
        //let offset = rand::random::<usize>() % offset_max;
        let mut prev = 0;
        unsafe {
            while start.elapsed() < duration {
                let paddr = pmem.offset((((prev + 1000) % offset_max) * cus_size) as isize);
                prev = (prev + 100) % offset_max;
                memcpy_persist(paddr, dram_data as *mut u8, cus_size);
                pmem_drain();
                counter += 1;
            }
        }
        println!("drain(): counter : {}, time: {:?}", counter, duration);
    }

}
