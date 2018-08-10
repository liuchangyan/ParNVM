#![feature(alloc, allocator_api)]
extern crate libc;

#[cfg(not(any(feature = "profile", feature = "unstable")))]
extern crate alloc;
#[cfg(any(feature = "profile", feature = "unstable"))]
extern crate core;

#[macro_use]
extern crate log;

use libc::*;

#[cfg(any(feature = "profile", feature = "unstable"))]
pub use core::alloc::{Alloc, AllocErr, GlobalAlloc, Layout};

#[cfg(not(any(feature = "profile", feature = "unstable")))]
pub use alloc::allocator::{Alloc, AllocErr, Layout};

extern crate rand;

use std::{
    cell::RefCell,
    ffi::{CStr, CString},
    fmt,
    mem::*,
    ptr,
    rc::Rc,
    str,
    string::String,
    thread,
};

const LPREFIX: &'static str = "pnvm_sys::";

/* *************
 * Exposed APIS
 * **************/
pub fn alloc(layout: Layout) -> Result<*mut u8, AllocErr> {
    panic!("not used anymore");
    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().alloc(layout))
}

pub fn dealloc(ptr: *mut u8, layout: Layout) {
    panic!("not used anymore");
    warn!("freeing pointer {:p}", ptr);
    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().dealloc(ptr, layout))
}

pub fn flush(ptr: *mut u8, layout: Layout) {
    trace!("flush {:p} , {}", ptr, layout.size());
    unsafe { pmem_flush(ptr as *const c_void, layout.size()) };
}

pub fn drain() {
    unsafe {pmem_drain()};
}

//pub fn persist_single(addr : *const c_void, size : usize) {
//    trace!("persit_single::(addr : {:p}, size : {})", addr, size);
//    PMEM_LOGGER.with(|pmem_log| pmem_log.borrow_mut().append_single(addr, size));
//}

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
    PMEM_ALLOCATOR.with(|pmem_cell| pmem_cell.borrow_mut().check());
    PMEM_LOGGER.with(|pmem_log| pmem_log.borrow_mut().check());
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

#[link(name = "memkind")]
extern "C" {
    //Memkind Wrappers
    pub fn memkind_create_pmem(
        dir: *const c_char,
        max_size: size_t,
        kind: *mut *mut MemKind,
    ) -> c_int;

    pub fn memkind_malloc(kind: *mut MemKind, size: size_t) -> *mut u8;
    pub fn memkind_free(kind: *mut MemKind, ptr: *mut u8);
    pub fn memkind_check_available(kind: *mut MemKind) -> c_int;

    pub fn memkind_pmem_destroy(kind: *mut MemKind) -> c_int;
}

pub const PMEM_MIN_SIZE: usize = 1024 * 1024 * 16;
pub const PMEM_DEFAULT_SIZE: usize = 48 * PMEM_MIN_SIZE;
const PMEM_ERROR_OK: c_int = 0;
pub const PMEM_FILE_DIR: &'static str = "/home/v-xuc/ParNVM/data";
pub const PMEM_FILE_DIR_BYTES: &'static [u8] = b"/home/v-xuc/ParNVM/data\0";
const PLOG_FILE_PATH: &'static str = "/home/v-xuc/ParNVM/data/log";
const PLOG_MIN_SIZE: usize = 1024 * 1024 * 2;
const PLOG_DEFAULT_SIZE: usize = 2 * PLOG_MIN_SIZE;

#[repr(C)]
pub struct MemKind {
    ops_ptr: *mut c_void,
    partitions: c_uint,
    name: [u8; 64],
    init_once: c_int, //No matching type in libc tho
    arena_map_len: c_uint,
    arena_map: *mut c_uint,
    arena_key: pthread_key_t,
    _priv: *mut c_void,
    arena_map_mask: c_uint,
    arena_zero: c_uint,
}

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
    poolset_uuid: uuid_t,
    uuid: uuid_t,
    prev_part_uuid: uuid_t,
    next_part_uuid: uuid_t,
    prev_repl_uuid: uuid_t,
    next_repl_uuid: uuid_t,

    crtime: uint64_t,
    arch_flags: ArchFlags,
    unused: [c_uchar; 1888],

    unused2: [c_uchar; 1992],

    sds: ShutdownState,
    checksum: uint64_t,
}

type uuid_t = [c_uchar; 16];

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

#[derive(Debug, Copy, Clone)]
pub struct PMem {
    pub kind: *mut MemKind,
    pub size: usize,
}

thread_local!{
    //This init should just be dummy
    pub static PMEM_ALLOCATOR : Rc<RefCell<PMem>> = Rc::new(RefCell::new(PMem::new(String::from(PMEM_FILE_DIR), PMEM_DEFAULT_SIZE)));

    pub static PMEM_LOGGER : Rc<RefCell<PLog>> = Rc::new(RefCell::new(PLog::new(String::from(PLOG_FILE_PATH), PLOG_DEFAULT_SIZE, !std::env::var("DEBUG").unwrap_or("false".to_string()).parse::<bool>().unwrap())));

}

//FIXME::Potentially could implement Alloc Trait from rust
impl PMem {
    //Allocate max_size pmem and returns the memory allocator

    pub fn new_bytes_with_nul_unchecked(dir: &[u8], max_size: usize) -> PMem {
        let dir_str = unsafe { CStr::from_bytes_with_nul_unchecked(dir) };
        let dir_ptr = dir_str.as_ptr();

        let mut kind_ptr: *mut MemKind = ptr::null_mut();
        let kind_ptr_ptr = (&mut kind_ptr) as *mut _ as *mut *mut MemKind;

        if max_size < PMEM_MIN_SIZE {
            panic!("pmem size too small");
            //return None;
        }

        //println!("pemem  create @ {:} {:} {:p} ",  _dir, max_size, kind_ptr_ptr);
        let err = unsafe { memkind_create_pmem(dir_ptr, max_size, kind_ptr_ptr) };
        if err != PMEM_ERROR_OK {
            panic!(
                "pemem failed create {} @ {:?} {:} {:p}\n",
                err,
                dir,
                max_size,
                unsafe { *kind_ptr_ptr }
            );
            //return None;
        }

        PMem {
            kind: unsafe { &mut *(kind_ptr) },
            size: max_size,
        }
    }

    pub fn new(dir: String, max_size: usize) -> PMem {
        trace!("{:}new(dir: {:}, max_size:{:})", LPREFIX, dir, max_size);
        let _dir = String::clone(&dir);
        let dir = CString::new(dir).unwrap();
        //let dir_ptr = dir.as_ptr();
        let dir_ptr = dir.into_raw();
        let mut kind_ptr: *mut MemKind = ptr::null_mut();
        let kind_ptr_ptr = (&mut kind_ptr) as *mut _ as *mut *mut MemKind;

        if max_size < PMEM_MIN_SIZE {
            panic!("pmem size too small");
            //return None;
        }

        //println!("pemem  create @ {:} {:} {:p} ",  _dir, max_size, kind_ptr_ptr);
        let err = unsafe { memkind_create_pmem(dir_ptr, max_size, kind_ptr_ptr) };
        let _ = unsafe { CString::from_raw(dir_ptr) };
        if err != PMEM_ERROR_OK {
            panic!(
                "pemem failed create {} @ {:} {:} {:p}\n",
                err,
                _dir,
                max_size,
                unsafe { *kind_ptr_ptr }
            );
            //return None;
        }

        PMem {
            kind: unsafe { &mut *(kind_ptr) },
            size: max_size,
        }
    }

    pub fn alloc(&mut self, layout: Layout) -> Result<*mut u8, AllocErr> {
        debug_assert!(layout.size() > 0, "alloc: size of layout must be non-zero");
        let res = unsafe { memkind_malloc(self.kind, layout.size()) };

        if res.is_null() {
            #[cfg(not(any(feature = "profile", feature = "unstable")))]
            return Err(AllocErr::Exhausted { request: layout });

            #[cfg(any(feature = "profile", feature = "unstable"))]
            return Err(AllocErr);
        } else {
            return Ok(res);
        }
    }

    pub fn dealloc(&mut self, ptr: *mut u8, layout: Layout) {
        debug_assert!(
            layout.size() > 0,
            "dealloc: size of layout must be non-zero"
        );

        unsafe { memkind_free(self.kind, ptr) };
    }

    pub fn check(&mut self) {
        let res = unsafe { memkind_check_available(self.kind) };
        if res != 0 {
            panic!("memkeind check failed");
        }
    }

    pub fn is_pmem(ptr: *mut u8, size: usize) -> bool {
        let res = unsafe { pmem_is_pmem(ptr as *const c_void, size) };
        println!("result {}", res);
        if res == 1 {
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct PLog {
    plp: *mut LogPool,
    size: usize,
    path: String,
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

impl fmt::Debug for MemKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //  write!(f, "heyehe")
        write!(
            f,
            "MemKind {{
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
            unsafe { str::from_utf8_unchecked(&(self.name)) },
            self.init_once,
            self.arena_map_len,
            self.arena_map,
            self.arena_key,
            self._priv,
            self.arena_map_mask,
            self.arena_zero
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate env_logger;

    const PMEM_TEST_PATH_ABS: &str = "/home/v-xuc/ParNVM/data";
    // const PMEM_TEST_PATH_WRONG : &str = "/home/v-xuc";

    #[test]
    fn test_create_ok() {
        //absolute path
        let _ = env_logger::init();
        let pmem = PMem::new(String::from(PMEM_TEST_PATH_ABS), 16 * super::PMEM_MIN_SIZE);
        assert_eq!(pmem.is_some(), true);
        pmem.unwrap().check();
        //relative path
        //let pmem = PMem::new(String::from("../data"), 16*super::PMEM_MIN_SIZE);
        //assert_eq!(pmem.is_some(), true);
        //assert_eq!(pmem.unwrap().check(), true);
    }

    #[test]
    #[should_panic]
    fn test_create_non_exist() {
        let _ = env_logger::init();
        let pmem = PMem::new(String::from("../../data"), 16 * super::PMEM_MIN_SIZE);
        assert_eq!(pmem.is_some(), false);
    }

    #[test]
    #[should_panic]
    fn test_size_too_small() {
        let _ = env_logger::init();
        let pmem = PMem::new(String::from("../data"), super::PMEM_MIN_SIZE / 2);
        assert_eq!(pmem.is_some(), false);
    }

    #[test]
    fn test_malloc_ok() {
        let _ = env_logger::init();
        let mut pmem = PMem::new(String::from("../data"), super::PMEM_MIN_SIZE * 4).unwrap();
        let res = pmem.alloc(Layout::new::<u32>());
        assert_eq!(res.is_ok(), true);
        //FIXME: This assert is never true due to pmem_is_pmem(3) caveats
        //More details at: http://pmem.io/pmdk/manpages/linux/v1.4/libpmem/pmem_is_pmem.3
        //assert_eq!(PMem::is_pmem(res.unwrap(), size_of::<u32>()), true);
    }

    //#[test]
    fn test_non_pem_check() {
        let _ = env_logger::init();

        let mut pmem = PMem::new(String::from("../dat"), super::PMEM_MIN_SIZE * 4).unwrap();
        let res = pmem.alloc(Layout::new::<u32>());
        assert_eq!(res.is_ok(), true);
        //assert_eq!(PMem::is_pmem(res.unwrap(), size_of::<u32>()), false);
    }

    #[test]
    fn test_malloc_fail() {
        let _ = env_logger::init();
        let mut pmem = PMem::new(String::from("../data"), super::PMEM_MIN_SIZE * 4).unwrap();
        let res = pmem.alloc(Layout::from_size_align(PMEM_MIN_SIZE * 5, 4).unwrap());
        assert_eq!(res.is_err(), true);
    }

    //FIXME: this one creating invalid references
    //#[test]
    fn test_dealloc_ok() {
        let mut pmem = PMem::new(String::from("../data"), super::PMEM_MIN_SIZE * 4).unwrap();
        let res = pmem.alloc(Layout::new::<u32>());
        pmem.dealloc(res.unwrap(), Layout::new::<u32>());
    }

    #[test]
    fn test_alloc_ok() {
        let _ = env_logger::init();
        let res = super::alloc(Layout::new::<u32>());
        assert_eq!(res.is_ok(), true);
    }

    #[test]
    fn test_free_thread_ok() {
        let _ = env_logger::init();
        let res = super::alloc(Layout::new::<u32>());
        assert_eq!(res.is_ok(), true);
        super::dealloc(res.unwrap(), Layout::new::<u32>());
    }

    #[test]
    fn test_flush_ok() {
        let _ = env_logger::init();
        let res = super::alloc(Layout::new::<u32>());
        let value = res.unwrap();
        unsafe { *value = 10 };
        trace!("here");
        super::flush(value, Layout::new::<u32>());
    }

    // #[test]
    // fn test_append_log_ok() {
    //     let _ = env_logger::init();
    //     let mut plog = PLog::new(String::from(PLOG_FILE_PATH), PLOG_DEFAULT_SIZE, false);
    //     let offset_before = plog.tell();
    //     trace!("offset_before : {}", offset_before);
    //     let tid = 999;
    //     plog.append(tid);
    //     let offset_after = plog.tell();
    //     trace!("offset_after : {}", offset_after);
    //     assert_eq!(offset_before + size_of::<u32>() as i64, offset_after);
    // }
    use std::sync::{Arc, Mutex};
    #[test]
    fn test_multiple_create() {
        //let mut pmems = vec![];
        let mut handles = vec![];
        //let mtx = Arc::new(Mutex::new(0));
        for i in 1..80 {
            //let mtx = mtx.clone();
            let handle = thread::spawn(move || {
                //let g = mtx.lock().unwrap();
                let pmem1 = PMem::new(String::from("../data"), super::PMEM_MIN_SIZE).unwrap();
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_multiple_create_single_thread() {
        for i in 1..80 {
            std::fs::create_dir_all(format!("../data/{}", i)).unwrap();
            let mut pmem = PMem::new(format!("../data/{}", i), super::PMEM_MIN_SIZE).unwrap();
        }
    }
}
