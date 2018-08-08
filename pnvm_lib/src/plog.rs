use libc;
use pnvm_sys;
use std::{
    mem::{self, size_of},
    ptr::{self, Unique},
};
use tcore::TObject;
use txn::Tid;

//This is the nightly api. Waiting for alloc::allocator::Layout to be stable
use core::alloc::Layout;

#[repr(C)]
#[derive(Clone)]
pub struct PLog {
    header: PLogHeader,
    data:   PLogData,
}

#[repr(C)]
#[derive(Clone)]
pub struct PLogHeader {
    log_kind: u16,
    len:      usize,
    txn_id:   u32,
    is_none:  bool,
}

#[repr(C)]
#[derive(Clone)]
pub struct PLogData {
    addr: *mut u8,
    size: usize,
}

const LOG_KIND_DATA: u16 = 0;
const LOG_KIND_TXN: u16 = 1;

impl PLog {
    pub fn new(ptr: *mut u8, layout: Layout, id: Tid) -> PLog {
        PLog {
            header: PLogHeader {
                log_kind: LOG_KIND_DATA,
                len:      layout.size(),
                txn_id:   id.into(),
                is_none: false,
            },
            data:   PLogData {
                addr: ptr,
                size: layout.size(),
            },
        }
    }

    pub fn new_none(layout: Layout, id: Tid) -> PLog {
        PLog {
            header: PLogHeader {
                log_kind: LOG_KIND_DATA,
                len:    layout.size(),
                txn_id: id.into(),
                is_none: true,
            },
            data: PLogData {
                addr: ptr::null_mut(),
                size: layout.size(),
            }
        }
    }

    //pub fn new(obj : &TObject<T>, id : Tid) -> PLog<T> {
    //    let addr = (obj).get_addr();
    //
    //    PLog {
    //        header: PLogHeader {
    //            log_kind :LOG_KIND_DATA,
    //            len : size_of::<T>(),
    //            txn_id: id.into()
    //        },
    //        data : PLogData {
    //            addr : addr,
    //            size : size_of::<T>()
    //        }
    //    }
    //}
}

impl Into<libc::iovec> for PLogHeader {
    fn into(self) -> libc::iovec {
        libc::iovec {
            iov_base: &self as *const _ as *mut libc::c_void,
            iov_len:  size_of::<PLogHeader>(),
        }
    }
}

impl Into<libc::iovec> for PLogData {
    fn into(self) -> libc::iovec {
        libc::iovec {
            iov_base: self.addr as *mut libc::c_void,
            iov_len:  self.size,
        }
    }
}

pub fn into_iovec(log: PLog) -> (libc::iovec, libc::iovec) {
    let mut log = log;
    let iovp_header = libc::iovec {
        iov_base: &mut log.header as *mut _ as *mut libc::c_void,
        iov_len:  size_of::<PLogHeader>(),
    };

    let iovp_data = libc::iovec {
        iov_base: log.data.addr as *mut libc::c_void,
        iov_len:  log.data.size,
    };

    (iovp_header, iovp_data)
}

pub fn persist_log(logs: Vec<PLog>) {
    let mut iovecs = Vec::with_capacity(logs.len());

    for (iov_header, iov_data) in logs.into_iter().map(move |log| into_iovec(log)) {
        iovecs.push(iov_header);
        iovecs.push(iov_data);
    }

    iovecs.shrink_to_fit();
    debug_assert!(iovecs.capacity() == iovecs.len());
    pnvm_sys::persist_log(&iovecs);
}

pub fn persist_txn(id: u32) {
    let mut iovecs = Vec::with_capacity(2);

    let log = PLog {
        header: PLogHeader {
            log_kind: LOG_KIND_TXN,
            len:      size_of::<u32>(),
            txn_id:   id,
            is_none: false,
        },

        data: PLogData {
            addr: unsafe { mem::transmute::<&u32, *mut u8>(&id) },
            size: size_of::<u32>(),
        },
    };

    iovecs.push(log.header.into());
    iovecs.push(log.data.into());

    iovecs.shrink_to_fit();
    debug_assert!(iovecs.capacity() == iovecs.len());
    pnvm_sys::persist_log(&iovecs);
    //pnvm_sys::walk(0, visit_log);
}

//FOR u32 only
extern "C" fn visit_log(
    buf: *const libc::c_void,
    len: libc::size_t,
    _: *mut libc::c_void,
) -> libc::c_int {
    println!("------Starting Walk[{:p}, {}]-------", buf, len);
    let mut buf = buf as *mut u8;
    let end = unsafe { buf.add(len) };

    while buf < end {
        let headerp = buf as *const PLogHeader;
        let header = unsafe { &*headerp };

        println!("Entry from tid : {}", header.txn_id);
        println!("Len : {}", header.len);
        println!("Kind : {}", header.log_kind);

        buf = unsafe { buf.add(size_of::<PLogHeader>()) };

        let datap = buf as *const u32;
        let data = unsafe { *datap };
        println!("Data : {}", data);

        buf = unsafe { buf.add(size_of::<u32>()) };
    }
    0
}
