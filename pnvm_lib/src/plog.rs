use tcore::TObject;
use txn::Tid;
use std::{
    ptr::NonNull,
    mem::size_of,

};
use libc;
use pnvm_sys;

#[repr(C)]
pub struct PLog<T> 
where T: Clone
{
    header: PLogHeader,
    data : PLogData<T>,
}

#[repr(C)]
pub struct PLogHeader {
    log_kind : u16,
    len : usize,
    txn_id: u32
}


#[repr(C)]
pub struct PLogData<T>
where T : Clone
{
    addr : NonNull<T>,
    size : usize
}

const LOG_KIND_DATA : u16 = 0;

impl<T> PLog<T> 
where T : Clone
{
    
    pub fn new(obj : &TObject<T>, id : Tid) -> PLog<T> {
        let ver = obj.get_version().unwrap_or(Tid::new(0));

        let addr = (obj).get_addr();
        
        PLog {
            header: PLogHeader {
                log_kind :LOG_KIND_DATA,
                len : size_of::<T>(),
                txn_id: id.into()
            },
            data : PLogData {
                addr : addr, 
                size : size_of::<T>()
            }
        }
    }


}

pub fn persist_log<T: Clone>(logs : Vec<PLog<T>>){
    let mut iovecs = Vec::with_capacity(logs.len());
    for mut log in logs {
        let iovp_header = libc::iovec {
            iov_base : &mut log.header as *mut _ as *mut libc::c_void,
            iov_len : size_of::<PLogHeader>()
        };

        let iovp_data = libc::iovec {
            iov_base : log.data.addr.as_ptr() as *mut libc::c_void,
            iov_len : log.data.size 
        };

        iovecs.push(iovp_header);
        iovecs.push(iovp_data);
    }
    iovecs.shrink_to_fit();
    debug_assert!(iovecs.capacity() == iovecs.len());
    pnvm_sys::persist_log(&iovecs);
}
