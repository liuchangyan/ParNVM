extern crate pnvm_sys;

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

fn main() -> std::io::Result<()> {
    let dir = CString::new("/home/v-xuc/ParNVM/data").unwrap();
    let dir_ptr = dir.as_ptr();
    let mut kind_ptr : *mut MemKind= null_mut();
    let kind_ptr2 = (&mut kind_ptr) as *mut _ as *mut *mut MemKind;
    let pmem_max_size: usize = 1024 * 1024 * 16 * 16;
    let err =  unsafe { memkind_create_pmem(dir_ptr, pmem_max_size, kind_ptr2)};
    //println!("Return code {}", err);
    //println!("Debug : {:?}", unsafe {&(**kind_ptr2)});
    //println!("Debug : {:?}", unsafe {&(*kind_ptr)});
    if err == 0 {
        println!("yes got you!");
    } else {
        println!("GG create failed");
        panic!("noooooo!");
    }
    let kind_ref = unsafe {&mut *(kind_ptr)};

    let data =  unsafe { memkind_malloc(kind_ref, size_of::<u32>()) };
    
    if data.is_null() {
        println!("Malloc failed");
    } else {

        println!("Malloc ok @ {:p}", data);

        let val : &mut u32 = unsafe {&mut *(data as *mut u32)};
        println!("Val : {:?}", val);
        *val = 10;
        println!("Val : {:?}", val);
        println!("Val address : {:p}", val);
        unsafe { memkind_free(kind_ref, data)};
    }

    Ok(())
}
