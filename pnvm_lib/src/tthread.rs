//Not used

use pnvm_sys::PMem;
use std::{
    cell::RefCell,
    rc::Rc,
    string::String
};
use conf;

thread_local!{
    //This init should just be dummy
    pub static PMEM_ALLOCATOR : Rc<RefCell<PMem>> = Rc::new(RefCell::new(PMem::new(String::from(conf::PNVM_PMEM_DIR), conf::PNVM_DEFAULT_MAX_SIZE).unwrap()));
}

pub fn set_mem_alloc(dir : String, size : usize) {
    let pmem = PMem::new(dir, size).unwrap();
    PMEM_ALLOCATOR.with(|pmem_cell| *pmem_cell.borrow_mut() = pmem);
}

pub fn get_mem_alloc() -> Rc<RefCell<PMem>> {
    PMEM_ALLOCATOR.with(|pmem_cell| Rc::clone(&pmem_cell))
}

