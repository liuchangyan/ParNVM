/* A mirror of the original Box implimentation
 * Had there been a placement operator, this
 * wouldn't not need to be implemented from scratch.
 * The key goal is to allocate some datum with 
 * customized memory location, in this case, the 
 * persistent memory mapped.
 * Issue from rust here: 
 * https://github.com/rust-lang/rust/issues/27779#issuecomment-378416911 
 * */


pub struct PBox<T> {
    prt : Unique<T>,

}

impl<T> PBox<T> {
        
    pub fn new(val : T) -> Arc<RwLock<PBox<T>> {
        let ptr = unsafe { pnvm_sys::alloc(Layout::new::<T>()) };
        let ptr = Unique::new(ptr).unwrap();  


    }


}




