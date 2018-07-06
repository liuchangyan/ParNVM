use tcore::TObject;
use txn::Tid;

pub struct PLog<T> {
    addr : NonNull<T>,
    val : T,
    ver: Tid,
    
    id: Tid,
}

impl<T> PLog<T> {
    
    pub fn new(obj : &TObject<T>, id : Tid) -> PLog<T> {
        
    }


}
