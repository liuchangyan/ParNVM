



const INITIAL_CAP: usize = 8;

pub struct TArray<T>
where 
    T: Clone,
{
    vers_: Vec<TVersion>,
    data_ : RawVec<T>,
    size_: usize,
}



impl<T> TArray<T>
where 
    T: Clone,

{
    pub fn with_capacity(cap: usize) -> TArray {
        TArray {
            vers_ : Vec::with_capacity(cap),
            data_ : RawVec::with_capacity(cap),
            size_ : cap,
        }
    }


    pub fn trans_read(idx: usize) -> 
}



