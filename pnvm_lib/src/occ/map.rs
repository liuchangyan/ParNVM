use chashmap::{
    CHashMap,
    ReadGuard
};

use std::{
    hash::Hash,
    fmt::Debug,
    sync::Arc,
};
use tbox::TBox;


#[derive(Debug)]
pub struct TMap<K, V> 
where K:PartialEq + Hash, 
      V: Debug + Clone
{
    inner_ : CHashMap<K, Arc<TBox<V>>>,
}


impl<K,V> TMap<K, V>
where K :PartialEq + Hash,
      V: Debug + Clone
{
    pub fn new() -> TMap<K, V> {
        TMap {
            inner_ : CHashMap::new()
        }
    }
    
    pub fn insert(&self, key : K, tbox: Arc<TBox<V>>) -> Option<Arc<TBox<V>>>
    {
        self.inner_.insert(key, tbox)
    }

    pub fn get(&self, k : &K) -> Option<ReadGuard<K, Arc<TBox<V>>>>
    {
        self.inner_.get(k)
    }
}


