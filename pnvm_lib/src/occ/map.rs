use chashmap::{CHashMap, ReadGuard};

use concurrent_hashmap::*;
use std::collections::hash_map::RandomState;

use datatype::tbox::TBox;
use std::{fmt::Debug, hash::Hash, sync::Arc};

pub struct TMap<K, V>
where
    K: PartialEq + Hash + Send + Sync + Eq,
    V: Debug + Clone,
{
    //    inner_ : CHashMap<K, Arc<TBox<V>>>,
    inner_: ConcHashMap<K, Arc<TBox<V>>, RandomState>,
}

impl<K, V> TMap<K, V>
where
    K: PartialEq + Hash + Send + Sync + Eq,
    V: Debug + Clone,
{
    pub fn new() -> TMap<K, V> {
        TMap {
            //inner_ : CHashMap::new()
            inner_: ConcHashMap::<_, _, RandomState>::new(),
        }
    }

    pub fn insert(&self, key: K, tbox: Arc<TBox<V>>) -> Option<Arc<TBox<V>>> {
        self.inner_.insert(key, tbox)
    }

    //pub fn get(&self, k : &K) -> Option<ReadGuard<K, Arc<TBox<V>>>>
    pub fn get(&self, k: &K) -> Option<Accessor<K, Arc<TBox<V>>>> {
        //self.inner_.get(k)
        self.inner_.find(k)
    }

    pub fn new_with_options(conc: u16) -> TMap<K, V> {
        let mut opt = Options::default();
        opt.concurrency = conc;
        TMap {
            inner_: ConcHashMap::<_, _, RandomState>::with_options(opt),
        }
    }
}
