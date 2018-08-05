//Map Container that adds version on each value 
use chashmap::{
    CHashMap,
    ReadGuard,
};

use super::nvm_txn::*;
use std::sync::{
    Mutex,
    MutexGuard,
    Arc,    
    atomic::{Ordering, AtomicBool},
};

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use crossbeam::sync::ArcCell;

use std::hash::Hash;

pub struct PMap<K,V>
where K : PartialEq+Hash,
      V : Debug
      {
          inner_ : CHashMap<K, PValue<V>>
      }


impl<K,V> PMap<K,V> 
where K : PartialEq+Hash,
      V : Debug
{
    pub fn new() -> PMap<K,V> {
        PMap {
            inner_ : CHashMap::new()
        }
    }

    pub fn new_with_keys(keys: Vec<K>) -> PMap<K, V> 
    {
        let pmap = PMap::new();
        for key in keys.into_iter() {
           let pval = PValue::default(); 
           pmap.insert_new(key, pval);
        }

        pmap
    }

    pub fn get(&self, k: &K) 
        -> Option<ReadGuard<K,PValue<V>>> 
              {
                  self.inner_.get(k) 
              }


    pub fn insert_new(&self, key: K, pval : PValue<V>) 
    {
        self.inner_.insert_new(key, pval);
    }

    pub fn insert(&self, key: K, pval : PValue<V>) 
        -> Option<PValue<V>>
    {
        self.inner_.insert(key, pval)
    }
}

#[derive(Debug)]
pub struct PValue<V> 
where V : Debug
{
    data_ : Mutex<Option<V>>,
    last_writer_ : ArcCell<TxnInfo>, //ASSUMP: Must exist a creator>
    is_write_locked: AtomicBool,
}


impl<V> PValue<V>  
where V : Debug
{
    pub fn new(t : V, tx: &mut TransactionPar) -> PValue<V> {
        let ctor = tx.txn_info();
        PValue {
            data_ : Mutex::new(Some(t)),
            last_writer_ : ArcCell::new(ctor.clone()),
            is_write_locked: AtomicBool::new(false),
        }
    }

    pub fn read(&self, tx: &mut TransactionPar) -> PMutexGuard<V> {
        println!("read\t{:?}", self);
        match self.data_.try_lock() {
            Ok(g) => {
                self.is_write_locked.store(false, Ordering::SeqCst);
                PMutexGuard {
                    g_ : g,
                    val_ : self,
                }
            }
            Err(_) => { 
                if self.is_write_locked.load(Ordering::SeqCst) { /* Locked by a writer */
                    tx.add_dep(self.last_writer_.get());
                }
                let g = self.data_.lock().unwrap();
                self.is_write_locked.store(false, Ordering::SeqCst);
                PMutexGuard {
                    g_ : g,
                    val_ : self,
                }
            }
        }
    }

    pub fn write(&self, tx: &mut TransactionPar) -> PMutexGuard<V> {
        println!("write\t{:?}", self);
        match self.data_.try_lock() {
            Ok(g) => {
                self.is_write_locked.store(true, Ordering::SeqCst);
                self.last_writer_.set(tx.txn_info().clone());
                PMutexGuard {
                    g_ : g,
                    val_ : self,
                }
            },
            Err(_) => {
                tx.add_dep(self.last_writer_.get());
                let g = self.data_.lock().unwrap();
                self.is_write_locked.store(true, Ordering::SeqCst);
                self.last_writer_.set(tx.txn_info().clone());
                PMutexGuard {
                    g_ : g,
                    val_ : self,
                }
            }
        }
    }

    pub fn unlock(&self) {
        self.is_write_locked.store(false, Ordering::SeqCst);
    }
}

impl<V> Default for PValue<V> 
where V: Debug 
{
    fn default() -> Self {
        PValue  {
            data_ : Mutex::new(None),
            last_writer_: ArcCell::new(Arc::new(TxnInfo::default())),
            is_write_locked: AtomicBool::default(),
        }
    }
}


pub struct PMutexGuard<'mutex, 'v, V> 
where V: Debug + 'mutex + 'v
{
    g_ : MutexGuard<'mutex,Option<V>>,
    val_ : &'v PValue<V>
}


impl<'mutex, 'v,  V> Drop for PMutexGuard<'mutex,'v, V> 
where V: Debug 
{
    fn drop(&mut self) {
        self.val_.unlock()
    }
}


impl<'mutex, 'v, V> Deref for PMutexGuard<'mutex,'v,  V>
where V: Debug 
{
    type Target = Option<V>;

    fn deref(&self) -> &Option<V> {
        &*(self.g_)
    }
}

impl<'mutex,'v,  V> DerefMut for PMutexGuard<'mutex,'v,  V> 
where V: Debug 
{
    fn deref_mut(&mut self) -> &mut Option<V> {
        &mut *(self.g_)
    }
}
