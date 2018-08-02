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

use crossbeam::sync::ArcCell;

use std::hash::Hash;

pub struct PMap<K,V>
where K : PartialEq+Hash
      {
          inner_ : CHashMap<K, PValue<V>>
      }


impl<K,V> PMap<K,V> 
where K : PartialEq+Hash
{
    pub fn new() -> PMap<K,V> {
        PMap {
            inner_ : CHashMap::new()
        }
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
}


pub struct PValue<V> 
{
    data_ : Mutex<V>,
    last_writer_ : ArcCell<TxnInfo>, //ASSUMP: Must exist a creator>
    is_write_locked: AtomicBool,
}


impl<V> PValue<V>  
{
    pub fn new(t : V, ctor: Arc<TxnInfo>) -> PValue<V> {
        PValue {
            data_ : Mutex::new(t),
            last_writer_ : ArcCell::new(ctor),
            is_write_locked: AtomicBool::new(false),
        }
    }

    pub fn read(&self) -> MutexGuard<V> {
        let tx_ref = TransactionPar::get_thread_txn();
        let mut tx = tx_ref.borrow_mut();
        match self.data_.try_lock() {
            Ok(g) => g,
            Err(_) => { 
                if self.is_write_locked.load(Ordering::SeqCst) { /* Locked by a writer */
                    tx.add_dep(self.last_writer_.get());
                }
                let g = self.data_.lock().unwrap();
                self.is_write_locked.store(false, Ordering::SeqCst);
                g
            }
        }
    }

    pub fn write(&self) -> MutexGuard<V> {
        //Since it is thread local
        let tx_ref = TransactionPar::get_thread_txn();
        let mut tx = tx_ref.borrow_mut();
        match self.data_.try_lock() {
            Ok(g) => g,
            Err(_) => {
                tx.add_dep(self.last_writer_.get());
                let g = self.data_.lock().unwrap();
                self.is_write_locked.store(true, Ordering::SeqCst);
                self.last_writer_.set(tx.txn_info().clone());
                g
            }
        }
    }
}

