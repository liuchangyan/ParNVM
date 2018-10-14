//Map Container that adds version on each value 
use chashmap::{
    CHashMap,
    ReadGuard,
};

use concurrent_hashmap::*;
use std::collections::hash_map::RandomState;

use super::nvm_txn_occ::*;
use super::nvm_txn_2pl::*;
use txn::TxnInfo;
use std::sync::{
    Mutex,
    MutexGuard,
    Arc,    
    atomic::{Ordering, AtomicU32, AtomicBool, AtomicU8},
};

use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
    cell::{UnsafeCell},
};

use crossbeam::sync::ArcCell;

use std::hash::Hash;

//FIXME: waiting for alloc::allocator to be stable
#[cfg(feature="pmem")]
use core::alloc::Layout;

#[cfg(feature="pmem")]
use std::{
    ptr,
    mem,
};


pub struct PMap<K,V>
where K : PartialEq+Hash + Send+ Sync + Eq,
      V : Debug
      {
          //inner_ : CHashMap<K, PValue<V>>,
          inner_ : ConcHashMap<K, PValue<V>>,
      }


impl<K,V> PMap<K,V> 
where K : PartialEq+Hash + Send+ Sync + Eq,
      V : Debug
{
    pub fn new() -> PMap<K,V> {
        PMap {
            //inner_ : CHashMap::new()
            inner_ : ConcHashMap::<_, _, RandomState>::new(),
        }
    }

    pub fn new_with_size(cap : usize, bucket_num : u16) -> PMap<K, V> {
        let mut opt = Options::default();
        
        opt.concurrency = bucket_num;
        opt.capacity = cap;

        PMap {
            inner_ : ConcHashMap::with_options(opt),
        }
    }

   // pub fn new_with_keys(keys: Vec<K>) -> PMap<K, V> 
   // {
   //     let pmap = PMap::new();
   //     for key in keys.into_iter() {
   //        let pval = PValue::default(); 
   //        pmap.insert_new(key, pval);
   //     }

   //     pmap
   // }

    pub fn get(&self, k: &K) 
        -> Option<Accessor<K,PValue<V>>> 
              {
                  self.inner_.find(k) 
              }


   // pub fn insert_new(&self, key: K, pval : PValue<V>) 
   // {
   //     self.inner_.insert_new(key, pval);
   // }

    pub fn insert(&self, key: K, pval : PValue<V>) 
        -> Option<PValue<V>>
    {
        self.inner_.insert(key, pval)
    }
}

//pub struct PValue<V> 
//where V : Debug
//{
//    data_ : Mutex<Option<V>>,
//    last_writer_ : ArcCell<TxnInfo>, //ASSUMP: Must exist a creator>
//    is_write_locked: AtomicBool,
//}

pub struct PValue<V>
where V : Debug 
{
    lock_ : AtomicU32,
    data_ : UnsafeCell<Option<V>>,
    is_write_locked_ : AtomicBool,
    last_writer_ : ArcCell<TxnInfo>,
    count_ : AtomicU8,
}


impl<V> PValue<V>  
where V : Debug
{
    pub fn new(t : V, tx: &mut TransactionPar) -> PValue<V> {
        let ctor = tx.txn_info();
        PValue {
            data_ : UnsafeCell::new(Some(t)),
            last_writer_ : ArcCell::new(ctor.clone()),
            is_write_locked_: AtomicBool::new(false),
            lock_ : AtomicU32::new(0),
            count_ : AtomicU8::new(0),
        }
    }

    pub fn new_default(t : V) -> PValue<V> {
        PValue {
            data_: UnsafeCell::new(Some(t)),
            last_writer_ : ArcCell::new(Arc::new(TxnInfo::default())),
            is_write_locked_: AtomicBool::default(),
            lock_: AtomicU32::new(0),
            count_ : AtomicU8::new(0),
        }
    }

    fn lock(&self, tx: &mut TransactionPar) -> PMutexGuard<V> {
        let mut prev = 0;
        let tid :u32 = tx.id().into();
        loop {
            let cur = self.lock_.compare_and_swap(0, tid, Ordering::SeqCst);
            if cur == 0 || cur == tid { /* Get the lock */
                self.count_.fetch_add(1, Ordering::SeqCst);
                return PMutexGuard {
                    data_: unsafe{&mut *self.data_.get()},
                    val_ : self,
                    cur_: tid,
                }
            } else { /* Add dep */
                /* FIXME: Anti-dependency not tracked */
                if prev != cur && self.is_write_locked_.load(Ordering::SeqCst){
                    tx.add_dep(cur, self.last_writer_.get());
                    prev = cur;
                }
            }
        }
    }

    pub fn read(&self, tx: &mut TransactionPar) -> PMutexGuard<V> {
        self.lock(tx)
    }

    pub fn write(&self, tx : &mut TransactionPar) -> PMutexGuard<V> {
        let g = self.lock(tx);
        /* These can be delayed after lock acquired since others'll spin*/
        self.is_write_locked_.store(true, Ordering::SeqCst);
        self.last_writer_.set(tx.txn_info().clone());

        #[cfg(feature = "pmem")]
        {
            let (ptr, layout) = Self::make_record(&g, tx);
            tx.add_record(ptr, layout);
        }

        return g;
    }

    
    #[cfg(feature = "pmem")]
    fn make_record(g : &PMutexGuard<V>, _tx: &TransactionPar) -> (Option<*mut u8>, Layout) {
        let ref_ : &Option<V> = g.data_;
        match ref_.as_ref() {
            None =>  (None, Layout::new::<V>()),
            Some(t)  => {
                let ptr = unsafe {mem::transmute::<&V, *const V>(t)};
                (Some(ptr as *mut u8), Layout::new::<V>())
            }
        }
    }
    
    //FIXME: unlock twice
    pub fn unlock(&self, cur: u32) {
        if self.count_.fetch_sub(1, Ordering::SeqCst) == 1 { /* Last unlock */
            self.lock_.compare_exchange(cur, 0,
                                        Ordering::SeqCst, 
                                        Ordering::Relaxed)
                .expect("lock poisoned");
             
        }
    }
}

impl<V> Default for PValue<V> 
where V: Debug 
{
    fn default() -> Self {
        PValue  {
            data_ : UnsafeCell::new(None),
            last_writer_: ArcCell::new(Arc::new(TxnInfo::default())),
            is_write_locked_: AtomicBool::default(),
            lock_: AtomicU32::new(0),
            count_ : AtomicU8::new(0),
        }
    }
}

impl<V> Debug for PValue<V>
where V: Debug 
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PValue {{ data: {:?}, last_writer_: {:?}, write_locked: {:?} }}", self.data_, self.last_writer_.get(), self.is_write_locked_)
    }
}

unsafe impl<V:Debug> Sync for PValue<V> {}
unsafe impl<V:Debug> Send for PValue<V> {}


#[derive(Debug)]
pub struct PMutexGuard<'mutex,'v:'mutex, V> 
where V: Debug +'v
{
    data_ : &'mutex mut Option<V>,
    val_ : &'v PValue<V>,
    cur_ : u32
}


impl<'mutex, 'v,  V> Drop for PMutexGuard<'mutex,'v, V> 
where V: Debug 
{
    fn drop(&mut self) {
        self.val_.unlock(self.cur_)
    }
}


impl<'mutex, 'v, V> Deref for PMutexGuard<'mutex,'v,  V>
where V: Debug 
{
    type Target = Option<V>;

    fn deref(&self) -> &Option<V> {
        self.data_
    }
}

impl<'mutex,'v,  V> DerefMut for PMutexGuard<'mutex,'v,  V> 
where V: Debug 
{
    fn deref_mut(&mut self) -> &mut Option<V> {
        self.data_
    }
}
