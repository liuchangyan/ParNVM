use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

pub struct Dep {
    btree_ : Mutex<BTreeMap<Arc<String>, u32>>
}

impl Dep {
    pub fn new() -> Dep {
        Dep{
            btree_: Mutex::new(BTreeMap::new())
        }
    }

    pub fn add(&mut self, key: String, val:u32) -> Option<u32> {
        let key = Arc::new(key);
       // let mx = Rc::new(RefCell::new(self.btree_.lock().unwrap()));
       // let mx2 = Rc::clone(&mx);
       // if let Some(cur_ver) = mx2.borrow_mut().get_mut(&key) {
       //     *cur_ver += 1;
       //     Some(*cur_ver) 
       // } else {
       //     mx.borrow_mut().insert(key, val)
       // }
       self.btree_.lock().unwrap().insert(key, val)
    }

    pub fn remove(&mut self, key_ref: &Arc<String>) -> Option<u32> {
        self.btree_.lock().unwrap().remove(key_ref)
    }

    pub fn contains(&mut self, key: String) -> bool {
        self.btree_.lock().unwrap().contains_key(&Arc::new(key))
    }
}


