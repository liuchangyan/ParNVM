
use table::{Key, Table};


pub struct Warehouse {
    pub w_id: i32,
    pub w_name: String,
}

impl Key<i32> for Warehouse {
    fn primary_key(&self) -> i32 {
        self.w_id
    }
}
