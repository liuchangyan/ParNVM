






pub struct LTable<Entry>
where Entry : Clone + Debug
{
    buckets_ : Vec<LBucket<Entry>>,
    bucket_num_ : usize,
    name: String,
}


impl<Entry> LTable<Entry>
where Entry: Clone + Debug
{
    pub fn new_with_buckets(num: usize, name: &str)
        -> LTable<Entry>
        {
            let mut buckets= Vec::with_capacity(num);

            for _ in 0..num {
                buckets.push(LBucket::new());
            }

            LTable {
                buckets_: buckets,
                bucket_num_: num,
                name_: String::from(name),
            }
        }
}
