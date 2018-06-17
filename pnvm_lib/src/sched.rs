extern crate num_cpus;

use std::thread;
use std::sync::Arc;
pub struct Scheduler<T>
where
T: Fn() + Send + Sync + 'static
{
    nthreads_:       u32,
    task_:           &'static T,
}

impl<T> Scheduler<T>
where
T: Fn() + Send + Sync + 'static
{
    pub fn new(nthreads_: u32, task_: &'static T) -> Scheduler<T> {
        Scheduler {
            nthreads_,
            task_,
        }
    }

    pub fn run(&self) {
        let cpu_num = num_cpus::get();
        println!("Number of CPUs Available: {}", cpu_num);

        let task = Arc::new(self.task_);

        let mut handles = vec![];
        for i in 0..self.nthreads_ {
            let task = task.clone();
            handles.push(thread::spawn(move || (task)()));
        }


        for handle in handles {
            handle.join().unwrap();
        }

        println!("All done");
    }   
}
