
use txn::{Tid};


#[derive(Eq, PartialEq,  Hash, Debug, Clone)]
pub struct Pid(u32);

impl Pid {
    pub fn new(pid: u32) -> Pid {
        Pid(pid)
    }
}


pub struct Piece<F> 
where F : FnMut()-> i32
{
    callback_ : F,
    pid_ : Pid,
    tid_ : Tid,
    //R/W sets?
}


impl<F> Piece<F>
where F : FnMut()->i32 
{
    pub fn new(pid : Pid, tid: Tid, cb : F) -> Piece<F> {
        Piece {
            callback_ : cb,
            pid_: pid,
            tid_ : tid
        }
    }
    

    pub fn run(&mut self) -> i32 {
        (self.callback_)()
    }

    pub fn id(&self) -> Pid {
        self.pid_.clone()
    }
}


pub enum PieceState {
    Ready,
    Running,
    Executed,
    Persisted,
}

