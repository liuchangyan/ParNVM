
use txn::{Tid};
use std::fmt::{
    Formatter,
    Debug,
    Result,
};

#[derive(Eq, PartialEq,  Hash, Debug, Clone)]
pub struct Pid(u32);

impl Pid {
    pub fn new(pid: u32) -> Pid {
        Pid(pid)
    }
}

pub struct Piece
{
    callback_ : Box<FnMut()->i32 >,
    pid_ : Pid,
    tid_ : Tid,
    title_ : &'static str,
    //R/W sets?
}

impl Debug for Piece {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "[pid: {:?}, tid: {:?}, name: {:?}]", self.pid_, self.tid_, self.title_)
            
    }
}


impl Piece
{
    pub fn new(pid : Pid, tid: Tid, cb : Box<FnMut()->i32>, title: &'static str) -> Piece {
        Piece {
            callback_ : cb,
            pid_: pid,
            tid_ : tid,
            title_ : title
        }
    }
    

    pub fn run(&mut self) -> i32 {
        (self.callback_)()
    }

    pub fn id(&self) -> &Pid {
        &self.pid_
    }
}

#[derive(Debug,Copy, Clone)]
pub enum PieceState {
    Ready,
    Running,
    Executed,
    Persisted,
}

