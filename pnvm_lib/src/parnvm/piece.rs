
use txn::{Tid};
use std::sync::Arc;
use std::fmt::{
    Formatter,
    Debug,
    Result,
};

#[derive(Eq, PartialEq,  Hash, Debug, Clone, Copy)]
pub struct Pid(u32);

impl Pid {
    pub fn new(pid: u32) -> Pid {
        Pid(pid)
    }
}

type FnPtr = Arc<Box<Fn()->i32 + Send + Sync>>;

#[derive(Clone)]
pub struct Piece
{
    callback_ : FnPtr,
    pid_ : Pid,
    tname_ : String,
    title_ : &'static str,
    //R/W sets?
}

impl Debug for Piece {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "[pid: {:?}, tname: {:?}, name: {:?}]", self.pid_, self.tname_, self.title_)
            
    }
}


impl Piece
{
    pub fn new(pid : Pid, tname: String, cb : FnPtr, title: &'static str) -> Piece {
        Piece {
            callback_ : cb,
            pid_: pid,
            tname_ : tname,
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

