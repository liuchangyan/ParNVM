


//Produced by the chopper along with each transaction
//FIXME::It can actually be shared since it will never 
//be modified afterwards.

use super::piece::Pid;
use std::collections::HashMap;

pub struct Dep
{
    conflicts_ : HashMap<Pid, Vec<ConflictInfo>>
}


impl Dep {
    
    pub fn new() -> Dep {
        Dep {
            conflicts_: HashMap::new()
        }
    }

    pub fn add(&mut self, pid : Pid, clf: ConflictInfo) {
        let conflicts = self.conflicts_
            .entry(pid)
            .or_insert_with( || Vec::new());

        conflicts.push(clf);
    }

    pub fn get_conflict_info(&self, pid : &Pid) -> Option<&Vec<ConflictInfo>> {
        self.conflicts_.get(pid)
    }
}



pub struct ConflictInfo {
    pub txn_name_: String,
    pub piece_id_ : Pid,
    pub conflict_type_: ConflictType,
}

impl ConflictInfo {
    pub fn new(name : String, id: Pid, cfl_type: ConflictType)-> ConflictInfo {
        ConflictInfo{
            txn_name_: name,
            piece_id_: id,
            conflict_type_: cfl_type
        }
    }

}



pub enum ConflictType{
    ReadWrite,
    Write,
}
