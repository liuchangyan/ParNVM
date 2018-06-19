[TOC]

------



:construction_worker: [WORK IN PROGRESS....]

## Offline Chopper

### What is the input of the chopper 

### What does the chopper do 

### What is the output of the chopper

### What is the structure of the transaction and piece 



## Runtime Scheduling 

### Overview 

A per-core scheduler thread is responsible for executing the transactions in the correct order, as well as making the data persistent. Upon receiving a dispatched transaction, the scheduler will execute the transaction's pieces in the program order. 

When executing a particular piece, the scheduler will first have to check with scheulers' from other cores, to 1) find out if there are any conflicting pieces that have already been execting [@a1](#a1)   2) find out if all the conflicting pieces from its dependency sources have been executed. [@a2](#a2)

If there are conflicting pieces running on other cores, the scheduler will have to delay the execution of this piece by placing it into a pending piece buffer, and add that already running transaction as its dependency source. It will then try to execute another piece . The scheduler will simply block when no other pieces can be run from this transaction. [@a3](#a3)

If there are no conflicting pieces running on this thread, the scheduler will execute the piece and marking the piece as running. Undo logging is inserted at the beginning of each piece followed by a `sfence`. The pieces could then be run after the undo logging is persisted. [@a5](#a5)

**[TODO: Transaction structure]**

On finishing executing a piece, the scheduler will mark this piece as finished and run the next piece. 

When all the pieces have been sucessfully executed, the scheduler needs to check if all its dependency sources have been committed. If not, it will have to busy wait for them [@a3](#a3); if yes, the scheduler will compute a TID based on its dependency sources, and persist TID and commit the transaction.[@a6](#a6) [@a4](#a4) 



### Details

#### A1

#### 	How to find out if there are conflicting pieces running?

> Assuming information of all conflicting pieces and their respective transactions are known, a global data structure can be used to keep track of the running pieces from each transaction at each core.  



#### A2

#### 	How to find out if dependency source transactions have already executed the conflicting pieces? 

> A global data structure could allow lookup of transaction and piece' identity can be shared among cores. A scheduler can find out what are pieces that conflicting transactions have ran, and delay its execution according to that information. 
>
> An alternative, more scalable design can rely on static mappings of transactions to cores when transactions are dispatched. Transactions are then partitioned to different cores based on some static information known in advance. In this way, information regarding running pieces could be distributed to cores. 



#### A3

#### 	Can the pieces from another transaction to be ran instead of blocking here?

> Intuitively, since the logging pieces from the next transaction can be run in parrallel with any part of another transaction (<u>proof required</u>), we can save time by avoiding busy waiting. 



#### A4

#### 	How to compute the TID?

> The computation of TID should avoid a single syncrhonization point, and I borrowed the idea of TID generation from Silo. The TID can be computed from the versions of data access by the transactions and the conflicting transactions.  In a nutshell, the TID of a commiting transaction should have the following gaurantee : 
>
> 1. Conflicting transactions (only Write-Write conflicts) should be ordered by their TIDs so that they can be recovered in the correct order. 
> 2. Transactions from different checkpointing epochs should be ordered (<u>TODO: hasn't fully figured out the detailed implementation</u>)



#### A5

#### 	How to do logging?

> - Undo logging has to capture the dependecy between conflicting transactions so that the undo logs to the same location (data object) can be replayed in the correct order when recovery. Only write-write conflicts need to be captured for recovery since read recovery does not concern of read events. However, R-W conflicts still have to be obeyed at runtime. 
> - An extra read/load might be necessary to obtain relevant information to construct an undo log. 
> - There has been research into how to do undo logging: namely, full logging versus incremental logging ([Hiding the long latency of persist barriers using speculative execution](https://dl.acm.org/citation.cfm?id=3080240))
> - An optimization opportunity: 
>   - According to the paper: [Steal but No Force: Efficient Hardware Undo+Redo Logging for Persistent Memory Systems](https://www.researchgate.net/publication/324095506_Steal_but_No_Force_Efficient_Hardware_UndoRedo_Logging_for_Persistent_Memory_Systems). Non-temporal stores can almost always reach NVM faster than stores that go through cache hierachy. Then theoretically, we can use non-temporal stores for undo logs to order the logs and the data mutation without explicit memory fence. 
>
> 
>
> Another option is to go with the **redo logging**: 
>
> When executing a transaction, the first piece will always contain redo loggings (either programmer labelled, or complier generated , as well as an `sfence` at the end of the first piece. It will then continue executing pieces which contain data mutations.
>
> However, redo logging has the issue of handling control flow/data dependencies within a transaction. For example, which arm to run for an `if..else..` block can only be determined at runtime. There a few ways to counter this issue, each comes with its own constrain. 
>
> 1. Disallow any  dynamic determined execution. This imposes a constrain on all the transactions such that stores/persists have to be determined statically prior to running. 
> 2. Use logical logging which enables replay of the entire history at recovery. This will enforce the recovery manager obey Read-Write/ Write-Read ordering between transactions as well. 
> 3. Add undo logging so that undo logs could help recover uncomitted transaction, and redo logs could be dynamcially modified based on the runtime execution. (Of course, this will come at a cost of doubling the NVM traffic)





#### A6

## 	How to commit a transaction?

> Correct recovery requires the final commit of a transaction to be ordered after all the data mutations from the transaction to be visible in NVM. (Otherwise if a crash happens between the commit persist and the pending data persists, the recovery manager will not be able to recover the transaction )
>
> However, there doesn't have to be a `sfence` between the previous transaction's commit and the next transaction since the reordering of them is fine [<u>TODO: proof required as the Peter's Paper seems to state otherwise, in particular the PB before lock in DST strand persistency</u>]



## Checkpointing 











## Notes 

#### Integration of Rust features 

- [Dynamic and manual piece labelling] The idea of ownership might help to define the boundaries of pieces in terms of data dependency. 





## Questions 

- How much information is known at offline chopping. (All transactions known in advance so they can be labelled?)





## TODO:

- [ ] Proof that persistency is enforced with the current logging design.
- [ ] How to do checkpointing effectively. 