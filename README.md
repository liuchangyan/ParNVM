### Building and Running ### 
1) Install Rust and its other tools with rustup   
`curl https://sh.rustup.rs -sSf | sh`
- Install with the **nightly** version by choosing a customized installation 

2) Go to the pnvm project root and build the project  
`git clone https://github.com/githubxxcc/ParNVM.git`  
`cd ParNVM && git checkout demo`  
`cd pnvm && cargo +nightly build --release --features unstable`  

3) Create the config file and run the application  
`cp Settings.toml.sample Settings.toml`  
`cargo +nightly run --release --features unstable`  

Some Options for running TPCC workload
```
TEST_NAME="TPCC_NVM" | "TPCC_OCC" // For ppnvm or occ contention management 

WH_NUM: warehouse number 
```

Sample output from the program: 
```
/* Running TPCC_OCC */
1, 1, 12389, 0, 5000
//thread_num, wh_num, txn_success, txn_abort, duration in mili second

/* Running TPCC_NVM"
1, 1, 128908, 0, 0 ,0, 5000
//thread_num, wh_num, txn_success, txn_abort, pc_success, pc_abort, duration in mili second

```


### DOC ###
[Algo_Draft](doc/Algo_Draft.md)  



### TIMELINE PLAN ###  
State | MileStone | Task | Deliverable | Expected Time 
--- | --- | --- | ---|--- 
. | :heavy_exclamation_mark: Benchmark Experiment | - | - | 31 July 
:ballot_box_with_check: |  | **Unit test with OCC using TBox** | [Code](https://github.com/githubxxcc/ParNVM/tree/master/pnvm_lib/src) | 1 Week 
:construction: |  | **PMDK library interface** <br /> - [x] How to call C library from Rust <br /> - [x] How to use PMDK library to manage transaction's persistence <br /> - [x] PMDK interface<br /> | Code | 1 Week 
 |  | **Algorithm Draft**  <br /> - [x] First draft of the Algorithm <br /> - [ ]  Second draft | - [Design Doc](https://github.com/githubxxcc/ParNVM/blob/master/doc/Algo_Draft.md)<br /> -  Pseudocode | 1 Week 
 . | | **Implement Offline Chopping** | - Code | 1 Week 
. | . | **Benchmarking using OCC + Slow Persistency v0** | - Datasets | 1 - 2 Weeks 
 |  |  |  |  
. | :heavy_exclamation_mark: Proposed Algorithm Experiment | - | - | 15 August 
. | . | **Benchmarking using our proposed protocol** | - Experiment Graph/Result | 1 - 2 Weeks 
 |  |  |  |  
. | :exclamation: More Experiments | Perform more benchmarking <br /> - 2PL + Strand persistency?<br /> - OCC + Stand persistency? | - | 1 September 
 |  |  |  |  
. | :heavy_exclamation_mark:Paper | Paper | - | 30 Septermber 
. | . | **First Draft** | - | 1 Week 
. | . | **Second Draft** | - | 1 Week 









