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


### Main Project Structure ###

```
pnvm/src/                           // Hold benchmarking related files
    main.rs                         // Functions that run the benchmark
    tpcc/                           // Folder for TPCC workload
        table.rs                    // Table data structures for TPCC
        numeric.rs                  // Numeric data type for TPCC
        entry.rs                    // Entry type for each table
        tpcc_tables.rs              // Tables with queries functions built on top 
                                    // of table.rs
        workload_occ.rs             // Prepare tables for benchmark
        workload_ppnvm.rs           // Piece generation for each txn

pnvm_lib/                           // Library for txn management
    occ/
        map.rs                      // Map data structure for microbenchmark
        occ_txn.rs                  // OCC txn methods
    parnvm/
        nvm_txn_2pl.rs              // Pieces with 2PL as contension management
        nvm_txn_occ.rs              // Pieces with OCC as contention management
        pieces.rs                   // Piece data structure
        map.rs                      // Map data structure for microbenchmark
    plog.rs                         // NVM log data structure
    tbox.rs                         // TBox data structure for microbenchmark
    tcore.rs                        // Tag, Version data structure 
    txn.rs                          // Common data structure related to txn

pnvm_sys/
    lib.rs                          // PMDK interface 

```

### Running with persistent memory ###
1. Check if the machine has persistent memory (or disk simulated non-volatile memory). Follow instructions on [this](http://pmem.io/2016/02/22/pm-emulation.html).`libelf-dev` might need to be installed for this to work

2. Install PMDK (v1.4)  
```
git clone https://github.com/pmem/pmdk.git
cd pmdk && make
sudo make install
```
Some dependencies for pmdk:  
`sudo apt-get install pkg-config autoconf doxygen`

3. Run with pmem features flag  
`PMEM_FILE_DIR=/path/to/pmem/dir cargo +nightly run --release --features "unstable pmem"`

#### Troubleshoot ####
- `error while loading shared libraries`
> Refer to this [issue](https://github.com/rust-lang/rust/issues/24677)
> Try adding `usr/local/lib` to `etc/ld.so.conf` and run `ldconfig`


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









