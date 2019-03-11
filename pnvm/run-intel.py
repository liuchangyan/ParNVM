#!/usr/bin/env python3
import subprocess
import os
import numpy as np





def print_header_ycsb(out_fd):
    # out_fd.write("thread_num,obj_num,set_size,zipf,pc_num,success,abort,total_time,new_order\n")
    out_fd.write("thread_num,zipf,rw_ratio,txn_num_ops,ops_per_iter,mode,success,abort,pc_success,pc_abort,mmap_cnt,total_time,log_size,flush_size\n")
    out_fd.flush()

def print_header(out_fd):
    # out_fd.write("thread_num,obj_num,set_size,zipf,pc_num,success,abort,total_time,new_order\n")
    out_fd.write("thread_num,wh_num,success,abort,pc_success,pc_abort,mmap_cnt,total_time,log_size,flush_size\n")
    out_fd.flush()

def run(bench_config, out_fd):
    print('-------------CONFIG-----------')
    print(bench_config)

    command = ["../target/release/pnvm"]
    env = dict(os.environ)
    for (idx, thread_num) in enumerate(bench_config["thread_num"]):
        exp_env= {
                'PNVM_THREAD_NUM' : str(thread_num),
                'PNVM_TEST_NAME' : bench_config['name'],
                'PNVM_WH_NUM' : str(bench_config['wh_num'][idx]),
                'PNVM_NO_WARMUP' : str(bench_config['no_warmup']),
                'PNVM_WARMUP_TIME' : str(bench_config['warmup_time']),
                'PNVM_DURATION' : str(bench_config['duration']),
                'PNVM_PARTITION' : str(bench_config['partition']),
                }
        sys_env = dict(os.environ)
        env = {**sys_env, **exp_env}
        run_exp(env, command, out_fd)


def run_ycsb(bench_config, out_fd):
    print('-------------CONFIG-----------')
    print(bench_config)

    command = ["../target/release/pnvm"]
    env = dict(os.environ)
    for (thd_i, thread_num) in enumerate(bench_config["thread_num"]):
        print("thread {}".format(thread_num))
        for(ratio_i , rw_ratio) in enumerate(bench_config["ycsb_rw_ratio"]):
            print("rw_ratio {}".format(rw_ratio))
            for(zipf_i, zipf) in enumerate(bench_config["zipf"]):
                print("zipf {}".format(zipf))
                exp_env= {
                        'PNVM_THREAD_NUM' : str(thread_num),
                        'PNVM_TEST_NAME' : bench_config['name'],
                        'PNVM_NO_WARMUP' : str(bench_config['no_warmup']),
                        'PNVM_WARMUP_TIME' : str(bench_config['warmup_time']),
                        'PNVM_DURATION' : str(bench_config['duration']),
                        'PNVM_YCSB_NUM_ROWS' : str(bench_config['ycsb_row']),
                        'PNVM_YCSB_SAMPLER': str(bench_config['ycsb_sampler']),
                        'PNVM_ZIPF_COEFF': str(zipf),
                        'PNVM_YCSB_RW_MODE': str(bench_config['ycsb_rw_mode']),
                        'PNVM_YCSB_RW_RATIO': str(rw_ratio),
                        'PNVM_YCSB_TXN_NUM_OPS': str(bench_config['ycsb_txn_num_ops']),
                        'PNVM_YCSB_OPS_CNT': str(bench_config['ycsb_ops_cnt']),
                        }
                sys_env = dict(os.environ)
                env = {**sys_env, **exp_env}
                run_exp(env, command, out_fd)

def run_exp(env, command, out_fd):
    #print(env)

    for i in range(0,3):
        os.system("rm -rf ../data/log*")
        #subprocess.run(command,shell=True, env=env, stderr=out_fd, stdout=out_fd)
        subprocess.run(command,shell=True, env=env)



def do_vol_no_partition(bench_config, runs):
    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1,1,1, 1, 1]],
            "cont_names": ['extreme'],
    }
    compile_vol = 'cargo clean && cargo +nightly build --release --features unstable'
    os.system(compile_vol)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        bench_config["partition"] = 1
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-vol-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)


def do_vol_rel(bench_config, runs):
    # Volatile Memory
    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1, 1,1], [1, 4, 8, 16, 32,48]],
            "cont_names": ['high', 'low'],
    }
    compile_vol = 'cargo clean && cargo +nightly build --release --features unstable'
    os.system(compile_vol)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        bench_config["partition"] = 0
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-vol-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)


def do_pmem_rel(bench_config):
    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1, 1,1], [1, 4, 8, 16, 32,48]],
            "cont_names": ['high', 'low'],
    }

    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog"'
    os.system(compile_pmem)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        bench_config["partition"] = 0
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)

def do_pmem_dir(bench_config, runs):
    # Directly using PMEM
    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog dir"'
    os.system(compile_pmem)

    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1, 1,1], [1, 4, 8, 16, 32,48]],
            "cont_names": ['high', 'low'],
    }

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        bench_config["partition"] = 0
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-dir-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)

def do_pmem_no_partition(bench_config, runs):
    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1,1,1]],
            "cont_names": ['extreme'],
    }

    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog"'
    os.system(compile_pmem)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        bench_config["partition"] = 1
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)


def do_pmem_pdrain(bench_config):
    runs = {
            "proto" : ['TPCC_NVM',  'NO_NVM'],
            "proto_names": ['ppnvm',  'no-ppnvm'],
            "cont" : [[1, 1, 1, 1, 1,1], [1, 4, 8, 16, 32,48]],
            "cont_names": ['high', 'low'],
    }
    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog pdrain"'
    os.system(compile_pmem)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-pd-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)

def do_pmem_drain_freq(bench_config, runs, partition):
    pdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog pdrain dir"'
    wdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog wdrain dir"'
    tdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog dir"'
    runs = {
            "proto" : ['TPCC_NVM'],
            "proto_names": ['ppnvm'],
            "cont" : [[1, 1, 1, 1,1,1], [1,4,8,16,32,48]],
            "cont_names": ['high', 'low'],
            "cmd": [pdrain_cmd, wdrain_cmd, tdrain_cmd],
            "drain_freq": ["pdrain", "wdrain", "tdrain"],
    }

    # os.system(compile_pmem)

    for (i, cmd) in enumerate(runs["cmd"]):
        # print(cmd)
        os.system(cmd)
        drain_freq = runs["drain_freq"][i]
        bench_config["name"] ="TPCC_NVM"
        bench_config["partition"] =partition
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-{}-{}par-output.csv".format(cont_name, drain_freq, partition)
            with open(os.path.expandvars(path), "w+") as out_fd:
                # print(bench_config)
                # print(out_fd)
                # print("\n")
                print_header(out_fd)
                run(bench_config, out_fd)

def do_pmem_ycsb(bench_config, runs):
    # pdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog pdrain dir"'
    # wdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog wdrain dir"'
    # tdrain_cmd = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog dir"'
    pdrain_cmd = 'PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog pdrain dir"'
    wdrain_cmd = 'PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog wdrain dir"'
    tdrain_cmd = 'PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog dir"'

    runs = {
            "proto" : ['YCSB'],
            "proto_names": ['occ'],
            "thread_num" : [16],
            "cont_names": '16thd',
            #"thread_num" : [48],
            #"cont_names": ['48thd'],
            "zipf": np.linspace(0.000001, 1.0, num=10),
            "cmd": [pdrain_cmd, wdrain_cmd, tdrain_cmd],
            "drain_freq": ["pdrain", "wdrain", "tdrain"],
    }

    # os.system(compile_pmem)

    for (i, cmd) in enumerate(runs["cmd"]):
        # print(cmd)
        os.system(cmd)
        drain_freq = runs["drain_freq"][i]
        bench_config["name"] ="YCSB_OCC"
        bench_config["thread_num"] = runs["thread_num"]
        bench_config["zipf"] = runs["zipf"]
        cont_name = runs["cont_names"]
        path  = "$PNVM_ROOT/pnvm/benchmark/ycsb-{}-pmem-{}-output.csv".format(cont_name, drain_freq)
        with open(os.path.expandvars(path), "w+") as out_fd:
            print_header_ycsb(out_fd)
            run_ycsb(bench_config, out_fd)


if __name__ == '__main__':
    # For TPCC
    bench_config = {
            "thread_num" :[1, 4, 8,16, 32, 48],
            #"zipf": np.linspace(0.000001, 1.0, num=10),
            "name": 'TPCC_OCC',
            "wh_num" : [1, 1, 2, 4],
            "duration": 10,
            "no_warmup" : 'false',
            "warmup_time" : 8,
            "partition" : 0,
            }
    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1, 1,1], [1, 4, 8, 16, 32,48]],
            "cont_names": ['high', 'low'],
    }

    # For YSBS
    ycsb_bench_config = {
            "thread_num" :[1, 4, 8,16, 32, 48],
            #"zipf": np.linspace(0.000001, 1.0, num=10),
            "name": 'YCSB_OCC',
            "wh_num" : [1, 1, 2, 4],
            "ycsb_row": 1000000,
            "ycsb_sampler" : "Zipf",
            "ycsb_rw_mode": "Random",
            "ycsb_rw_ratio": [0.5],
            "ycsb_txn_num_ops": 20,
            "ycsb_ops_cnt": 1000000,
            "duration": 10,
            "no_warmup" : 'false',
            "warmup_time" : 8,
    }

    runs = {
            "proto": ['YCSB_OCC', 'YCSB_PP'],
            "proto_names" : ['occ, pp'],
            "zipf": np.linspace(0.000001, 1.0, num=10),
    }

    do_pmem_ycsb(ycsb_bench_config, runs)


    # With paritions
    # do_pmem_rel(bench_config)
    # do_pmem_pdrain(bench_config)
    # do_pmem_dir(bench_config, runs)
    # do_pmem_no_partition(bench_config, runs)

    # do_vol_rel(bench_config,runs)
    # do_vol_no_partition(bench_config, runs)

    # do_pmem_drain_freq(bench_config, runs, 0)
    # without paritions
    #do_pmem_drain_freq(bench_config, runs, 1)









