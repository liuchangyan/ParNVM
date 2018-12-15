#!/usr/bin/env python3
import subprocess
import os
import numpy as np






def print_header(out_fd):
    # out_fd.write("thread_num,obj_num,set_size,zipf,pc_num,success,abort,total_time,new_order\n")
    out_fd.write("thread_num,wh_num,success,abort,pc_success,pc_abort,mmap_cnt,total_time,log_size,flush_size\n")
    out_fd.flush()

def run(bench_config, out_fd):
    print('-------------CONFIG-----------')
    print(bench_config)

    command = ["../target/release/pnvm"]
    env = dict(os.environ)

    # for thread_num in bench_config["thread_num"] :
    #     for (idx, pc_num) in enumerate(bench_config["pc_num"]):
    #         for zipf in bench_config["zipf"]:
    #             for set_size in bench_config["set_size"]:
    #                 obj_num = bench_config["obj_num"]
    #                 exp_env= {
    #                         "PNVM_ZIPF_COEFF" : str(zipf),
    #                         'PNVM_THREAD_NUM' : str(thread_num),
    #                         'PNVM_OBJ_NUM' : str(obj_num),
    #                         'PNVM_SET_SIZE' : str(set_size),
    #                         'PNVM_PC_NUM': str(pc_num),
    #                         'PNVM_TEST_NAME' : bench_config['name'],
    #                         'PNVM_ROUND_NUM' : str(bench_config['round_num']),
    #                         }
    #                 sys_env = dict(os.environ)
    #                 env = {**sys_env, **exp_env}
    #                 run_exp(env, command, out_fd)
    for (idx, thread_num) in enumerate(bench_config["thread_num"]):
        exp_env= {
                'PNVM_THREAD_NUM' : str(thread_num),
                'PNVM_TEST_NAME' : bench_config['name'],
                'PNVM_WH_NUM' : str(bench_config['wh_num'][idx]),
                'PNVM_NO_WARMUP' : str(bench_config['no_warmup']),
                'PNVM_WARMUP_TIME' : str(bench_config['warmup_time']),
                'PNVM_DURATION' : str(bench_config['duration']),
                }
        sys_env = dict(os.environ)
        env = {**sys_env, **exp_env}
        run_exp(env, command, out_fd)



def run_exp(env, command, out_fd):
    #print(env)

    for i in range(0,3):
        os.system("rm -rf ../data/log*")
        subprocess.run(command,shell=True, env=env, stderr=out_fd, stdout=out_fd)



if __name__ == '__main__':
    high_con_wh = [1, 1, 1, 1]
    low_con_wh = [1, 4, 8, 16]
    bench_config = {
            "thread_num" :[1, 4, 8,16],
            #"zipf": np.linspace(0.000001, 1.0, num=10),
            "name": 'TPCC_OCC',
            "wh_num" : [1, 1, 2, 4],
            "duration": 10,
            "no_warmup" : 'false',
            "warmup_time" : 8,
            }

    runs = {
            "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
            "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
            "cont" : [[1, 1, 1, 1], [1, 4, 8, 16]],
            "cont_names": ['high', 'low'],
    }

    # Directly using PMEM
    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog dir"'
    os.system(compile_pmem)


    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-dir-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)


    # With MemCpy
    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem plog"'
    os.system(compile_pmem)


    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)

    # Volatile Memory
    compile_vol = 'cargo clean && cargo +nightly build --release --features unstable'
    os.system(compile_vol)

    for (i, proto) in enumerate(runs["proto"]):
        protocol_name = runs["proto_names"][i]
        bench_config["name"] = proto
        for (j,cont) in enumerate(runs["cont"]):
            bench_config["wh_num"] = cont
            cont_name = runs["cont_names"][j]
            path  = "$PNVM_ROOT/pnvm/benchmark/{}-vol-{}-output.csv".format(cont_name, protocol_name)
            with open(os.path.expandvars(path), "w+") as out_fd:
                print_header(out_fd)
                run(bench_config, out_fd)

    ####################
    # For no conflcit
    ####################
   #  runs = {
   #          "proto" : ['TPCC_OCC', 'TPCC_NVM', 'NO_2PL', 'NO_NVM'],
   #          "proto_names": ['occ', 'ppnvm', 'no-2pl', 'no-ppnvm'],
   #          "cont" : [[1, 4, 8, 16]],
   #          "cont_names": ['noconf'],
   #  }

   #  compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem noconflict"'
   #  os.system(compile_pmem)

   #  # Prevent memory overflow killed
   #  bench_config["duration"] = 10
   #  for (i, proto) in enumerate(runs["proto"]):
   #      protocol_name = runs["proto_names"][i]
   #      bench_config["name"] = proto
   #      for (j,cont) in enumerate(runs["cont"]):
   #          bench_config["wh_num"] = cont
   #          cont_name = runs["cont_names"][j]
   #          path  = "$PNVM_ROOT/pnvm/benchmark/{}-pmem-{}-output.csv".format(cont_name, protocol_name)
   #          with open(os.path.expandvars(path), "w+") as out_fd:
   #              print_header(out_fd)
   #              run(bench_config, out_fd)

   #  compile_vol = 'cargo clean && cargo +nightly build --release --features "unstable noconflict"'
   #  os.system(compile_vol)

   #  for (i, proto) in enumerate(runs["proto"]):
   #      protocol_name = runs["proto_names"][i]
   #      bench_config["name"] = proto
   #      for (j,cont) in enumerate(runs["cont"]):
   #          bench_config["wh_num"] = cont
   #          cont_name = runs["cont_names"][j]
   #          path  = "$PNVM_ROOT/pnvm/benchmark/{}-vol-{}-output.csv".format(cont_name, protocol_name)
   #          with open(os.path.expandvars(path), "w+") as out_fd:
   #              print_header(out_fd)
   #              run(bench_config, out_fd)







