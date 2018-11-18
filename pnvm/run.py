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
    high_con_wh = [1, 1, 2, 4, 8]
    low_con_wh = [1, 4, 8, 16, 30]
    bench_config = {
            "thread_num" :[1, 4, 8,16, 30],
            #"zipf": np.linspace(0.000001, 1.0, num=10),
            "name": 'TPCC_OCC',
            "wh_num" : [1, 1, 2, 4,8],
            "duration": 20,
            "no_warmup" : 'false',
            "warmup_time" : 10,
            }
    compile_pmem = 'cargo clean && PMEM_FILE_DIR=~/ParNVM/data PLOG_FILE_PATH=~/ParNVM/data/log cargo +nightly build --release --features "unstable pmem"'
    os.system(compile_pmem)

    # Run OCC-PMEM High-cont
    bench_config['wh_num'] = high_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/high-pmem-occ-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run OCC-PMEM low-cont
    bench_config['wh_num'] = low_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/low-pmem-occ-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run PPNVM-PMEM High
    bench_config['name'] ='TPCC_NVM'
    bench_config['wh_num'] = high_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/high-pmem-ppnvm-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run PPNVM-PMEM Low
    bench_config['name'] ='TPCC_NVM'
    bench_config['wh_num'] = low_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/low-pmem-ppnvm-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)


    # Recompile for VOL version
    compile_vol = 'cargo clean && cargo +nightly build --release --features unstable'
    os.system(compile_vol)

    # Run OCC-VOL High
    bench_config['name'] ='TPCC_OCC'
    bench_config['wh_num'] = high_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/high-vol-occ-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run OCC-VOL Low
    bench_config['name'] ='TPCC_OCC'
    bench_config['wh_num'] = low_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/low-vol-occ-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run PPNVM-VOL High
    bench_config['name'] ='TPCC_NVM'
    bench_config['wh_num'] = high_con_wh
    bench_config['no_warmup'] = 'true'
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/high-vol-ppnvm-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)

    # Run PPNVM-VOL Low
    bench_config['name'] ='TPCC_NVM'
    bench_config['wh_num'] = low_con_wh
    with open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/low-vol-ppnvm-output.csv"), "w+") as out_fd:
        print_header(out_fd)
        run(bench_config, out_fd)









