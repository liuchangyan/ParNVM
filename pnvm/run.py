#!/usr/bin/env python3
import subprocess
import os
import numpy as np





micro_bench_config = {
        "thread_num" :[1, 4, 8,16],
        "pc_num": [3],
        "obj_num" : 50000,
        "set_size" : [5],
        #"zipf": np.linspace(0.000001, 1.0, num=10),
        "zipf" : [0.9],
        "name": 'TPCC_NVM',
        "wh_num" : [1, 4, 8, 16],
        "round_num": 100000,
}
out_fd = open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/output.csv"), "w+")

def print_header():
    # out_fd.write("thread_num,obj_num,set_size,zipf,pc_num,success,abort,total_time,new_order\n")
    out_fd.write("thread_num,wh_num,success,abort,total_time\n")
    out_fd.flush()

def run():
    print('-------------CONFIG-----------')
    print(micro_bench_config)

    command = ["../target/release/pnvm"]
    env = dict(os.environ)

    # for thread_num in micro_bench_config["thread_num"] :
    #     for (idx, pc_num) in enumerate(micro_bench_config["pc_num"]):
    #         for zipf in micro_bench_config["zipf"]:
    #             for set_size in micro_bench_config["set_size"]:
    #                 obj_num = micro_bench_config["obj_num"]
    #                 exp_env= {
    #                         "PNVM_ZIPF_COEFF" : str(zipf),
    #                         'PNVM_THREAD_NUM' : str(thread_num),
    #                         'PNVM_OBJ_NUM' : str(obj_num),
    #                         'PNVM_SET_SIZE' : str(set_size),
    #                         'PNVM_PC_NUM': str(pc_num),
    #                         'PNVM_TEST_NAME' : micro_bench_config['name'],
    #                         'PNVM_ROUND_NUM' : str(micro_bench_config['round_num']),
    #                         }
    #                 sys_env = dict(os.environ)
    #                 env = {**sys_env, **exp_env}
    #                 run_exp(env, command, out_fd)
    for (idx, thread_num) in enumerate(micro_bench_config["thread_num"]):
        obj_num = micro_bench_config["obj_num"]
        exp_env= {
                'PNVM_THREAD_NUM' : str(thread_num),
                'PNVM_TEST_NAME' : micro_bench_config['name'],
                'PNVM_WH_NUM' : str(micro_bench_config['wh_num'][idx]),
                }
        sys_env = dict(os.environ)
        env = {**sys_env, **exp_env}
        run_exp(env, command, out_fd)




def run_exp(env, command, out_fd):
    #print(env)
    for i in range(0,5):
        subprocess.run(command,shell=True, env=env, stderr=out_fd, stdout=out_fd)




print_header()
run()




