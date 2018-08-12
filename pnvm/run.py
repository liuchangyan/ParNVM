#!/usr/bin/env python3
import subprocess
import os
import numpy as np





micro_bench_config = {
        "thread_num" :[4,8,16, 32],
        "pc_num": [10],
        "obj_num" : 20000,
        "set_size" : [30],
        "zipf": np.linspace(0.0001, 1.000, num=10),
        "name": 'OCC',
        "round_num": 1000,
}
out_fd = open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/output.csv"), "w+")

def print_header():
    out_fd.write("thread_num,obj_num,set_size,zipf,pc_num,success,abort,total_time,spin_time,prep_time\n")
    out_fd.flush()

def run():
    print('-------------CONFIG-----------')
    print(micro_bench_config)

    command = ["../target/release/pnvm"]
    env = dict(os.environ)

    for thread_num in micro_bench_config["thread_num"] :
        for (idx, pc_num) in enumerate(micro_bench_config["pc_num"]):
            for zipf in micro_bench_config["zipf"]:
                for set_size in micro_bench_config["set_size"]:
                    obj_num = micro_bench_config["obj_num"]
                    exp_env= {
                            "PNVM_ZIPF_COEFF" : str(zipf),
                            'PNVM_THREAD_NUM' : str(thread_num),
                            'PNVM_OBJ_NUM' : str(obj_num),
                            'PNVM_SET_SIZE' : str(set_size),
                            'PNVM_PC_NUM': str(pc_num),
                            'PNVM_TEST_NAME' : micro_bench_config['name'],
                            'PNVM_ROUND_NUM' : str(micro_bench_config['round_num']),
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




