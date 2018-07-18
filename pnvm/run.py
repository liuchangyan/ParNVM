#!/usr/bin/env python3
import subprocess
import os
import numpy as np





micro_bench_config = {
        "thread_num" :np.linspace(4, 28, num=4, dtype=np.int16),
        "obj_num" : [10],
        "set_size" : [2000],
        "zipf": np.linspace(0.050, 1.000, num=100),
}

def run():
    out_fd = open(os.path.expandvars("$PNVM_ROOT/pnvm/benchmark/output.csv"), "w+")
    print('-------------CONFIG-----------')
    print(micro_bench_config)

    command = ["../target/debug/pnvm"]
    env = dict(os.environ)

    for thread_num in micro_bench_config["thread_num"] :
        for (idx, obj_num) in enumerate(micro_bench_config["obj_num"]):
            for zipf in micro_bench_config["zipf"]:
                set_size = micro_bench_config["set_size"][idx]
                exp_env= {
                        "PNVM_ZIPF_COEFF" : str(zipf),
                        'PNVM_THREAD_NUM' : str(thread_num),
                        'PNVM_OBJ_NUM' : str(obj_num),
                        'PNVM_SET_SIZE' : str(set_size),
                        'PNVM_USE_PMEM' : 'true',
                        }

                # out_name = "benchmark/out.{}.{}.{}.{}".format(thread_num, obj_num, set_size, zipf)
                sys_env = dict(os.environ)
                env = {**sys_env, **exp_env}
                run_exp(env, command, out_fd)
                # process_result(out_name, thread_num, obj_num, set_size, zipf)

    print("thread,obj_num,set_size,zipf,success,abort,time, pmem", file=out_fd)



def run_exp(env, command, out_fd):
    #print(env)
    for i in range(0,5):
        subprocess.run(command,shell=True, env=env, stderr=out_fd, stdout=out_fd)


# [Deprecated] print results
def process_result(file_name, thread_num, obj_num, set_size, zipf):
    with open(file_name, 'r+') as f:
        text = f.read()
        sucess = text.count("true")
        abort = text.count("false")
        print("{}, {}, {}, {}, {}, {}, {}"
                .format(thread_num, obj_num, set_size, zipf, sucess, abort))



run()




