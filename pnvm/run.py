#!/usr/bin/env python3
import subprocess
import os






micro_bench_config = {
        "thread_num" :[4, 8, 12, 16, 20, 24, 28],
        "obj_num" : [5, 10,15,20],
        "set_size" : [20, 50, 100, 200],
        "zipf": [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
}

print('-------------CONFIG-----------')
print(micro_bench_config)
print()

result_file = open("result", "w+")

command = ["../target/debug/pnvm"]

env = dict(os.environ)
env["RUST_LOG"] = "pnvm=info"

def run_exp(env, out_name):
    with open(out_name, 'w+') as out_fd:
        subprocess.run(command,shell=True, env=env, stderr=out_fd)


# [Deprecated] print results
def process_result(file_name, thread_num, obj_num, set_size, zipf):
    with open(file_name, 'r+') as f:
        text = f.read()
        sucess = text.count("true")
        abort = text.count("false")
        print("{}, {}, {}, {}, {}, {}, {}"
                .format(thread_num, obj_num, set_size, zipf, sucess, abort))



print("thread, obj_num, set_size, zipf, success, abort, time")
for thread_num in micro_bench_config["thread_num"] :
    for (idx, obj_num) in enumerate(micro_bench_config["obj_num"]):
        for zipf in micro_bench_config["zipf"]:
            set_size = micro_bench_config["set_size"][idx]
            exp_env= {
                    "PNVM_ZIPF_COEFF" : str(zipf),
                    'PNVM_THREAD_NUM' : str(thread_num),
                    'PNVM_OBJ_NUM' : str(obj_num),
                    'PNVM_SET_SIZE' : str(set_size),
                    'RUST_LOG' : 'pnvm=info',
                    }

            out_name = "benchmark/out.{}.{}.{}.{}".format(thread_num, obj_num, set_size, zipf)
            sys_env = dict(os.environ)
            env = {**sys_env, **exp_env}
            run_exp(env, out_name)
            # process_result(out_name, thread_num, obj_num, set_size, zipf)





