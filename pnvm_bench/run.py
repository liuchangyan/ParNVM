import subprocess
import os
import numpy as np

def run_exp(env, command, out_fd):
    # print(env)
    for i in range(0,3):
        # subprocess.run(command, shell = True, env=env, stderr=out_fd, stdout=out_fd)
        subprocess.run(command, shell = True, env=env)



def run():
    out_fd = open("output", "a+")
    command = ["../target/release/pnvm_bench"]
    sys_env = dict(os.environ)
    # Test movnv empty
    config_env = {
        "PMEM_NO_FLUSH" : str(1),
    }
    env = {**sys_env, **config_env}
    print("Movnt empy")
    run_exp(env, command, out_fd)

    # Test monvnt clwb
    config_env = {
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clwb")
    run_exp(env, command, out_fd)


    # Test movnt clflushopt
    config_env = {
        "PMEM_NO_CLWB" : str(1),
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clflushopt")
    run_exp(env, command, out_fd)

    # Test movnt clflush
    config_env = {
        "PMEM_NO_CLWB" : str(1),
        "PMEM_NO_CLFLUSHOPT" : str(1),
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clflush")
    run_exp(env, command, out_fd)

    # Test mov empty
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
        "PMEM_NO_FLUSH" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV empty")
    run_exp(env, command, out_fd)

    # Test mov clwb
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clwb")
    run_exp(env, command, out_fd)

    # Test mov clflushop
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
        "PMEM_NO_CLWB" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clflushopt")
    run_exp(env, command, out_fd)

    # Test move clflush
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
        "PMEM_NO_CLWB": str(1),
        "PMEM_NO_CLFLUSHOPT" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clflush")
    run_exp(env, command, out_fd)


run()
