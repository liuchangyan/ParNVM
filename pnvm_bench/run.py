import subprocess
import os
import numpy as np


def print_header(out_fd):
    out_fd.write("copy_mode,thd_block_size,nthread,nops,chunk_size,bandwidth,flush_freq\n")
    out_fd.flush()


def run_exp(g_env, mode, command, out_fd):
    # print(env)
        # subprocess.run(command, shell = True, env=env, stderr=out_fd, stdout=out_fd)
    threads = [16, 2, 4, 8, 1]
    nops = 1000000
    chunk_sizes = [512, 64, 256, 1024]
    freq_list = [1,2, 4, 16, 32, 64,128]

    for chunk in chunk_sizes:
        for thread in threads:
            for freq in freq_list:
                bench_env = {
                        "BENCH_THREAD_NUM" : str(thread),
                        "BENCH_CHUNK_SIZE" : str(chunk),
                        "BENCH_OPS_NUM": str(nops),
                        "BENCH_PMDK_MODE" : mode,
                        "BENCH_THD_BLOCK_SIZE": '100000',
                        "BENCH_DEST_MODE": "MmapNVM",
                        "BENCH_FLUSH_FREQ": str(freq),
                        "BENCH_PRINT_HEADER" : 'false',
                }
                env = {**g_env, **bench_env}
                subprocess.run(command, shell = True, env=env, stdout=out_fd)

def run():


    out_fd = open("benchmark/intel-output.csv", "w+")
    print_header(out_fd)
    command = ["../target/release/pnvm_bench"]
    sys_env = dict(os.environ)

    # # Test simple clflush
    # config_env = {
    #     "PMEM_NO_CLWB" : str(1),
    #     "PMEM_NO_CLFLUSHOPT" : str(1),
    #     "PMEM_NO_MOVNT" : str(0),
    # }
    # env = {**sys_env, **config_env}
    # print("simple")
    # run_exp(env, "simple", command, out_fd)

    # Test movnt clflush
    config_env = {
        "PMEM_NO_CLWB" : str(1),
        "PMEM_NO_CLFLUSHOPT" : str(1),
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clflush")
    run_exp(env, "movnt-clflush", command, out_fd)

    # # Test movnv empty
    # config_env = {
    #     "PMEM_NO_FLUSH" : str(1),
    # }
    # print("Movnt empty")
    # env = {**sys_env, **config_env}
    # run_exp(env, "movnt-empty",command, out_fd)

    # Test monvnt clwb
    config_env = {
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clwb")
    run_exp(env, "movnt-clwb",command, out_fd)


    # Test movnt clflushopt
    config_env = {
        "PMEM_NO_CLWB" : str(1),
        "PMEM_NO_MOVNT" : str(0),
    }
    env = {**sys_env, **config_env}
    print("Movnt clflushopt")
    run_exp(env, "movnt-clflushopt",command, out_fd)


    # # Test mov empty
    # config_env = {
    #     "PMEM_NO_MOVNT" : str(1),
    #     "PMEM_NO_FLUSH" : str(1),
    # }
    # env = {**sys_env, **config_env}
    # print("MOV empty")
    # run_exp(env, "mov-empty", command, out_fd)

    # Test mov clwb
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clwb")
    run_exp(env, "mov-clwb", command, out_fd)

    # Test mov clflushop
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
        "PMEM_NO_CLWB" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clflushopt")
    run_exp(env, "mov-clflushopt", command, out_fd)

    # # Test move clflush
    config_env = {
        "PMEM_NO_MOVNT" : str(1),
        "PMEM_NO_CLWB": str(1),
        "PMEM_NO_CLFLUSHOPT" : str(1),
    }
    env = {**sys_env, **config_env}
    print("MOV clflush")
    run_exp(env, "mov-clflush", command, out_fd)


run()
