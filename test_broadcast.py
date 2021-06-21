import dask
import dask.dataframe as dd
import json
import pandas as pd
import numpy as np
import os.path
import csv

from dask.distributed import Client
from dask.distributed import wait

import boto3
import traceback
import time


@dask.delayed
def generate(nbytes):
    return np.zeros(nbytes, dtype=np.uint8)

@dask.delayed
def task_no_gil(array):
    return array.sum()

@dask.delayed
def task_with_gil(array):
    for _ in range(10000):
        np.random.choice(array)

def trial(nbytes, num_consumers, max_trials, with_gil):
    times = []
    start = time.time()
    for i in range(max_trials):
        x = generate(nbytes)
        if with_gil:
            fn = task_with_gil
        else:
            fn = task_no_gil
        results = [fn(x) for _ in range(num_consumers)]

        print("Trial {} start".format(i))
        trial_start = time.time()
        #dask.visualize(results, filename='dask.svg')
        dask.compute(results)

        trial_end = time.time()
        duration = trial_end - trial_start
        times.append(duration)
        print("Trial {} done after {}".format(i, duration))

        if time.time() - start > 60:
            break
    return times


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--nbytes", type=int, default=1_000_000)
    parser.add_argument("--nconsumers", type=int, default=1_000)
    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--max-trials", type=int, default=3)
    parser.add_argument("--with-gil", action="store_true")

    parser.add_argument("--dask-nprocs", type=int, default=-1)
    parser.add_argument("--dask-nthreads", type=int, default=-1)
    parser.add_argument("--dask-memlimit", type=str, default=-1)
    parser.add_argument("--ray", action="store_true")
    args = parser.parse_args()

    if args.ray:
        import ray
        ray.init(address='auto')
        from ray.util.dask import ray_dask_get
        dask.config.set(scheduler=ray_dask_get)

        node_resources = []
        while len(node_resources) < args.num_nodes:
            time.sleep(3)
            resources = ray.available_resources()
            node_resources = [resource for resource in resources if 'node' in resource]
    else:
        client = Client("localhost:8786")
        nthreads_total = 0
        for nthreads in client.nthreads().values():
            nthreads_total += nthreads
        nthreads_expected = args.num_nodes * args.dask_nprocs * args.dask_nthreads
        assert nthreads_total == nthreads_expected, f'Found {nthreads_total}, expected {nthreads_expected}'

    # Warmup.
    output = trial(1_000_000, args.nconsumers, 1, args.with_gil)[0]
    print(f"Finished warmup in {output}")

    try:
        output = trial(args.nbytes, args.nconsumers, args.max_trials, args.with_gil)
        print("mean over {} trials: {} +- {}".format(len(output), np.mean(output), np.std(output)))
    except Exception as e:
        output = "x"
        print(traceback.format_exc())

    output_file = "output-broadcast.csv"
    write_header = not os.path.exists(output_file) or os.path.getsize(output_file) == 0
    with open(output_file, "a+") as csvfile:
        fieldnames = ["gil", "num_nodes", "nbytes", "nconsumers", "ray", "dask_nprocs", "dask_nthreads", "dask_memlimit", "duration"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
                "gil": args.with_gil,
                "num_nodes": args.num_nodes,
                "nbytes": args.nbytes,
                "nconsumers": args.nconsumers,
                "ray": args.ray,
                "dask_nprocs": args.dask_nprocs,
                "dask_nthreads": args.dask_nthreads,
                "dask_memlimit": args.dask_memlimit,
                }
        for output in output:
            row["duration"] = output
            writer.writerow(row)
