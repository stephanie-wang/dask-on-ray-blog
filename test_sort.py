import dask
import dask.dataframe as dd
import json
import pandas as pd
import numpy as np
import os.path
import csv
import boto3
import traceback
import sys

from dask.distributed import Client
from dask.distributed import wait

import time


def load_dataset(client, data_dir, s3_bucket, nbytes, npartitions):
    num_bytes_per_partition = nbytes // npartitions
    filenames = []

    @dask.delayed
    def generate_s3_file(i, data_dir, s3_bucket):
        s3 = boto3.Session().client('s3')
        key = "df-{}-{}.parquet.gzip".format(num_bytes_per_partition, i)
        contents = s3.list_objects(Bucket=s3_bucket, Prefix=key)
        for obj in contents.get('Contents', []):
            if obj['Key'] == key:
                print(f"S3 partition {i} exists")
                return

        filename = os.path.join(data_dir, key)
        if not os.path.exists(filename):
            print("Generating partition", filename)
            nrows = num_bytes_per_partition // 8
            dataset = pd.DataFrame(np.random.randint(0, np.iinfo(np.int64).max, size=(nrows, 1), dtype=np.int64), columns=['a'])
            dataset.to_parquet(filename, compression='gzip')
        print("Writing partition to S3", filename)
        with open(filename, 'rb') as f:
            s3.put_object(Bucket=s3_bucket, Key=key, Body=f)

    #for i in range(npartitions):
    #    filenames.append(foo(i, data_dir))
    #filenames = dask.compute(filenames)[0]

    x = []
    for i in range(npartitions):
        x.append(generate_s3_file(i, data_dir, s3_bucket))
    dask.compute(x)


    #filenames = []
    #for i in range(npartitions):
    #    filename = "df-{}-{}.parquet.gzip".format(num_bytes_per_partition, i)
    #    filenames.append(f"s3://dask-on-ray-data/{filename}")

    filenames = [f's3://{s3_bucket}/df-{num_bytes_per_partition}-{i}.parquet.gzip' for i in range(npartitions)]
    df = dd.read_parquet(filenames)
    return df



def trial(client, data_dir, s3_bucket, nbytes, n_partitions, generate_only):
    df = load_dataset(client, data_dir, s3_bucket, nbytes, n_partitions)

    if generate_only:
        return

    times = []
    start = time.time()
    for i in range(10):
        print("Trial {} start".format(i))
        trial_start = time.time()

        if client is None:
            print(df.set_index('a', shuffle='tasks', max_branch=float('inf')).head(10, npartitions=-1))
        else:
            print(df.set_index('a').head(10, npartitions=-1))

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
    parser.add_argument("--npartitions", type=int, default=100, required=False)
    # Max partition size is 1GB.
    parser.add_argument("--max-partition-size", type=int, default=1000_000_000, required=False)
    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--ray", action="store_true")
    parser.add_argument("--data-dir", default="/home/ubuntu/dask-benchmarks")
    parser.add_argument("--s3-bucket", default="dask-on-ray-data")

    parser.add_argument("--dask-nprocs", type=int, default=0)
    parser.add_argument("--dask-nthreads", type=int, default=0)
    parser.add_argument("--dask-memlimit", type=int, default=0)
    args = parser.parse_args()

    npartitions = args.npartitions
    if args.nbytes // npartitions > args.max_partition_size:
        npartitions = args.nbytes // args.max_partition_size

    row = {
            "num_nodes": args.num_nodes,
            "nbytes": args.nbytes,
            "npartitions": npartitions,
            "dask_nprocs": args.dask_nprocs,
            "dask_nthreads": args.dask_nthreads,
            "dask_memlimit": args.dask_memlimit,
            }
    if args.skip_existing and os.path.exists("output.csv"):
        with open("output.csv", "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for done_row in reader:
                duplicate = True
                for field in row:
                    if int(row[field]) != int(done_row[field]):
                        duplicate = False
                if duplicate:
                    print("Already found row with spec", row)
                    sys.exit(0)
    print("Running", row)

    if args.ray:
        import ray
        ray.init(address='auto')
        from ray.util.dask import ray_dask_get, dataframe_optimize
        dask.config.set(scheduler=ray_dask_get, dataframe_optimize=dataframe_optimize)
        client = None
    else:
        assert args.dask_nprocs != -0
        assert args.dask_nthreads != -0
        assert args.dask_memlimit != -0

        client = Client('localhost:8786')

    print(trial(client, args.data_dir, args.s3_bucket, 1000, 10, args.generate_only))
    print("WARMUP DONE")

    try:
        output = trial(client, args.data_dir, args.s3_bucket, args.nbytes, npartitions, args.generate_only)
        print("mean over {} trials: {} +- {}".format(len(output), np.mean(output), np.std(output)))
    except Exception as e:
        output = "x"
        print(traceback.format_exc())

    write_header = not os.path.exists("output.csv") or os.path.getsize("output.csv") == 0
    with open("output.csv", "a+") as csvfile:
        fieldnames = ["num_nodes", "nbytes", "npartitions", "dask_nprocs", "dask_nthreads", "dask_memlimit", "duration"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for output in output:
            row["duration"] = output
            writer.writerow(row)
