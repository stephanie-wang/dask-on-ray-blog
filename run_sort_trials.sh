#!/bin/bash

set -x


run_dask_trial () {
    num_nodes=$1
    nprocs=$2
    nthreads=$3
    memlimit=$4

    nbytes=$5
    npartitions=$6

    pkill -9 dask
    ray stop -f
    # Use Ray to start Dask.
    ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml
    python start_dask.py --num-nodes $num_nodes --dask-nprocs $nprocs --dask-nthreads $nthreads --dask-memlimit $memlimit || exit -1
    # Stop Ray so it doesn't interfere with Dask.
    ray stop -f

    python test_sort.py --num-nodes $num_nodes --nbytes $nbytes --npartitions $npartitions --dask-nprocs $nprocs --dask-nthreads $nthreads --dask-memlimit $memlimit --data-dir /data0 --s3-bucket $S3_BUCKET --skip-existing
}

run_ray_trial() {
    num_nodes=$1
    nbytes=$2
    npartitions=$3

    # Stop Dask so it doesn't interfere with Dask-on-Ray.
    pkill -9 dask
    ray stop -f
    # Start Dask-on-Ray.
    # We use 50GB object store and the RAY_plasma_unlimited flag due to a known
    # issue with memory fragmentation at larger object store sizes. This should
    # be fixed by Ray v1.5.
    # Follow https://github.com/ray-project/ray/issues/14182 for more info.
    RAY_plasma_unlimited=0 ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config ~/ray_bootstrap_config.yaml --system-config='{"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":[\"/data0/spill\",\"/data1/spill\",\"/data2/spill\",\"/data3/spill\"]}}"}' --object-store-memory 50000000000

    python test_sort.py --num-nodes $num_nodes --nbytes $nbytes --npartitions $npartitions --ray --data-dir /data0 --s3-bucket $S3_BUCKET --skip-existing
}

# Usage:
#   run_dask_trial <num nodes> <nprocs> <nthreads> <memory limit per worker> <nbytes> <npartitions>
#   run_ray_trial <num nodes> <nbytes> <npartitions>

# 10GB, nprocs vs nthreads in Dask.
run_dask_trial 1 1 32 -1 10_000_000_000 100
run_dask_trial 1 32 1 -1 10_000_000_000 100
run_ray_trial 1 10_000_000_000 100

# 20GB, effect of npartitions.
run_dask_trial 1 32 1 -1 20_000_000_000 100
run_dask_trial 1 32 1 -1 20_000_000_000 200
run_dask_trial 1 32 1 -1 20_000_000_000 500
run_ray_trial 1 20_000_000_000 100
run_ray_trial 1 20_000_000_000 200
run_ray_trial 1 20_000_000_000 500

# Scaling dataset size up to 100GB, keeping npartitions constant. Show effect
# of nprocs in Dask.
for nbytes in 1_000_000_000 10_000_000_000 20_000_000_000 100_000_000_000; do
    run_dask_trial 1 32 1 -1 $nbytes 100
    run_dask_trial 1 8 4 30_000_000_000 $nbytes 100
    run_dask_trial 1 1 32 240_000_000_000 $nbytes 100
    run_ray_trial 1 $nbytes 100
done
