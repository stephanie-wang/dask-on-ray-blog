#!/bin/bash

set -x

num_nodes=$1

run_dask_trial () {
    num_nodes=$1
    nprocs=$2
    nthreads=$3

    # Use Ray to start Dask.
    ray stop -f
    ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml
    python start_dask.py --num-nodes $num_nodes --dask-nprocs $nprocs --dask-nthreads $nthreads --dask-memlimit -1 || exit -1
    # Stop Ray so it doesn't interfere with Dask.
    ray stop -f

    nbytes=1000_000_000
    nconsumers=$(( 100 * $num_nodes ))
    python test_broadcast.py --num-nodes $num_nodes --nbytes $nbytes --nconsumers $nconsumers --dask-nprocs $nprocs --dask-nthreads $nthreads --dask-memlimit -1
    python test_broadcast.py --num-nodes $num_nodes --nbytes $nbytes --nconsumers $nconsumers --dask-nprocs $nprocs --dask-nthreads $nthreads --dask-memlimit -1 --with-gil
}

run_ray_trial() {
    num_nodes=$1

    # Stop Dask so it doesn't interfere with Dask-on-Ray.
    pkill -9 dask
    # Start Dask-on-Ray.
    ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

    nbytes=1000_000_000
    nconsumers=$(( 100 * $num_nodes ))
    python test_broadcast.py --num-nodes $num_nodes --nbytes $nbytes --nconsumers $nconsumers --ray
    python test_broadcast.py --num-nodes $num_nodes --nbytes $nbytes --nconsumers $nconsumers --ray --with-gil
}


run_dask_trial $num_nodes 32 1
run_dask_trial $num_nodes 1 32
run_ray_trial $num_nodes
