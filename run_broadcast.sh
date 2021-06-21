#!/bin/bash

set -x

run_trial () {
    num_nodes=$1
    num_workers=$(( $num_nodes - 1 ))

    sed 's/{num_workers}/'$num_workers'/g' cluster.yaml.template > cluster.yaml
    ray up -y cluster.yaml

    ray exec cluster.yaml "cd dask-benchmarks && bash run_broadcast_trials.sh $num_nodes" --screen
}

run_trial 10
run_trial 5
run_trial 1
