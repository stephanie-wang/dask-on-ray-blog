#!/bin/bash

set -x

run_trial () {
    num_nodes=$1
    num_workers=$(( $num_nodes - 1 ))

    sed 's/{num_workers}/'$num_workers'/g' cluster.yaml.template > cluster.yaml
    ray up -y cluster.yaml

    ray exec cluster.yaml "cd dask-benchmarks && source activate dask-38 && S3_BUCKET=<s3_bucket> AWS_ACCESS_KEY_ID=<access_key_id> AWS_SECRET_ACCESS_KEY=<secret_access_key> bash run_sort_trials.sh" --screen
}

run_trial 1
