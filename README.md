# dask-on-ray-blog

All benchmarks are run with the Ray cluster launcher on Amazon AWS. The included bash scripts will launch a cluster on EC2 and run the benchmarks in a remote `screen` session. You can check the status of the job by attaching to the screen session with
```bash
ray attach cluster.yaml --screen
```

## Broadcast benchmark
Run:
```bash
bash run_broadcast.sh
```

This script should finish in <20 minutes. The output will be written to `output-broadcast.csv` and can be downloaded with:
```bash
ray rsync-down cluster.yaml /home/ubuntu/dask-benchmarks/output-broadcast.csv .
```

An example output is included at `example-broadcast.csv`.

## Sort benchmark

The sort benchmark reads from S3, so you will first have to set a bucket for the data.
Modify the `run_sort.sh` script to replace `<s3_bucket>` with the bucket name.
You will also have to fill out the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables in the same script with the appropriate values.
Make sure that your AWS profile has the right permissions to access the S3 bucket!

Next, run:
```bash
bash run_sort.sh
```

This script can take several hours to finish completely since some of the larger datasets take some time to complete. To only run some of the trials, modify `run_sort_trials.sh`.

 The output will be written to `output.csv` and can be downloaded with:
```bash
ray rsync-down cluster.yaml /home/ubuntu/dask-benchmarks/output-broadcast.csv .
```

An example output is included at `example-sort.csv`.
