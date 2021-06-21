import ray
import subprocess
import time
import socket


@ray.remote
def start_dask(scheduler_ip, nprocs, nthreads, memlimit):
    p = subprocess.Popen(['pkill', '-9', 'dask-worker'])
    p.wait()
    cmd = ['dask-worker', f'{scheduler_ip}:8786', '--nprocs', str(nprocs), '--nthreads', str(nthreads), '--local-directory', '/data0']
    if memlimit != '-1' and memlimit != -1:
        cmd += ['--memory-limit', str(memlimit)]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--num-nodes", type=int, default=1)
    parser.add_argument("--dask-nprocs", type=int, required=True)
    parser.add_argument("--dask-nthreads", type=int, required=True)
    parser.add_argument("--dask-memlimit", type=str, required=True)
    args = parser.parse_args()

    ray.init(address='auto')

    p = subprocess.Popen(['pkill', '-9', 'dask-scheduler'])
    p.wait()
    subprocess.Popen(['dask-scheduler'])

    node_resources = []
    while len(node_resources) < args.num_nodes:
        time.sleep(3)
        resources = ray.available_resources()
        node_resources = [resource for resource in resources if 'node' in resource]
    print("Found nodes", node_resources)

    head_ip = socket.gethostbyname(socket.gethostname())
    tasks = []
    for node_resource in node_resources:
        tasks.append(start_dask.options(resources={node_resource: 1}).remote(
            head_ip, args.dask_nprocs, args.dask_nthreads, args.dask_memlimit))
    ray.get(tasks)

    time.sleep(5)
    from dask.distributed import Client
    c = Client('localhost:8786')
    nthreads_total = 0
    for nthreads in c.nthreads().values():
        nthreads_total += nthreads
    print("Found", nthreads_total, "threads")
    assert nthreads_total == args.num_nodes * args.dask_nthreads * args.dask_nprocs
