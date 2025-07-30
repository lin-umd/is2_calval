#!/usr/bin/env python
# coding: utf-8

import glob,os
from dask import delayed, compute
from dask.distributed import Client
from dask.diagnostics import ProgressBar
out_p="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las_reprocess/"
@delayed
def process_file(f, out_p):
    # Remove noise, points 100-500m
    out_f = out_p + os.path.basename(f)
    cmd1 = f"las2las64 -i {f} -keep_z 250 450 -o {out_f} -v"
    os.system(cmd1)
    
    cmd2 = f"lasground_new64 -i {out_f} -o {f} -v"
    os.system(cmd2)
    print('overwrite old las file:', f)

if __name__ == '__main__': 
    client = Client(n_workers=10, threads_per_worker=1)
    files=glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las/usa/neon_clbj2022/*.las')
    tasks = [process_file(f, out_p) for f in files]
    with ProgressBar():
        compute(*tasks)
    client.close()

