#!/usr/bin/env python
# coding: utf-8

import laspy
import glob,os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def get_center_profie(f, output):
    las = laspy.read(f)
    point_format = las.point_format
    list(point_format.dimension_names)
    x1 = las.header.mins[0] ; x2 = las.header.maxs[0]
    y1 = (las.header.maxs[1] + las.header.mins[1] )/2 - 5 
    y2 = (las.header.maxs[1] + las.header.mins[1] )/2 + 5
    X_valid = (x1 < las.x) & (x2 > las.x)
    Y_valid = (y1 < las.y) & (y2 > las.y)
    good_indices = np.where(X_valid & Y_valid )
    x = las.x[good_indices]
    y = las.y[good_indices]
    z = las.z[good_indices]
    c = las.classification[good_indices]

    # Or stack as (N, 4) array
    xyz = np.vstack((x, y, z, c)).T


    df = pd.DataFrame({'x': xyz[:,0], 'y': xyz[:,1], 'z': xyz[:,2], 'c': xyz[:,3]})



    plt.figure(figsize=(10, 6))
    # color by classification, use decrete colormap
    sc = plt.scatter(df['x'], df['z'], c=df['c'], cmap='tab10', s=1, alpha=0.7)
    plt.xlabel('X')
    plt.ylabel('Z (Height)')
    plt.title('X vs. Z colored by classification')
    # use integer colorbar
    plt.colorbar(sc, ticks=np.arange(0, 10), label='Class')
    plt.grid(True)
    plt.tight_layout()
    #plt.show()
    plt.savefig(f"{output}/{f.split('/')[-3]}_{f.split('/')[-2]}_{f.split('/')[-1].replace('.las', '.png')}", dpi=300)
    plt.close()



folders = glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las/*/*/')
output = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/profile/clbj2022"
# get proect names for files
names= [] 
regions =[]
for f in folders:
    names.append(f.split('/')[-2])
    regions.append(f.split('/')[-3])

for r, p in zip(regions, names): 
    if p == 'neon_clbj2022':
        files = glob.glob(f'/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las/{r}/{p}/*.las')
        if True:
            for f in files:
                if 'NEON_D11_CLBJ_DP1_L027-1_2022050613_unclassified_point_cloud_0000013.las' in f:
                    print(f)
                    get_center_profie(f, output)

        if False:
            # random pick a file 
            if not files:
                print(f"[WARNING] No LAS files found in: {p}")
                continue
            # randomly select a file that file size large than 10mb
            files = [f for f in files if os.path.getsize(f) > 10 * 1024 * 1024]
            f = np.random.choice(files)
            print(f)
            get_center_profie(f,output)