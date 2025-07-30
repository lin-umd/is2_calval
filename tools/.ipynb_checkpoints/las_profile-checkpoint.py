#!/usr/bin/env python
# coding: utf-8

import laspy
import glob,os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# In[4]:


files = glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las/usa/neon_blan2022/*.las')

def get_center_profie(f):
    output = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/profile"


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

    # Or stack as (N, 3) array
    xyz = np.vstack((x, y, z)).T


    df = pd.DataFrame({'x': xyz[:,0], 'y': xyz[:,1], 'z': xyz[:,2]})



    plt.figure(figsize=(10, 6))
    sc = plt.scatter(df['x'], df['z'], c=df['z'], cmap='viridis', s=1, alpha=0.7)
    plt.xlabel('X')
    plt.ylabel('Z (Height)')
    plt.title('X vs. Z colored by Height (Z)')
    plt.colorbar(sc, label='Height (Z)')
    plt.grid(True)
    plt.tight_layout()
    #plt.show()
    plt.savefig(f"{output}/{f.split('/')[-1].replace('.las', '.png')}", dpi=300)
    plt.close()

for f in files:
    #print(f)
    #get_center_profie(f)
    ## for blan2022:
    ## remove noise, points 100-500m
    out_p="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las_reprocess/neon_blan2022"
    out_f = out_p+os.path.basename(f)
    cmd = f"las2las64 -i {f} -keep_z 100 500 -o {out_f}"
    os.system(cmd)
    ## classify ground points 
    out_re = out_f[:-4]+'_reclass.las'
    #print(out_re)
    cmd = f"lasground_new64 -i {out_f} -o {out_re}"
    os.system(cmd)
    ## remove noise above 