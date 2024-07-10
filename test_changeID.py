#!/usr/bin/env python
# coding: utf-8

from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd 
import numpy as np
import glob
import time
import subprocess
# read H5 file 
import h5py
########
import utm
import pandas as pd
import math
import matplotlib.pyplot as plt
from scipy.stats import poisson
from shapely.geometry import Point
from shapely.geometry import Polygon
# old is2 data for simulation
import sys
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import subprocess
import multiprocessing as mp
from tqdm import tqdm


#@dask.delayed
def change_id(f,gdf_want):
    df= pd.read_parquet(f)
    if 'fid' in df.columns and 'land_segments/longitude_20m' not in df.columns:
        print('-- processing: ', f)
        res_merge = pd.merge(df, gdf_want, on='fid', how='inner')
        res_merge.to_parquet(f)

if __name__ == '__main__':
        print('# read old is2 in cal/val sites ...')
        gdf_is2 = gpd.read_parquet('../result/is2_20m_calval_09252023.parquet')
        gdf_is2['fid'] = gdf_is2.index
        cols = ['fid', 'land_segments/longitude_20m' , 'land_segments/latitude_20m', 'land_segments/delta_time' ]
        gdf_want = gdf_is2[cols]
        # remove fid, get lat_20, lon_20 , delta_time
        data_sim = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/' + '*/*' + '/rh_*.parquet'
        files = glob.glob(data_sim)
        # for f in files:
        #     change_id(f,gdf_is2)
        def update_progress_bar(_):
            progress_bar.update()   
        nprocesses = len(files)
        progress_bar = tqdm(total=nprocesses)
        pool = mp.Pool(10)
        for f in files:
            pool.apply_async(change_id, (f,gdf_want), callback=update_progress_bar)
        pool.close()
        pool.join()






