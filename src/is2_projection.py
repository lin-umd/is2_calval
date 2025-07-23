#!/usr/bin/env python3
"""
reproject is2 data over each als project site, also add is2 track 
lin xiong
lxiong@umd.edu
07/03/2024
example:
python is2_projection.py
"""
# library
import os
import glob 
import argparse
import geopandas as gpd
from pyproj import Transformer
from shapely.geometry import Point, Polygon
import numpy as np
import math
import matplotlib.pyplot as plt
import pandas as pd 
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import sys
import rasterio
import time
import subprocess
import multiprocessing
from tqdm import tqdm

######## get projected all sites
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/is2_20m_cal_val_disturbed_20250721.parquet'
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet"
CALVAL_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/calval_sites_20250721.parquet" 
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'


    
def add_orientation(is2_in_als_projected): 
    df = is2_in_als_projected.copy()
    # group by root file and beam
    root_files = df['root_file'].unique()
    beams = ['gt1l', 'gt1r','gt2l', 'gt2r','gt3l', 'gt3r']
    for f in root_files:
        for beam in beams:
            # get single beam track data
            tmp = df[ (df['root_file'] == f ) &  (df['root_beam'] == beam)]
            if (len(tmp) < 2): continue
            orientation, b = np.polyfit(tmp['e'], tmp['n'], 1) # best fit direction. 
            df.loc[(df['root_file'] == f ) &  (df['root_beam'] == beam), 'orientation'] = orientation
    return df  

def projection2site(row):
    print('# now processing als project: ', f"{row.region}_{row.name}")
    out_projected = os.path.join(FOLDER_PROJ, f"{row.region}_{row.name}.parquet")
    als_index = gdf_is2.sindex.query(row.geometry)
    is2_in_als = gdf_is2.loc[als_index]
    is2_in_als = is2_in_als.clip(row.geometry)
    print("# is2 points in this als project: ", len(is2_in_als))
    if len(is2_in_als) < 2:
        return
    print('# convert segments using EPSG of this project...', row.epsg)
    is2_in_als_projected = is2_in_als.to_crs(epsg=row.epsg)
    is2_in_als_projected['e'] = is2_in_als_projected.geometry.x
    is2_in_als_projected['n'] = is2_in_als_projected.geometry.y
    #print('is2_in_als_projected', is2_in_als_projected.head())
    is2_in_als_projected_add_ori = add_orientation(is2_in_als_projected)
    #print('add_ori', is2_in_als_projected_add_ori.head())
    if 'orientation' not in is2_in_als_projected_add_ori.columns: return # no values.
    print('# filter NaN values in track orientation')
    is2_in_als_projected_add_ori = is2_in_als_projected_add_ori.dropna(subset=['orientation'])
    # just in 
    print('# Writing parquet...')
    is2_in_als_projected_add_ori.to_parquet(out_projected)
    

if __name__ == '__main__':
    print('## read is2 calval segments')
    gdf_is2 = gpd.read_parquet(IS2_20M_FILE)
    gdf_is2.reset_index(drop=True, inplace=True)
    print('## number of footprints...', len(gdf_is2))
    print('## read ALS bounds in cal/val sites ...')
    gdf_als = gpd.read_parquet(CALVAL_SITES)
    print('## processing sites one by one in a for loop...')
    for row in tqdm(gdf_als.itertuples(index=False), total=len(gdf_als)):
        projection2site(row)
    sys.exit("## -- DONE")