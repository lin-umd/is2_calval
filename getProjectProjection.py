#!/usr/bin/env python3
"""
reproject is2 data over each als project site, also add is2 track 
lin xiong
lxiong@umd.edu
07/03/2024
example:
python getProjectProjection.py
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
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023.parquet'
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet"
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'
def transform_coordinates(row, transformer):
    lat_c = row['land_segments/latitude_20m']
    lon_c = row['land_segments/longitude_20m']
    e, n = transformer.transform(lon_c, lat_c)
    return pd.Series({'e': e, 'n': n})
    
def add_orientation(is2_in_als_projected):
#def add_orientation (file, o_path): # need root_file, root_beam, 
    df = is2_in_als_projected
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



def projection(row, FOLDER_PROJ):
    print('# now processing als project: ', row['region'] + '_' + row['name'])
    out_projected = FOLDER_PROJ + '/' + row['region'] + '_' + row['name'] + '.parquet'
    als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
    is2_in_als = gdf_is2.loc[als_index] 
    is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
    print("# is2 points in this als project: ", len(is2_in_als))
    if (len(is2_in_als) < 2): return
    print('# convert segments using EPSG of this project...', row['epsg']) 
    transformer = Transformer.from_crs(4326, row['epsg'], always_xy = True)
    is2_in_als[['e', 'n']] = is2_in_als.apply(lambda row: transform_coordinates(row, transformer), axis=1)
    geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
    is2_in_als_projected = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry) # , crs="EPSG:4326")
    #print('is2_in_als_projected', is2_in_als_projected.head())
    is2_in_als_projected_add_ori = add_orientation(is2_in_als_projected)
    #print('add_ori', is2_in_als_projected_add_ori.head())
    if 'orientation' not in is2_in_als_projected_add_ori.columns: return # no values.
    print('# filter NaN values in track orientation')
    is2_in_als_projected_add_ori = is2_in_als_projected_add_ori.dropna(subset=['orientation'])
    # just in 
    print('# Writing parquet...')
    is2_in_als_projected_add_ori.to_parquet(out_projected)
    return 


if __name__ == '__main__':
    print('## read all is2 in cal/val sites ...')
    gdf_is2 = gpd.read_parquet(IS2_20M_FILE) # 
    print('## number of footprints...', len(gdf_is2))
    #### need to reset index, not using h3 level 12 index
    gdf_is2 = gdf_is2.reset_index(drop=True)
    gdf_als = gpd.read_parquet(ALS_SITES)
    nCPU = len(gdf_als)
    if nCPU > 15 : 
       nCPU = 15  # number of cores to use  
    print('# parallel processing...')
    pool = multiprocessing.Pool(nCPU) # Set up multi-processing
    progress_bar = tqdm(total=len(gdf_als))
    def update_progress_bar(_):
          progress_bar.update()  
    for index, row in gdf_als.iterrows():
        pool.apply_async(projection, (row, FOLDER_PROJ), callback=update_progress_bar)
    pool.close()
    pool.join()
    # Close the progress bar
    progress_bar.close()
    sys.exit("## -- DONE")

    