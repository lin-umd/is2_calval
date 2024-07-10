#!/usr/bin/env python
# coding: utf-8
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import sys
import os
import shutil
import geopandas as gpd
from shapely.geometry import Polygon
import icepyx as ipx
import argparse
import subprocess
import time
import numpy as np
import geopandas as gpd
from shapely.geometry import box
# one account one granuel one time. 
# 41459 polygons .......
#@dask.delayed
def get_atl03_photon(spatial_extent, name):
    folder_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/atl03_data/'+ name
    if os.path.exists(folder_out): return 
    short_name = 'ATL03'
    date_range = ['2018-01-01','2023-12-31']
    try: 
        region_project = ipx.Query(short_name, spatial_extent, date_range)
        # check how many granuels. 
        #print(region_usda_me.avail_granules())
        region_project.order_granules() # save display. 
        # data/atl03_data/atl03_data_csir_agincourt
        # better to get order ID, then download later ?????????????
        region_project.download_granules(folder_out)
        print('-- Done: ', name)
        time.sleep(1)
    except Exception as e:
        print('-- No granuels in polygon:', name)

#@dask.delayed
def check_atl03_granules(spatial_extent, name):
    short_name = 'ATL03'
    date_range = ['2018-01-01','2023-12-31']
    try: 
        region_project = ipx.Query(short_name, spatial_extent, date_range)
        # check how many granuels. 
        print(region_project.avail_granules())
        print('-- Done: ', name)
    except Exception as e:
        print('-- No granuels in polygon:', name)

if __name__ == "__main__":
    parse = argparse.ArgumentParser(description="Run icesat-2 subset")
    parse.add_argument("--check", help="Check granules only", action="store_true")
    parse.add_argument("--download", help="Download granules only", action="store_true")
    parse.add_argument("--project", help="Project name", type=str, required=True)
    args = parse.parse_args()
    gdf = gpd.read_parquet('../data/all_sites_20231218.parquet')
    project_names = gdf['name'].unique()
    name = args.project
    print('-- Processing ', name)
    spatial_extent = np.array(gdf[gdf['name'] == name].total_bounds)
    total_area = (spatial_extent[2] - spatial_extent[0]) * (spatial_extent[3] - spatial_extent[1])
    if total_area > 1: # 1 degree * 1 degree
            print('-- ROI is too large! Use a smaller ROI!')
            sys.exit("-- DONE")
    if args.download:
        get_atl03_photon(spatial_extent, name)
    if args.check:
        check_atl03_granules(spatial_extent, name)
    sys.exit("-- DONE")
# example use
# python 3is2subset_projects.py  --download  --project neon_wref2021