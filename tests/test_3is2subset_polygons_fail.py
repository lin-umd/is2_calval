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
@dask.delayed
def get_atl03_photon(spatial_extent, name, index):
    folder_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/atl03_data/'+ name +'_' + str(index)
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
        print('-- Done: ', name, index)
        time.sleep(1)
    except Exception as e:
        print('-- No granuels in polygon:', name, index)

@dask.delayed
def check_atl03_granules(spatial_extent, name, index):
    short_name = 'ATL03'
    date_range = ['2018-01-01','2023-12-31']
    try: 
        region_project = ipx.Query(short_name, spatial_extent, date_range)
        # check how many granuels. 
        print(region_project.avail_granules())
        print('-- Done: ', name, index)
    except Exception as e:
        print('-- No granuels in polygon:', name, index)

if __name__ == "__main__":
    parse = argparse.ArgumentParser(description="Run icesat-2 subset")
    # Add the --check argument
    parse.add_argument("--check", help="Check granules only", action="store_true")
    parse.add_argument("--download", help="Download granules only", action="store_true")
    parse.add_argument("--test", help="Test first 10 polygons", action="store_true")
    # parse.add_argument("--output_folder", help="Output folder to write", required=True)
    # #parse.add_argument("--tile", help="Tile number [integer]", required = True)
    args = parse.parse_args()
    gdf = gpd.read_parquet('../data/all_sites_20231218.parquet')
    # loop through very polgyon 
    gdf_polygons = gdf.explode(index_parts=False)
    gdf_polygons = gdf_polygons.reset_index(drop=True)
    print(len(gdf_polygons))
    
##### simple loop version ##################################################
    # for index, row in gdf_polygons[:2].iterrows(): 
    #     spatial_extent = np.array(row['geometry'].bounds)
    #     name = row['name']
    #     print('-- project name and poylon index: ', name, index)
    #     print('-- Bounding box:', spatial_extent)
    #     if args.download:
    #         get_atl03_photon(spatial_extent, name, index)
    #     if args.check:
    #         check_atl03_granules(spatial_extent, name, index)
#############################################################################    
    # dask version try 
        #### start dask client 
    print('## start client')
    client = Client(n_workers=5)
    print(f'## -- dask client opened at: {client.dashboard_link}')
    if args.test:
            gdf_polygons = gdf_polygons[:10]
    if args.download:
            cmds = [get_atl03_photon(np.array(row['geometry'].bounds), row['name'], index) for index, row in gdf_polygons.iterrows()]
            _ = dask.persist(*cmds)
            progress(_)
            print('') 
    if args.check:
            cmds = [check_atl03_granules(np.array(row['geometry'].bounds), row['name'], index) for index, row in gdf_polygons.iterrows()]
            _ = dask.persist(*cmds)
            progress(_)
            print('') 
    client.close()
    sys.exit("-- DONE")
# example use
# python 3is2subset.py  --check  --test 
# python 3is2subset.py  --download  --test  