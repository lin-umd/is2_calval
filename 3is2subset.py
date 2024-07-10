#!/usr/bin/env python
# coding: utf-8
import os
os.environ['USE_PYGEOS'] = '0'
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import sys
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
# https://urs.earthdata.nasa.gov/documentation/for_users/data_access/create_net_rc_file
# creat this file for earth data login.
# cannot parallel computing for icepyx ordering commands. 
# NSIDC maintenance MST 3:30pm 12/20/2023  5:30 PM EST. ----

VALID_SITES = ['amani','csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_venetia', 'csir_welverdient', 'drc_ghent_field_32635', 
               'drc_ghent_field_32733', 'drc_ghent_field_32734', 'gsfc_mozambique', 'jpl_lope', 'jpl_rabi', 'tanzania_wwf_germany', 'khaoyai_thailand', 
               'chowilla', 'credo', 'karawatha', 'litchfield', 'rushworth_forests', 'tern_alice_mulga', 'tern_robson_whole', 'costarica_laselva2019', 
               'skidmore_bayerischer', 'zofin_180607', 'spain_exts1', 'spain_exts2', 'spain_exts3', 'spain_exts4', 'spain_leonposada', 'spain_leon1', 
               'spain_leon2', 'spain_leon3', 'jpl_borneo_004', 'jpl_borneo_013', 'jpl_borneo_040', 'jpl_borneo_119', 'jpl_borneo_144', 'chave_paracou', 
               'embrapa_brazil_2020_and_a01', 'embrapa_brazil_2020_bon_a01', 'embrapa_brazil_2020_cau_a01', 'embrapa_brazil_2020_duc_a01', 
               'embrapa_brazil_2020_hum_a01', 'embrapa_brazil_2020_par_a01', 'embrapa_brazil_2020_rib_a01', 'embrapa_brazil_2020_tal_a01',
               'embrapa_brazil_2020_tan_a01', 'embrapa_brazil_2020_tap_a01', 'embrapa_brazil_2020_tap_a04', 'walkerfire_20191007', 
               'neon_abby2018', 'neon_abby2019', 'neon_abby2021', 'neon_bart2018', 'neon_bart2019', 'neon_blan2019', 'neon_blan2021', 
               'neon_clbj2018', 'neon_clbj2019', 'neon_clbj2021', 'neon_clbj2021', 'neon_dela2018', 'neon_dela2019', 'neon_dela2021', 
               'neon_dsny2018', 'neon_dsny2021', 'neon_grsm2018', 'neon_grsm2021', 'neon_guan2018', 'neon_harv2018', 'neon_harv2019', 
               'neon_jerc2019', 'neon_jerc2021', 'neon_jorn2018', 'neon_jorn2019', 'neon_jorn2021', 'neon_konz2019', 'neon_konz2020', 
               'neon_leno2018', 'neon_leno2019', 'neon_leno2021', 'neon_mlbs2018', 'neon_mlbs2021', 'neon_moab2018', 'neon_moab2021', 
               'neon_niwo2019', 'neon_niwo2020', 'neon_nogp2021', 'neon_onaq2019', 'neon_onaq2021', 'neon_osbs2018', 'neon_osbs2019', 
               'neon_osbs2021', 'neon_puum2020', 'neon_rmnp2018', 'neon_rmnp2020', 'neon_scbi2019', 'neon_scbi2021', 'neon_serc2019', 
               'neon_serc2021', 'neon_sjer2019', 'neon_soap2018', 'neon_soap2019', 'neon_soap2021', 'neon_srer2019', 'neon_srer2021', 
               'neon_stei2019', 'neon_stei2020', 'neon_ster2021', 'neon_tall2018', 'neon_tall2019', 'neon_tall2021', 'neon_teak2021', 
               'neon_ukfs2018', 'neon_ukfs2019', 'neon_ukfs2020', 'neon_unde2019', 'neon_unde2020', 'neon_wood2021', 'neon_wref2019', 
               'neon_wref2021', 'neon_yell2018', 'neon_yell2019', 'neon_yell2020', 
               'neon_blan2022', 'neon_clbj2022', 'neon_grsm2022', 'neon_moab2022', 'neon_onaq2022', 'neon_rmnp2022', 'neon_serc2022', 
               'neon_stei2022', 'neon_steicheq2022', 'neon_ster2022', 'neon_unde2022', 'inpe_brazil31983', 'inpe_brazil31981', 
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978',
               'jrsrp_ilcp2015_wholeq6', 'csir_limpopo'] # add this two for Mikhail. 

def global_cells():
    # Create a regular grid of 0.1 degree
    lon_min, lon_max = -180, 180
    lat_min, lat_max = -90, 90
    cell_size = 0.1
    grid_cells = []
    # Iterate over longitude and latitude with the specified cell size
    for lon in range(int(lon_min * 10), int(lon_max * 10), int(cell_size * 10)):
        for lat in range(int(lat_min * 10), int(lat_max * 10), int(cell_size * 10)):
            cell = box(lon / 10, lat / 10, (lon + cell_size * 10) / 10, (lat + cell_size * 10) / 10)
            grid_cells.append(cell)
    # Create a GeoDataFrame for the grid cells
    grid_gdf = gpd.GeoDataFrame(geometry=grid_cells, crs=gdf.crs)
    return grid_gdf


def get_name(spatial_extent):
    min_lon = int(spatial_extent[0]*10)
    min_lat = int(spatial_extent[1]*10)
    letter_lon='E'
    if min_lon < 0: 
       letter_lon = 'W'
       min_lon = -min_lon
    letter_lat='N'
    if min_lat < 0: 
       letter_lat = 'S'
       min_lat = -min_lat
    name = letter_lon + str(min_lon) + letter_lat + str(min_lat)
    return name

#@dask.delayed
def get_is2_photon(spatial_extent, short_name = 'ATL03'):
    name = get_name(spatial_extent)
    folder_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/' + short_name.lower()+ '_data/'+ name
    if os.path.exists(folder_out): return
    date_range = ['2018-01-01','2023-12-31']
    try: 
        region_project = ipx.Query(short_name, spatial_extent, date_range)
        # check how many granuels. 
        #print(region_usda_me.avail_granules())
        region_project.order_granules() # save display. 
        region_project.download_granules(folder_out)
        print('-- This cell is done: ', name)
        time.sleep(1)
    except Exception as e:
        print('-- No granuels in this cell:', name)

#@dask.delayed
def check_is2_granules(spatial_extent, short_name = 'ATL03'):
    name = get_name(spatial_extent)
    date_range = ['2018-01-01','2023-12-31']
    try: 
        region_project = ipx.Query(short_name, spatial_extent, date_range)
        # check how many granuels. 
        print(region_project.avail_granules())
        print('-- This cell is done: ', name)
    except Exception as e:
        print('-- No granuels in this cell:', name)

if __name__ == "__main__":
    parse = argparse.ArgumentParser(description="Run icesat-2 subset")
    # Add the --check argument
    parse.add_argument("--product", help="product name [ATL03 or ATL08]", type=str, required=True)
    parse.add_argument("--check", help="Check granules only", action="store_true")
    parse.add_argument("--download", help="Download granules only", action="store_true")
    parse.add_argument("--test", help="Test the first 0.1 degree cell", action="store_true")
    # parse.add_argument("--output_folder", help="Output folder to write", required=True)
    # #parse.add_argument("--tile", help="Tile number [integer]", required = True)
    args = parse.parse_args()
    gdf = gpd.read_parquet('../data/all_sites_20231218.parquet')
    if os.path.exists('../result/atl03_grid_cells_v2.parquet'):
        intersection_cells=gpd.read_parquet('../result/atl03_grid_cells_v2.parquet')
    else:
        grid_gdf = global_cells()
        filtered_gdf = gdf[gdf['name'].isin(VALID_SITES)]
        intersection_cells = gpd.sjoin(grid_gdf, filtered_gdf, how='inner', op='intersects')
        intersection_cells.to_parquet('../result/atl03_grid_cells_v2.parquet') # save grids file .
    print('-- number of 0.1 degree cells: ', len(intersection_cells))
    if args.test:
            intersection_cells = intersection_cells[:1]
    # Drop the current index and reset to default integer index
    intersection_cells = intersection_cells.reset_index(drop=True)
    for index, row in intersection_cells.iterrows(): 
        print('-- Cell index: ', index)
        spatial_extent = np.array(row['geometry'].bounds)
        print('-- Bounding box:', spatial_extent)
        if args.download:
            get_is2_photon(spatial_extent, args.product)
        if args.check:
            check_is2_granules(spatial_extent, args.product)
    sys.exit("-- DONE")
# example use
# python 3is2subset.py  --check --test --product ATL08
# python 3is2subset.py  --download --test --product ATL08