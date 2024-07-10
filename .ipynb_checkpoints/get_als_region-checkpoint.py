#!/usr/bin/env python
# coding: utf-8
'''
script to read laz files and output bounds at each project site
lin xiong
07/03/2024
'''


import argparse, os, sys, re
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd 
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import numpy as np
import time
import multiprocessing
from tqdm import tqdm
import h5py
import math
import matplotlib.pyplot as plt
from scipy.stats import poisson
from shapely.geometry import Point
from shapely.geometry import Polygon
import utm
import subprocess
import glob
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gpd
from pyproj import Transformer
import dask.dataframe as dd
import dask_geopandas as ddg
from dask.diagnostics import ProgressBar
# example use
# python get_als_region.py --merge  --projects usa/neon_yell2020 
# python get_als_region.py --merge --all ../data/all_sites_20240703.parquet
def getCmdArgs():
        p = argparse.ArgumentParser(description = "Extract or update las bounds of als projects")
        p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true', help="merge laz bounds by als project")
        p.add_argument("-u", "--update", dest="update", required=False, action='store_true', help="update laz bounds")
        p.add_argument("-p", "--projects", dest="projects", nargs='+', required=False, help="List of projects, e.g. ['southamerica/embrapa_brazil_2020_jar_a01b', 'southamerica/embrapa_brazil_2020_jar_a01a']") 
        p.add_argument("-a", "--all", dest="all", required=False, type=str, help="Path of all als projects. ['../data/all_sites_20231218.parquet']")
        cmdargs = p.parse_args()
        return cmdargs

BAD_LAZ_bounds=['africa_amani_0118_0000002','africa_amani_0104_0000000','africa_amani_0140_0000000','africa_amani_0134_0000000','africa_amani_0154_0000000', 'africa_drc_ghent_field_32734_Plot136_000012','europe_froscham', 'africa_jpl_wwf_germany_drc', 'southamerica_embrapa_brazil_2020_jar_a01a_JAR_A01aL0002C0014_NP',
'southamerica_embrapa_brazil_2020_jar_a01b_JAR_A01bL0002C0009_NP']

def remove_bad_bounds(files): # files list 
    res = []
    for f in files:
        flag = 0
        for b in BAD_LAZ_bounds:
            if b in f: # contain bad las file. 
                flag = 1
                break
        if flag == 0: res.append(f)
    return res

@dask.delayed
def get_project_bound(project):
        region = project.split('/')[0]
        name  =  project.split('/')[1]
        file_paths = glob.glob(f'../data/laz_bounds/{region}_{name}_*.parquet')
        file_paths = remove_bad_bounds(file_paths) # files list
        print(f'\n ## processing project {region}_{name}, {len(file_paths)} bounds from laz files.')
        if len(file_paths) < 1: return 
        res = []
        for f in file_paths: # if files are two many -----
            laz_df = gpd.read_parquet(f)
            laz_df['file'] = os.path.basename(f)
            res.append(laz_df) # add delay 
        if len(res) == 0: return
        gdf_combined = gpd.GeoDataFrame( pd.concat(res, ignore_index=True) )
        result_gdf = gdf_combined#.dissolve() # dissolve here?
        result_gdf['area_ha'] = result_gdf.to_crs("EPSG:3395").area/ 10000 
        result_gdf = result_gdf[result_gdf['area_ha'] > 0] # filter 0 area_ha
        if len(result_gdf) ==0: return # edge case
        result_gdf = result_gdf[['region', 'name', 'area_ha','epsg', 'geometry', 'file']]
        print('\n ## writing result to a single project...')
        result_gdf.to_parquet(f'../data/bounds_project/{region}_{name}.parquet')
        return result_gdf   # 

@dask.delayed
def get_laz_bound(f):
    region = f.split('/')[7]
    name = f.split('/')[8]
    out_txt = f'../data/lasinfo_txt/{region}_{name}_{os.path.basename(f)[:-4]}.txt'  # Replace with the path to your text file
    if not os.path.exists(out_txt):     
        command = f"lasinfo -i {f}  -o  {out_txt}"  # Replace this with your Bash command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    epsg_number = None
    with open(out_txt, 'r') as file:
        for line in file:
            if 'min x y z: ' in line:
                    words = line.split()
                    xmin = words[4]
                    ymin = words[5]
            if 'max x y z: ' in line:
                    words = line.split()
                    xmax = words[4]
                    ymax = words[5]
            if 'key 3072 tiff_tag_location 0 count 1 value_offset' in line: # if defined ..
                    pattern = re.compile(r'key 3072 tiff_tag_location 0 count 1 value_offset (.*?) - ProjectedCSTypeGeoKey')
                    epsg_number = pattern.findall(line)[0]
            if epsg_number is None:
                if 'GDA94 / MGA zone 56' in line and 'australia' in out_txt:
                        epsg_number = '28356'
                if 'WGS 84 / UTM zone 30N' in line and 'africa_ghana' in out_txt :
                        epsg_number = '32630'
                if 'zofin_180416' in out_txt or 'zofin_180607' in out_txt :
                        epsg_number = '5514'
    file.close()   
    if epsg_number is None:
        print('site can not determine epsg: ', f) 
        return
    transformer = Transformer.from_crs("EPSG:" + epsg_number, "EPSG:4326",  always_xy=True) # important
    lon_min, lat_min = transformer.transform(float(xmin),float(ymin))
    lon_max, lat_max = transformer.transform(float(xmax),float(ymax))
    geometry = Polygon([(lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max), (lon_min, lat_max)])
    # Create a DataFrame
    data = pd.DataFrame({'region': [region],
                        'name': [name],
                        'f': [f],
                        'xmin': [xmin],
                        'ymin': [ymin],
                        'xmax': [xmax],
                        'ymax': [ymax],
                        'epsg': [epsg_number],
                        'lon_min': [lon_min],
                        'lat_min': [lat_min],
                        'lon_max': [lon_max],
                        'lat_max': [lat_max],
                        'geometry': [geometry]})
    laz_gdf = gpd.GeoDataFrame(data, geometry = 'geometry', crs = "EPSG:4326")
    laz_gdf['area_ha'] = laz_gdf.to_crs("EPSG:3395").area/ 10000 
    # ['region', 'name', 'area_ha', 'geometry']
    laz_gdf = laz_gdf[['region', 'name', 'area_ha', 'epsg', 'geometry']] 
    laz_gdf.to_parquet(f'../data/laz_bounds/{region}_{name}_{os.path.basename(f)[:-4]}.parquet')
    return laz_gdf

if __name__ == '__main__':
    args = getCmdArgs()
    print('---- start client')
    client = Client(n_workers=10, threads_per_worker=1)
    print(f'## -- dask client opened at: {client.dashboard_link}')
    if args.projects:
        print('## get my als projects')
        matches = args.projects
    else:
        print('## get all als projects')
        folders = glob.glob("/gpfs/data1/vclgp/data/gedi/imported/*/*/LAZ_ground")
        regex_pattern = r'/imported/(.*?)/LAZ_ground'
        matches = [re.search(regex_pattern, folder).group(1) for folder in folders]
    print('Number of als projects: ', len(matches))
    
    if args.update:
        print('## get laz files in projects')
        laz_all = []
        for project in matches:
            laz_files = glob.glob("/gpfs/data1/vclgp/data/gedi/imported/"+ project + "/LAZ_ground/*.laz")
            laz_all.extend(laz_files)
        cmds = [get_laz_bound(f) for f in laz_all]  
        progress(dask.persist(*cmds))
        print('') 
    if args.merge:
        cmds = [get_project_bound(project) for project in matches]  
        progress(dask.persist(*cmds))
        print('') 
    client.close() 
    if args.all:
        print('---- merge all projects')
        projects = glob.glob(f'../data/bounds_project/*.parquet')
        all = []
        for item in projects:
            all.append(gpd.read_parquet(item))
        all_sites = gpd.GeoDataFrame( pd.concat(all, ignore_index=True) )
        all_sites.to_parquet(args.all)