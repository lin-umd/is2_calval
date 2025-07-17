#!/usr/bin/env python
# coding: utf-8
'''
script to check epsg in each project site.
lin xiong
07/03/2024

python check_als_epsg.py

# note 
# site can not determine epsg:  /gpfs/data1/vclgp/data/gedi/imported/africa/jpl_wwf_germany_drc
# jpl_wwf_germany_drc is the same data as the drc_ghent_field ALS sites, which have been properly divided by EPSG. (32634,32635,32733,32734,32735)
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
import subprocess
import glob
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gpd
from pyproj import Transformer
import dask.dataframe as dd
import dask_geopandas as ddg
from dask.diagnostics import ProgressBar

def get_laz_epsg(f):
    region = f.split('/')[7]
    name = f.split('/')[8]
    out_txt = f'../data/lasinfo_txt/{region}_{name}_{os.path.basename(f)[:-4]}.txt'  # Replace with the path to your text file
    if not os.path.exists(out_txt):     
        command = f"lasinfo -i {f}  -o  {out_txt}"  # Replace this with your Bash command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    epsg_number = None
    with open(out_txt, 'r') as file:
        for line in file:
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
base_dir = '/gpfs/data1/vclgp/data/gedi/imported'
laz_files = []
for folder_path in glob.glob(os.path.join(base_dir, '*', '*', 'LAZ_ground')):
    laz_files_in_folder = glob.glob(os.path.join(folder_path, '*.laz'))
    if laz_files_in_folder:
        laz_files.append(laz_files_in_folder[-1]) # each site first or last laz file.
for laz_file in laz_files:
    get_laz_epsg(laz_file)