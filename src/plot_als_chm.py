#!/usr/bin/env python
# coding: utf-8
"""
convert chm tif from local to wgs84, 1km resolution and plot
lin xiong
"""
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import glob
import rasterio
import matplotlib.pyplot as plt
from shapely.geometry import Point, box
import subprocess
import time
import numpy as np
import glob
import rioxarray 

OUT='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/chm_v2/'
os.makedirs(OUT, exist_ok=True)

def plot_tif(c):
    dataset = rasterio.open(c)
    band1 = dataset.read(1)
    data = band1[~np.isnan(band1)]
    data = data[data != -999999]
    if len(data) < 1: 
        print('No data in this file', c)
        return
    p5 = np.percentile(data, 5)
    p95 = np.percentile(data, 95)
    #if p5 < -10 or p95 > 120:
    print(f'file {c}, p5 {p5} , p95 {p95}')
    plt.figure(figsize=(8, 4))  # Adjust the width and height as desired
    plt.imshow(band1, cmap='rainbow' , vmin = 0, vmax = 120)
    cbar = plt.colorbar(fraction=0.025, pad=0.04)
    plt.savefig(OUT+os.path.basename(c)[:-4]+'.png')
    plt.close()

def mergeTif2site(reg, name):
    chms_path = '/gpfs/data1/vclgp/data/gedi/imported/' + reg + '/' + name + '/chm/*.tif'
    chms = glob.glob(chms_path)
    out_tif = OUT+ reg + '_' + name + '.tif'
    if os.path.exists(out_tif): return 
    list_tif = OUT+reg + '_' + name+'tiff_list.txt'
    with open(list_tif, 'w') as f:
        for file_name in chms:
            f.write(file_name + '\n')
    print(out_tif)
    command = f"gdal_merge.py -ps 1000 1000  -o {out_tif} --optfile {list_tif} "  # Example command (listing files in current directory)
    #command = f"gdal_merge.py -ps 1000 1000  -o {out_tif} {' '.join(chms)}"
    os.system(command)
    #subprocess.run(command, shell=True, capture_output=False, text=True)
    if os.path.exists(out_tif):
        plot_tif(out_tif)

def reproject_ease(tif):
    xds = rioxarray.open_rasterio(tif)
    xds_ease = xds.rio.reproject("EPSG:6933")
    xds_ease.rio.to_raster(tif[:-4] + '_ease.tif')


# cal/val paper
VALID_SITES = ['amani','csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_venetia', 'csir_welverdient', 'drc_ghent_field_32635', 
               'drc_ghent_field_32733', 'drc_ghent_field_32734', 'gsfc_mozambique', 'jpl_lope', 'jpl_rabi', 'khaoyai_thailand', 
               'chowilla', 'credo', 'karawatha', 'litchfield', 'rushworth_forests', 'tern_alice_mulga', 'tern_robson_whole', 'costarica_laselva2019', 
               'skidmore_bayerischer', 'zofin_180607', 'spain_exts1', 'spain_exts2', 'spain_exts3', 'spain_leonposada', 'spain_leon1', 
               'spain_leon2', 'spain_leon3', 'jpl_borneo_004', 'jpl_borneo_013', 'jpl_borneo_040', 'jpl_borneo_119', 'jpl_borneo_144', 'chave_paracou', 
               'embrapa_brazil_2020_and_a01', 'embrapa_brazil_2020_bon_a01', 'embrapa_brazil_2020_cau_a01', 'embrapa_brazil_2020_duc_a01', 
               'embrapa_brazil_2020_hum_a01', 'embrapa_brazil_2020_par_a01', 'embrapa_brazil_2020_rib_a01', 'embrapa_brazil_2020_tal_a01',
               'embrapa_brazil_2020_tan_a01', 'embrapa_brazil_2020_tap_a01', 'embrapa_brazil_2020_tap_a04', 'walkerfire_20191007', 
               'neon_abby','neon_bart2018','neon_blan2019','neon_clbj2019','neon_dsny2018',
               'neon_harv2018','neon_jerc2019','neon_leno2018','neon_mlbs2018','neon_osbs2018',
               'neon_puum2019','neon_scbi2019','neon_steicheq2019','neon_yell2019' 'inpe_brazil31983', 'inpe_brazil31981', 
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978',
               'cambridge_sepilok', 'g_aguilarNP', 'nsw_armidale', 'nsw_casino', 'nsw_coffsharbour',
               'southafrica_karkloof', 'southafrica_umgano']

path = '/gpfs/data1/vclgp/data/gedi/imported/*/*/'
folder_names = glob.glob(path)

for folder in folder_names:
    region = folder.split('/')[-3]
    name = folder.split('/')[-2]
    if name in VALID_SITES:
        #print(folder)
        mergeTif2site(region, name)
