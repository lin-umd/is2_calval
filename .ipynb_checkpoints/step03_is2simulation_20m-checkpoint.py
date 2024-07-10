#!/usr/bin/env python
# coding: utf-8
'''
this script is for is2 simulation of 20m segment data over cal/val sites.
'''
# example use:
# python step03_is2simulation_20m.py --lamda 1 --output ../result/lamda1 --test --ratiopvpg 0.75

import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd 
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import numpy as np
import glob
import time
import subprocess
import multiprocessing
from tqdm import tqdm
import h5py
import utm
import pandas as pd
import math
import matplotlib.pyplot as plt
from scipy.stats import poisson
from shapely.geometry import Point
from shapely.geometry import Polygon
from pyproj import Transformer
import sys
import argparse

# als sites
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
               'csir_limpopo','jrsrp_ilcp2015_wholeq6']
redo_sites =  ['csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_limpopo',  'csir_venetia',  'csir_welverdient', 'jrsrp_ilcp2015_wholeq6']
# global env.
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'
LAS_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
RES_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result' # default output.
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse' # from Amy
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023.parquet'
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet"

def getCmdArgs():
    p = argparse.ArgumentParser(description = "IS2 simulation over cal/val database")
    p.add_argument("-n", "--name", dest="name", required=False, type=str, help="ALS project name") # nargs='+', if I want multiple names.
    p.add_argument("-p", "--projection", dest="projection", required=False, action='store_true', help="projecting is2 data to sites.") # nargs='+', if I want multiple names.
    p.add_argument("-l", "--lamda", dest="lamda", required=False, type=float, default=3, help="number of average photons in poisson distribution")
    p.add_argument("-o", "--output", dest="output", required=False, help="Output folder path")
    p.add_argument("-t", "--test", dest="test", required=False, action='store_true', help="Simulate 10 files")
    p.add_argument("-r", "--ratiopvpg", dest="ratiopvpg", required=False, type=float, default=1.5, help="Reflactance ratio")
    cmdargs = p.parse_args()        
    return cmdargs

def remove_las_files(LAS_PATH, region, name):
    las_folder = LAS_PATH + '/' + region + '/' + name
    if not os.path.exists(las_folder):
        print(f"Directory {las_folder} does not exist.")
        return
    try:
        for file_name in os.listdir(las_folder):
            if file_name.endswith(".las"):
                file_path = os.path.join(las_folder, file_name)
                os.remove(file_path)   
        print(f"All LAS files in {las_folder} have been deleted.")
    except OSError as e:
        print(f"Error deleting files in {las_folder}: {e}")     

@dask.delayed
def sim_is2_laz(laz_path, output=RES_PATH, lamda=3, ratiopvpg=1.5, overwrite_wave=False, overwrite_photon=False): 
    als_name = laz_path.split('/')[-3]
    region = laz_path.split('/')[-4]
    out_projected = FOLDER_PROJ + '/' + region  + '_' + als_name  + '.parquet'
    is2_in_als_projected = gpd.read_parquet(out_projected)
    basename = os.path.basename(laz_path)  
    bounds_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/laz_bounds/' + region+ '_' + als_name+ '_' + basename[:-4] + '.parquet'
    if not os.path.isfile(bounds_file):
        print('/n this laz file may corrupted:',laz_path)
        return
    else:
        laz_gdf = gpd.read_parquet(bounds_file)
        polygon = laz_gdf.to_crs(laz_gdf.iloc[0]['epsg']).iloc[0]['geometry']
    is2_laz = is2_in_als_projected.clip(polygon) 
    if (len(is2_laz) == 0): return # no is2 points in laz
    # /gpfs/data1/vclgp/data/gedi/imported/centralamerica/gliht_mexico_nfi_32613/LAZ_ground/AMIGACarb_PM1_Herm_Guan_GLAS_Apr2013_s719.laz
    las_out = LAS_PATH + '/' + region + '/' + als_name
    os.makedirs( las_out , exist_ok = True)
    bs = os.path.basename(laz_path)
    bs_las = bs[:-1] + 's'
    bs_las_path = las_out+'/'+bs_las
    if not os.path.isfile(bs_las_path):
        os.system(f'las2las -i {laz_path} -odir {las_out} -olas')
    segment_footprints_list = []
    for index_20m, row_20m in is2_laz.iterrows():
        segment_footprints_list.append(get_footprint(row_20m['e'], row_20m['n'], row_20m['orientation']))
    segment_footprints = pd.concat(segment_footprints_list, ignore_index=True)
    res_out = output + '/' + region + '/' + als_name 
    os.makedirs( res_out , exist_ok = True)
    out_coor = res_out+ '/coordinates_'+ basename[:-4] + '.txt'####
    out_wave = res_out+ '/wave_'+ basename[:-4] + '.h5'
    out_sim_log = res_out+ '/sim_log_'+ basename[:-4] + '.txt'
    if not os.path.exists(out_wave) or overwrite_wave: # waveform not exist, run simulation; ovewrite= true, run simulation.
        segment_footprints[['e', 'n']].to_csv(out_coor, sep=' ', header = False,  index = False)
        os.system(f'gediRat -fSigma 2.75 -readPulse {Pulse_PATH} -input {bs_las_path} -listCoord {out_coor}  -output {out_wave} -hdf  -ground > {out_sim_log}')
    if os.path.exists(out_wave):  # no matter exist previously or just after simulation now.
        out_pho = res_out+ '/photon_'+ basename[:-4] + '.parquet' # each las file, output photons
        if os.path.exists(out_pho) and not overwrite_photon: return # file exist, and no need to overwrite, return. 
        f_wave = h5py.File(out_wave, 'r')
        byte_strings = f_wave['WAVEID'][()]
        f_wave.close() 
        wave_ids = []
        for item in byte_strings:
            result_string = ''.join([byte.decode('utf-8') for byte in item])
            wave_ids.append(result_string)
        # Find common strings
        df1 = pd.DataFrame(wave_ids)
        df1.columns = ['id']
        df1['wave_index'] = df1.index
        res_las = []
        res_pho = []
        for index_20m, row_20m in is2_laz.iterrows():
            footprint = get_footprint(row_20m['e'], row_20m['n'], row_20m['orientation']) # string already.
            footprint['id'] = footprint['e'].astype(str) + '.' + footprint['n'].astype(str) 
            res = pd.merge(df1, footprint, on='id', how='inner')
            if (len(res) == 0): continue   ##### why is there empty waveform for this is2 20m segment?
            start = res['wave_index'].iloc[0] # must have
            end = res['wave_index'].iloc[-1] # must end>= start.
            pho_w_ground = get_sim_photon(out_wave, start, end, lamda, ratiopvpg) # average ~30*3 = 90 photons/rows
            if pho_w_ground is None: continue # this segment no photons. 
            pho_w_ground['land_segments/longitude_20m'] = row_20m['land_segments/longitude_20m']  # lon, lat, time ---> unique ID. 
            pho_w_ground['land_segments/latitude_20m'] = row_20m['land_segments/latitude_20m']
            pho_w_ground['land_segments/delta_time'] = row_20m['land_segments/delta_time']
            res_pho.append(pho_w_ground)        
            rh = get_sim_rh(pho_w_ground)
            rh['land_segments/longitude_20m'] = row_20m['land_segments/longitude_20m'] 
            rh['land_segments/latitude_20m'] = row_20m['land_segments/latitude_20m']
            rh['land_segments/delta_time'] = row_20m['land_segments/delta_time']
            res_las.append(rh) 
        if len(res_pho) ==0: return # not likely to happen
        res_pho = pd.concat(res_pho, ignore_index=True)
        
        res_pho.to_parquet(out_pho) # output photons.                        
        if (len(res_las) ==0): return # not likely to happen
        res_las = pd.concat(res_las, ignore_index=True)
        out_rh = res_out+ '/rh_'+ basename[:-4] + '.parquet' # each las file, give me rh.
        res_las.to_parquet(out_rh) # Pandas will silently overwrite the file, if the file is already there
                        
def get_sim_photon(filename, start, end, lamda=3, ratiopvpg=1.5): # for each 20m segment, get the ~30 waveforms.
    pho_w_ground = pd.DataFrame()
    with h5py.File(filename, "r") as f:
        N_foorprints = f['RXWAVECOUNT'].shape[0]        
        for i in range(start, end+1): # inclusive for i th waveform
            RXWAVECOUNT = f['RXWAVECOUNT'][i]
            GRWAVECOUNT = f['GRWAVECOUNT'][i]
            zStart = f["Z0"][i]
            zEnd = f["ZN"][i]
            zG = f["ZG"][i] # can be NaN
            wfCount = f["NBINS"][0]
            if (wfCount < 1): continue
            wfStart = 1
            # Calculate zStretch
            zStretch = zEnd + (np.arange(wfCount, 0, -1) * ((zStart - zEnd) / wfCount))
            #plt.plot(RXWAVECOUNT, zStretch,color='red' )
            #plt.plot(RXWAVECOUNT - GRWAVECOUNT, zStretch,color='black', linestyle='--' )
            #plt.show()
            # sampling 
            n = np.random.poisson(lamda) # posson sample, how many photons? n=0, 1,2,...
            if n == 0: continue # no sample photons. continue to next waveform
            # canopy + ground photons
            rows = np.arange(RXWAVECOUNT.shape[0])
            scaled_RXWAVECOUNT = ((RXWAVECOUNT - RXWAVECOUNT)*ratiopvpg + RXWAVECOUNT*1)/ (ratiopvpg + 1) # scale the wavefrom by ratiopvpg
            total = sum(scaled_RXWAVECOUNT)
            # Normalize the data by dividing each value by the total
            p_data = [value / total for value in scaled_RXWAVECOUNT]
            photon_rows = np.random.choice(rows, size=n, p=p_data)
            df1 = zStretch[photon_rows] - zG
            df1 = pd.DataFrame(df1,  columns= ["Z"])
            df1['LON0'] = f["LON0"][i]
            df1['LAT0'] = f["LAT0"][i]
            df1.dropna(inplace=True) # drop nan rows, NaN caused by zG.
            if len(df1) == 0: continue # continue to next waveform
            # add a ground flag to df1 
            df1['ground_flag'] = GRWAVECOUNT[photon_rows] > 0 
            if (len(pho_w_ground) == 0):
                pho_w_ground = df1
            else:
                pho_w_ground = pd.concat([pho_w_ground, df1], ignore_index=True)
    f.close()
    if (len(pho_w_ground) == 0): return  # no photons in this laz file
    return pho_w_ground
def get_sim_rh(pho_w_ground): # for each segment.
    percentiles = np.arange(0, 101, 1) #0 --> min height # 100 --> max height
    pho_no_ground = pho_w_ground[pho_w_ground['ground_flag'] == False]
    ch_98 = np.nan # no ground photons in this laz simulation
    if len(pho_no_ground) > 0:
        height_percentiles = np.percentile(pho_no_ground["Z"], percentiles)
        ch_98 = height_percentiles[97] ### h_98
    height_percentiles = np.percentile(pho_w_ground["Z"], percentiles)
    column_names = ["rh" + str(i) for i in range(101)]  # Creates a list from rh0 to rh100
    sim = pd.DataFrame(columns=column_names)
    sim.loc[0] = height_percentiles
    sim['h_canopy_98'] = ch_98
    return sim

def get_footprint(e, n, slope): 
    theta = math.atan(slope)
    data = np.arange(-14, 15) # -14 --14
    e_array = e + data * 0.7*math.cos(theta)
    n_array = n + data * 0.7*math.sin(theta)
    e_array = [f'{round(e, 6):.6f}' for e in e_array] # round gediRat waveID is round to 6 decimals for sure.
    n_array = [f'{round(e, 6):.6f}' for e in n_array] 
    footprints = pd.DataFrame({'e': e_array, 'n': n_array})
    return footprints   # string now.

def add_orientation(is2_in_als_projected):
    df = is2_in_als_projected
    root_files = df['root_file'].unique()
    beams = ['gt1l', 'gt1r','gt2l', 'gt2r','gt3l', 'gt3r']
    for f in root_files:
        for beam in beams:
            # get single beam track data
            tmp = df[ (df['root_file'] == f )&  (df['root_beam'] == beam)]
            if (len(tmp) < 2): continue
            orientation, b = np.polyfit(tmp['e'], tmp['n'], 1) # best fit direction. Polynomial coefficients
            df.loc[(df['root_file'] == f )&  (df['root_beam'] == beam), 'orientation'] = orientation
    return df    

def transform_coordinates(row, transformer):
    lat_c = row['land_segments/latitude_20m']
    lon_c = row['land_segments/longitude_20m']
    e, n = transformer.transform(lon_c, lat_c)
    return pd.Series({'e': e, 'n': n})

def is2ProjectToSite(gdf_als): # save is2 in each site
    print('## read is2 in cal/val sites ...')
    gdf_is2 = gpd.read_parquet(IS2_20M_FILE) # 18550387 IS2 20 m segment points
    gdf_is2 = gdf_is2.reset_index(drop=True) # need to reset index, not using h3 level 12 index
    for index, row in gdf_als.iterrows(): 
        out_projected = FOLDER_PROJ + '/' + row['region'] + '_' + row['name'] + '.parquet'
        als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
        is2_in_als = gdf_is2.loc[als_index] 
        is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
        print("# is2 points in this als project: ", len(is2_in_als))
        if (len(is2_in_als) <= 1): continue
        print('# convert segments using EPSG of this project...', row['epsg'])
        transformer = Transformer.from_crs(4326, row['epsg'], always_xy = True)
        is2_in_als[['e', 'n']] = is2_in_als.apply(lambda row: transform_coordinates(row, transformer), axis=1)
        geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
        is2_in_als_projected = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry)
        is2_in_als_projected_add_ori = add_orientation(is2_in_als_projected)
        print('# filter NaN values in track orientation')
        is2_in_als_projected_add_ori = is2_in_als_projected_add_ori.dropna(subset=['orientation'])
        is2_in_als_projected_add_ori.to_parquet(out_projected)
        
if __name__ == '__main__':
    args = getCmdArgs()
    output_folder = args.output
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    gdf_als = gpd.read_parquet(ALS_SITES)
    if args.projection:
        is2ProjectToSite(gdf_als)
    all_lazs = [] # get all laz files
    for index, row in gdf_als.iterrows(): 
        if args.name and row['name'] != args.name: continue
        if row['name'] not in redo_sites: continue # redo simulation .....
        out_projected = FOLDER_PROJ + '/' + row['region'] + '_' + row['name'] + '.parquet'
        data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground'
        files_path = data_path + '/*.laz' 
        laz_files = glob.glob(files_path)
        all_lazs.extend(laz_files)      
    if args.test:
        print('# test simulation')
        all_lazs = all_lazs[:10]
    print('# number of laz files: ', len(all_lazs))
    print('# start client')
    client = Client(n_workers=30, threads_per_worker=1) # 
    print(f'# dask client opened at: {client.dashboard_link}')
    cmds = [sim_is2_laz(laz_f,output=output_folder, lamda = args.lamda, ratiopvpg=args.ratiopvpg, overwrite_wave=False, overwrite_photon=False) for laz_f in all_lazs]  
    progress(dask.persist(*cmds))
    print('') 
    client.close()
    sys.exit("# DONE")

