#!/usr/bin/env python
# coding: utf-8
'''
this script is for is2 simulation of 20m segment data over cal/val sites. The simulation is done per ATL08 file per beam.
lin xiong
lxiong@umd.edu
07/03/2024
# plz test lastool and gediRat before use.

# example use:
python /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/src/is2_simulation_20m.py --file file_path --sim --metric --prepare

# lastool windows version -- old system:
module load wine
wine /gpfs/data1/vclgp/software/lastools/bin/las2las.exe
# lastool linux version -- new system
export LD_LIBRARY_PATH=/gpfs/data1/vclgp/decontot/environments/lastools/lib:$LD_LIBRARY_PATH
/gpfs/data1/vclgp/xiongl/tools/lastool/bin/las2las64 -h
# or try to install free las tool commands.
https://anaconda.org/conda-forge/lastools .
# The native lastools for Linux are now available as a module. Type: module load rh9/lastools
# gediRat issue
conda install conda-forge::gsl
cd /gpfs/data1/vclgp/xiongl/env/linpy/lib
ln -s libgsl.so.28 libgsl.so.0
ln -s libgdal.so.34 libgdal.so.27
export LD_LIBRARY_PATH=/gpfs/data1/vclgp/xiongl/env/linpy/lib:$LD_LIBRARY_PATH
'''


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
# global env.
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'# in als local coordinates.
ALS_BOUND_PATH='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/bounds_project'
LAS_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
LAZ_PATH = '/gpfs/data1/vclgp/data/gedi/imported'
RES_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3' # default output.
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse' # from Amy
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/is2_20m_cal_val_disturbed_20250721.parquet'
CALVAL_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/calval_sites_20250721.parquet" 

def getCmdArgs():
    p = argparse.ArgumentParser(description = "IS2 simulation over cal/val database")
    p.add_argument("-f", "--file", dest="file", required=True, type=str, help="Text file containing ALS project names (one per line)")
    #p.add_argument("-n", "--name", dest="name", required=False, type=str, help="ALS project name") # nargs='+', if I want multiple names.
    p.add_argument("-w", "--workers", dest="workers", required=False, type=int, default=10, help="number of cores to be used")
    p.add_argument("-l", "--lamda", dest="lamda", required=False, type=float, default=1, help="number of average photons in poisson distribution")
    #p.add_argument("-o", "--output", dest="output", required=False, default=RES_PATH,  help="Output folder path")
    #p.add_argument("-t", "--test", dest="test", required=False, action='store_true', help="Simulate 10 files")
    p.add_argument("-e", "--prepare", dest="prepare", required=False, action='store_true', help="pepare coordinates, las lists for simulation")
    p.add_argument("-s", "--sim", dest="sim", required=False, action='store_true', help="run simulation to waves")
    p.add_argument("-m", "--metric", dest="metric", required=False, action='store_true', help="run waves to rh metrics")
    p.add_argument("-r", "--ratiopvpg", dest="ratiopvpg", required=False, type=float, default=0.86, help="Reflectance ratio")
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


def project2track2beam(region, name, output=RES_PATH): 
    is2file=os.path.join(FOLDER_PROJ, f"{region}_{name}.parquet")
    if not os.path.exists(is2file): return # projection is done in pre-processing.
    gdf_als = gpd.read_parquet(is2file)
    laz_project = gpd.read_parquet(f"{ALS_BOUND_PATH}/{region}_{name}.parquet")# wgs84_coordinates. every laz = one row.
    epsg = int(laz_project.iloc[0]['epsg'])
    laz_project = laz_project.to_crs(epsg)# projected
    res_out = output + '/' + region + '/' + name
    os.makedirs( res_out, exist_ok = True)
    cmds = []
    for t in gdf_als['root_file'].unique(): 
        t_df = gdf_als[gdf_als['root_file'] == t]
        for b in  t_df['root_beam'].unique():
            b_t_df = t_df[t_df['root_beam'] == b]
            print(name, t, b, len(b_t_df))
            segment_footprints_list = []
            for index_20m, row_20m in b_t_df.iterrows():
                segment_footprints_list.append(get_footprint(row_20m['e'], row_20m['n'], row_20m['orientation']))
            segment_footprints = pd.concat(segment_footprints_list, ignore_index=True)
            foots = gpd.GeoSeries(gpd.points_from_xy(segment_footprints.e, segment_footprints.n))
            idx = laz_project.sindex.query(foots.union_all())
            files_list = laz_project.iloc[idx]['file'].unique()
        
            las_list = []
            for f in files_list:
                bs = f.replace(region+'_'+name + '_', '').replace('.parquet' , '.las')
                bs_las_path = LAS_PATH + '/' + region + '/' + name + '/' + bs
                las_list.append(bs_las_path)
                if not os.path.isfile(bs_las_path):
                    bs_laz_path = LAZ_PATH + '/' + region + '/' + name + '/LAZ_ground/' + bs[:-1]+ 'z'
                    if os.path.isfile(bs_laz_path):
                        os.makedirs(os.path.dirname(bs_las_path), exist_ok=True)#make sure folder exists.
                        print('laz to las...', bs_laz_path)
                        os.system(f'las2las -i {bs_laz_path} -o {bs_las_path}')
                    else:
                        las_list.remove(bs_las_path)
                        print('no such laz file: ', bs_laz_path)
                        continue
            out_b_t = res_out+ '/df_'+ t[:-3] + '_' + b + '.parquet'
            if os.path.exists(out_b_t): continue # skip writing this file.
            b_t_df.to_parquet(out_b_t) # output is2 segments
            out_coor = res_out+ '/coordinates_'+ t[:-3] + '_' + b + '.txt'
            segment_footprints[['e', 'n']].to_csv(out_coor, sep=' ', header = False,  index = False)
            out_als_list = res_out+ '/alslist_'+ t[:-3] + '_' + b + '.txt'
            with open(out_als_list, 'w') as file:
                for item in las_list:
                    file.write(f"{item}\n")


#@dask.delayed
# coordinates_ATL08_20181201082105_09730106_006_02_gt1l.txt
# alslist_ATL08_20181201082105_09730106_006_02_gt1l.txt
def get_sim_cmds(regions, names, overwrite_wave=False): 
    all_als_lists = []
    cmds = []
    for region, name in zip(regions, names):
        als_list = glob.glob(RES_PATH+'/'+region+'/'+name+'/alslist*.txt')
        all_als_lists.extend(als_list)
    for alsfile in all_als_lists:
        out_coord = alsfile.replace('alslist' , 'coordinates')
        out_wave =  alsfile.replace('alslist', 'wave')[:-4] + '.h5'
        out_sim_log = alsfile.replace('alslist','sim_log') 
        if not os.path.exists(out_wave) or overwrite_wave: # waveform not exist, run simulation; ovewrite= true, run simulation.
            cmd = f'gediRat -fSigma 2.75 -readPulse {Pulse_PATH} -inList {alsfile} -listCoord {out_coord}  -output {out_wave} -hdf  -ground > {out_sim_log}'
            cmds.append(cmd)
    return cmds

def run_cmd(cmd):
    print(cmd)
    os.system(cmd)

#@dask.delayed
def read_waves(out_wave='../result_simV2/usa/neon_niwo2020/wave_ATL08_20230524021757_09731906_006_01_gt3l.h5', lamda=1, ratiopvpg=0.86, overwrite_photon=False):
    out_pho = out_wave.replace('wave_', 'photon_')[:-3] + '.parquet'
    if os.path.exists(out_pho) and not overwrite_photon: return # file exist, and no need to overwrite, return
    print('reading wave file', out_wave)
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
    b_t_df = gpd.read_parquet(out_wave.replace('wave_', 'df_')[:-3] + '.parquet') # read saved data file.
    for index_20m, row_20m in b_t_df.iterrows(): # every 20m segment
        footprint = get_footprint(row_20m['e'], row_20m['n'], row_20m['orientation']) 
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
    res_pho.to_parquet(out_pho) # output simulated photons.                        
    if (len(res_las) ==0): return # not likely to happen
    res_las = pd.concat(res_las, ignore_index=True)
    out_rh = out_wave.replace('wave_', 'rh_')[:-3] + '.parquet'
    res_las.to_parquet(out_rh) # Pandas will silently overwrite the file, if the file is already there

                        
def get_sim_photon(filename, start, end, lamda=1, ratiopvpg=0.86): # for each 20m segment, get the ~30 waveforms.
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
            zStretch = zEnd + (np.arange(wfCount, 0, -1) * ((zStart - zEnd) / wfCount))
            n = np.random.poisson(lamda) # posson sample, how many photons? n=0, 1,2,...
            if n == 0: continue # no sample photons. continue to next waveform
            # canopy + ground photons
            rows = np.arange(RXWAVECOUNT.shape[0])
            scaled_RXWAVECOUNT = (RXWAVECOUNT - GRWAVECOUNT)*ratiopvpg + GRWAVECOUNT*1 # scale the wavefrom by ratiopvpg
            total = sum(scaled_RXWAVECOUNT)
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
    data = np.arange(-14, 15)  # -14 to 14

    e_array = e + data * 0.7 * math.cos(theta)
    n_array = n + data * 0.7 * math.sin(theta)

    # Round coordinates to 6 decimals as floats
    e_array = np.round(e_array, 6)
    n_array = np.round(n_array, 6)

    footprints = pd.DataFrame({'e': e_array, 'n': n_array})
    return footprints  # floats rounded to 6 decimals

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

        
if __name__ == '__main__':
    args = getCmdArgs()
    with open(args.file, 'r') as f:
        process_names = [line.strip() for line in f if line.strip()]
    print(process_names)
    gdf_als = gpd.read_parquet(CALVAL_SITES)
    regions = []
    names = []
    for index, row in gdf_als.iterrows(): 
        if row['name'] not in process_names: continue
        regions.append( row['region'] )
        names.append(row['name'])    
    print('# number of projecs: ', len(names))
    print('# start client')
    client = Client(n_workers=args.workers, threads_per_worker=1) # 
    print(f'# dask client opened at: {client.dashboard_link}') 
    if args.prepare:
        futures = client.map(project2track2beam, regions, names)
        progress(futures)
    if args.sim:
        all_cmds = get_sim_cmds(regions, names)
        print('# number of simulation commands: ', len(all_cmds))
        futures = client.map(run_cmd, all_cmds)
        progress(futures)
    if args.metric:
        allwaves = []
        for region, name in zip(regions, names):
            waves = glob.glob(RES_PATH+'/'+region+'/'+name+'/wave*.h5')
            allwaves.extend(waves)
        futures = client.map(read_waves, allwaves)
        progress(futures)
    print('') 
    client.shutdown()
    sys.exit("# DONE")