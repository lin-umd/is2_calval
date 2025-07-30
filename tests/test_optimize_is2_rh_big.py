
#!/usr/bin/env python
# coding: utf-8

import os
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

# global env.
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'# in als local coordinates.
ALS_BOUND_PATH='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/bounds_project'
LAS_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
LAZ_PATH = '/gpfs/data1/vclgp/data/gedi/imported'
RES_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3' # default output.
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse' # from Amy
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/is2_20m_cal_val_disturbed_20250721.parquet'
CALVAL_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/calval_sites_20250721.parquet" 



def read_waves(out_wave, lamda=1, ratiopvpg=0.86, overwrite_photon=False):
    out_pho = out_wave.replace('wave_', 'photon_')[:-3] + '.parquet'
    if os.path.exists(out_pho) and not overwrite_photon: return # file exist, and no need to overwrite, return
    print('reading wave file', out_wave)
    f_wave = h5py.File(out_wave, 'r')
    byte_strings = f_wave['WAVEID'][()]
     
    wave_ids = [''.join(byte.decode('utf-8') for byte in item) for item in byte_strings]
    # Find common strings
    df1 = pd.DataFrame(wave_ids)
    df1.columns = ['id']
    df1['wave_index'] = df1.index
    print(len(df1))
    
    res_las = []
    res_pho = []
    b_t_df = gpd.read_parquet(out_wave.replace('wave_', 'df_')[:-3] + '.parquet') # read saved data file.
    print(len(b_t_df))
    for _, row_20m in b_t_df[:10].iterrows(): # every 20m segment
        footprint = get_footprint(row_20m['e'], row_20m['n'], row_20m['orientation']) 
        #footprint['id'] = footprint['e'].astype(str) + '.' + footprint['n'].astype(str) 
        footprint['id'] = footprint['e'].map('{:.6f}'.format) + '.' + footprint['n'].map('{:.6f}'.format)
        #print(footprint['id'])
        # find first start index 
        # start = -1; end = -1 # start and end index of the waveforms in this segment
        # for i, id in enumerate(footprint['id']):
        #     if id in df1['id'].values:
        #         start = df1[df1['id'] == id]['wave_index'].iloc[0]  # first index of this id
        #         break
        # # in reverse order, find the last end index
        # for i, id in enumerate(reversed(footprint['id'])): # reverse order 
        #     if id in df1['id'].values:
        #         end = df1[df1['id'] == id]['wave_index'].iloc[0]  # first index of this id
        #         break
        # print(start, end)
        # if start == -1 or end == -1: continue # no waveforms in this segment, skip to next segment.
        res = pd.merge(df1, footprint, on='id', how='inner') #  follow the order of the left DataFrame
        if (len(res) == 0): continue   ##### why is there empty waveform for this is2 20m segment?
        start = res['wave_index'].iloc[0] # must have
        end = res['wave_index'].iloc[-1] # must end>= start.
        print(start, end)
        pho_w_ground = get_sim_photon(f_wave, start, end, lamda, ratiopvpg) # average ~30*3 = 90 photons/rows
        #print(len(pho_w_ground))
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
    f_wave.close()
    if len(res_pho) ==0: return # not likely to happen
    res_pho = pd.concat(res_pho, ignore_index=True)
    #res_pho.to_parquet(out_pho) # output simulated photons.                        
    if (len(res_las) ==0): return # not likely to happen
    res_las = pd.concat(res_las, ignore_index=True)
    print(res_las)
    #out_rh = out_wave.replace('wave_', 'rh_')[:-3] + '.parquet'
    #res_las.to_parquet(out_rh) # Pandas will silently overwrite the file, if the file is already there

def get_sim_photon(f, start, end, lamda=1, ratiopvpg=0.86):
    wfCount = f['NBINS'][0]
    if wfCount < 1:
        return None

    z_bins = np.arange(wfCount, 0, -1)
    data = []

    RX = f['RXWAVECOUNT'][start:end+1]
    GR = f['GRWAVECOUNT'][start:end+1]
    Z0 = f['Z0'][start:end+1]
    ZN = f['ZN'][start:end+1]
    ZG = f['ZG'][start:end+1]
    LON0 = f['LON0'][start:end+1]
    LAT0 = f['LAT0'][start:end+1]

    for i in range(end - start + 1):
        if np.isnan(ZG[i]):
            continue

        zStretch = ZN[i] + z_bins * ((Z0[i] - ZN[i]) / wfCount)
        n = np.random.poisson(lamda)
        if n == 0:
            continue

        scaled = (RX[i] - GR[i]) * ratiopvpg + GR[i]
        total = scaled.sum()
        if total <= 0:
            continue

        probs = scaled / total
        photon_rows = np.random.choice(len(probs), size=n, p=probs)

        z = zStretch[photon_rows] - ZG[i]
        gflag = GR[i][photon_rows] > 0

        for z_, gf in zip(z, gflag):
            data.append((z_, LON0[i], LAT0[i], gf))

    if not data:
        return None

    return pd.DataFrame(data, columns=["Z", "LON0", "LAT0", "ground_flag"])


# def get_sim_photon(f, start, end, lamda=1, ratiopvpg=0.86): # for each 20m segment, get the ~30 waveforms.
#     pho_w_ground = []
#     N_foorprints = f['RXWAVECOUNT'].shape[0]        
#     for i in range(start, end+1): # inclusive for i th waveform
#         RXWAVECOUNT = f['RXWAVECOUNT'][i]
#         GRWAVECOUNT = f['GRWAVECOUNT'][i]
#         zStart = f["Z0"][i]
#         zEnd = f["ZN"][i]
#         zG = f["ZG"][i] # can be NaN
#         wfCount = f["NBINS"][0]
#         if (wfCount < 1): continue
#         wfStart = 1
#         zStretch = zEnd + (np.arange(wfCount, 0, -1) * ((zStart - zEnd) / wfCount))
#         n = np.random.poisson(lamda) # posson sample, how many photons? n=0, 1,2,...
#         if n == 0: continue # no sample photons. continue to next waveform
#         # canopy + ground photons
#         rows = np.arange(RXWAVECOUNT.shape[0])
#         scaled_RXWAVECOUNT = (RXWAVECOUNT - GRWAVECOUNT)*ratiopvpg + GRWAVECOUNT*1 # scale the wavefrom by ratiopvpg
#         total = sum(scaled_RXWAVECOUNT)
#         p_data = [value / total for value in scaled_RXWAVECOUNT]
#         photon_rows = np.random.choice(rows, size=n, p=p_data)
#         df1 = zStretch[photon_rows] - zG
#         df1 = pd.DataFrame(df1,  columns= ["Z"])
#         df1['LON0'] = f["LON0"][i]
#         df1['LAT0'] = f["LAT0"][i]
#         df1.dropna(inplace=True) # drop nan rows, NaN caused by zG.
#         if len(df1) == 0: continue # continue to next waveform
#         # add a ground flag to df1 
#         df1['ground_flag'] = GRWAVECOUNT[photon_rows] > 0 
#         if not df1.empty:
#             pho_w_ground.append(df1)

#     if pho_w_ground:
#         return pd.concat(pho_w_ground, ignore_index=True)
#     return None
    
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
        
if __name__ == '__main__':
    read_waves('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3/africa/tanzania_wwf_germany/wave_ATL08_20220513223350_07911514_006_01_gt2l.h5')
