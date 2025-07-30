#!/usr/bin/env python
# coding: utf-8
'''
this script is to combine is2 data with simulation metrics and als rh metrics. it also export plots of is2 vs als data.
# example use
# python /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/src/export_metrics.py --name neon_abby2019
'''

import glob
import pyproj
import os
os.environ['USE_PYGEOS'] = '0' # not use it. 
import geopandas as gpd
import pandas as pd
import re
import sys
import argparse
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend
import numpy as np
import datashader as ds
from datashader.mpl_ext import dsshow
import matplotlib.ticker as ticker
# # 18550387 IS2 20 m segment points
IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023.parquet'# too big to read.

FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'
#ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet" 
CALVAL_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/calval_sites_20250721.parquet" 
SIM_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3'
ALS_STAT_PATH  = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/result_als_stat'
OUT='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_calval_metrics'
VALID_SITES = ['amani','csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_venetia', 'csir_welverdient', 'drc_ghent_field_32635', 
               'drc_ghent_field_32733', 'drc_ghent_field_32734', 'gsfc_mozambique', 'jpl_lope', 'jpl_rabi', 'tanzania_wwf_germany', 'khaoyai_thailand', 
               'chowilla', 'credo', 'karawatha', 'litchfield', 'rushworth_forests', 'tern_alice_mulga', 'tern_robson_whole', 'costarica_laselva2019', 
               'skidmore_bayerischer', 'zofin_180607', 'spain_exts1', 'spain_exts2', 'spain_exts3', 'spain_exts4', 'spain_leonposada', 'spain_leon1', 
               'spain_leon2', 'spain_leon3', 'jpl_borneo_004', 'jpl_borneo_013', 'jpl_borneo_040', 'jpl_borneo_119', 'jpl_borneo_144', 'chave_paracou', 
               'embrapa_brazil_2020_and_a01', 'embrapa_brazil_2020_bon_a01', 'embrapa_brazil_2020_cau_a01', 'embrapa_brazil_2020_duc_a01', 
               'embrapa_brazil_2020_hum_a01', 'embrapa_brazil_2020_par_a01', 'embrapa_brazil_2020_rib_a01', 'embrapa_brazil_2020_tal_a01',
               'embrapa_brazil_2020_tan_a01', 'embrapa_brazil_2020_tap_a01', 'embrapa_brazil_2020_tap_a04', 'walkerfire_20191007', 
               'neon_abby2018', 'neon_abby2019', 'neon_abby2021', 'neon_bart2018', 'neon_bart2019', 'neon_blan2019', 'neon_blan2021', 
               'neon_clbj2018', 'neon_clbj2019', 'neon_clbj2021', 'neon_clbj2022', 'neon_dela2018', 'neon_dela2019', 'neon_dela2021', 
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
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978']


def get_project_simulation_results(region, name):
    data_sim = os.path.join(SIM_PATH, region, name, 'rh_*.parquet') 
    files = glob.glob(data_sim)
    res = []
    for f in files:
        df = pd.read_parquet(f)
        df.dropna(inplace=True) #    
        if len(df) >0: res.append(df)
    if len(res) == 0: return
    df_is2_sim = pd.concat(res, ignore_index=True)
    return df_is2_sim

def get_project_als_stats(region, name):
    data_path = os.path.join(ALS_STAT_PATH, region, name, 'df_ATL08*.parquet')
    files = glob.glob(data_path)
    res = []
    for f in files:
        df = pd.read_parquet(f)
        df.dropna(inplace=True) 
        if len(df) >0: res.append(df)
    if len(res) == 0: return
    df_is2_als = pd.concat(res, ignore_index=True)
    return df_is2_als

def get_stat(x,y):
    corr_matrix = np.corrcoef(x, y)
    corr = corr_matrix[0,1]
    R_sq= corr**2
    N = len(x)
    # Calculate the squared differences
    squared_diff = (x -y)**2
    # Calculate the mean squared difference
    mean_squared_diff = np.mean(squared_diff)
    # Calculate the RMSE
    rmse = np.sqrt(mean_squared_diff)
    bias = np.mean(x - y)
    return R_sq, rmse, N, bias

def get_x_y(row, col, final):
    filtered_df = final.copy() # filter out the outliers.
    if len(filtered_df) < 2: return
    y = filtered_df['land_segments/canopy/h_canopy_20m']
    if row ==0 and col == 0:
        x = filtered_df['als_rh98']
        return x, y, 'als_rh98'
    if row ==0 and col == 1:
        x = filtered_df['rh98']
        return x, y, 'rh98'
    if row ==0 and col == 2:
        x = filtered_df['h_canopy_98']
        return x, y, 'h_canopy_98'
    filtered_df = filtered_df[filtered_df['land_segments/night_flag'] >0 ]
    filtered_df = filtered_df[filtered_df['strong_flag'] == True ]
    if len(filtered_df) < 2: return
    y = filtered_df['land_segments/canopy/h_canopy_20m']
    if row ==1 and col == 0:
        x = filtered_df['als_rh98']
        return x, y,'als_rh98'
    if row ==1 and col == 1:
        x = filtered_df['rh98']
        return x, y, 'rh98'
    if row ==1 and col == 2:
        x = filtered_df['h_canopy_98']
        return x, y, 'h_canopy_98'
def plot_metrics(final, region, name):
        fig, axs = plt.subplots(2, 3, figsize=(11, 5))
        #cmaps = ['RdBu_r', 'viridis', 'viridis']
        for col in range(3):
            for row in range(2):
                ax = axs[row, col]    
                try:
                    x,y, x_var=get_x_y(row, col, final)
                except Exception as e:
                    print('no data!')
                    continue # go to next loop for extraction.
                R_sq, rmse, N , bias= get_stat(x,y)
                #pcm = ax.hist2d(x, y, bins=(50, 50), cmap='Reds', vmax = 40, range=[[0,100],[0,100]])# xmin, xmax, ymin, ymax
                #ax.scatter(x, y, color='blue', marker='o', label='IS2 points', s=0.5)
                pcm = using_datashader(ax, x, y)
                #norm =  density_scatter( x, y, ax = ax,  bins = [30,30])
                ax.text(0.1, 0.97, f'$R^2$={round(R_sq, 2)}', transform=ax.transAxes, fontsize=10, va='top')
                
                ax.text(0.1, 0.87, f'N={N}', transform=ax.transAxes, fontsize=10, va='top')
                ax.text(0.1, 0.77, f'Bias={round(bias, 2)}', transform=ax.transAxes, fontsize=10, va='top')
                ax.text(0.1, 0.67, f'RMSE={round(rmse, 2)}', transform=ax.transAxes, fontsize=10, va='top')
                # Add labels and title
                if ax in axs[1,:]:
                    ax.set_xlabel(x_var)
                    ax.text(0.8, 0.9, f'S+N', transform=ax.transAxes, fontsize=10, va='top')
                if ax in axs[:,0]:
                    ax.set_ylabel('IS2 h_canopy_20m')
                #plt.title('Height comparison')
                ax.set_xlim(0,80)
                ax.set_ylim(0,80)     
        cbar = fig.colorbar(pcm, ax=axs[:,:], shrink=0.9)
        cbar.ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))  # Ensure integer labels
        plt.savefig(OUT+ '/' + region + '_' + name + '.jpeg', dpi=300)
        #plt.show()
        plt.close()


def using_datashader(ax, x, y):
    df = pd.DataFrame(dict(x=x, y=y))
    dsartist = dsshow(
        df,
        ds.Point("x", "y"),
        ds.count(),
        vmin=0,
        vmax=20,
        norm="linear",
        aspect="auto",
        ax=ax,
        x_range = [0, 100], 
        y_range = [0, 100],
        plot_width=100,
        plot_height=100,
        #cmap='Reds',
    )
    return dsartist


if __name__ == '__main__':
    parse = argparse.ArgumentParser(description="Export icesat-2 cal/val metrics")
    parse.add_argument("--name", dest="name", help="Specific project name, if not, process all.", type=str, required=False)
    args = parse.parse_args()
    print('## read als bounds in cal/val sites ...')
    gdf_als = gpd.read_parquet(CALVAL_SITES)
    columns_to_concat = ['land_segments/latitude_20m', 'land_segments/longitude_20m', 'land_segments/delta_time']
    for index, row in gdf_als.iterrows(): ## all sites 
        region = row['region']
        name = row['name']
        if name not in VALID_SITES: continue
        if args.name and name != args.name: continue
        print('## processing als project name: ', name)
        f_out = OUT+ '/' + region + '_' + name + '.parquet'
        gdf_is2 = gpd.read_parquet(os.path.join(FOLDER_PROJ, f"{region}_{name}.parquet"))
        if os.path.exists(f_out):
            print(f"The file {f_out} exists.")
            final = gpd.read_parquet(f_out)
        else:
            try: 
                df_sim = get_project_simulation_results(region, name)
                df_als = get_project_als_stats(region, name)       
                df_sim_als = pd.merge(df_sim, df_als, on=columns_to_concat)
            except Exception as e:
                print(e)
                print(region, name)
                continue
            
            final = pd.merge(gdf_is2, df_sim_als, on=columns_to_concat)
            # strong flag
            final['strong_flag'] = ((final['root_beam'].isin(['gt1r', 'gt2r', 'gt3r'])) & \
                                    (final['orbit_info/sc_orient'] == 1)) | \
                                    ((final['root_beam'].isin(['gt1l', 'gt2l', 'gt3l'])) & \
                                        (final['orbit_info/sc_orient'] == 0))
            
            final.to_parquet(f_out)
        print('## making plots...')
        plot_metrics(final, region, name)
        #### save plot 
    sys.exit("## DONE")