#!/usr/bin/env python
# coding: utf-8
'''
# project to rerun 12.23.2023
# laz_path = 'europe/ucl_wytham'
# example use 
python src/get_segment_als_statistics.py --out /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/result_als_stat --region_name  usa/neon_puum2020
python src/get_segment_als_statistics.py --out /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/result_als_stat --workers 40

clip by is2 footprints. not by laz file. 
some laz file are splitted from same tile because the tile is large > 75Mb.
'''

import os
os.environ['USE_PYGEOS'] = '0'
import glob 
import argparse
import geopandas as gpd
from pyproj import Transformer
from shapely.geometry import Point, Polygon
import numpy as np
import math
import matplotlib.pyplot as plt
import laspy
import pandas as pd 
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import sys
import rasterio
from matplotlib.path import Path


IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023.parquet'
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet"
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'
OUT='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/result_als_stat'
laz_PATH = '/gpfs/data1/vclgp/data/gedi/imported'
ALS_BOUND_PATH='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/bounds_project'
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
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978']
def getCmdArgs():
        p = argparse.ArgumentParser(description = "Get ALS statistics of IS2 over cal/val database")
        p.add_argument("-o", "--out", dest="out", required=False, default=OUT, type=str, help="Output folder, give full path")
        p.add_argument("-r", "--region_name", dest="region_name", required=False, type=str, help="example, e.g. usa/neon_leno2018")
        p.add_argument("-n", "--workers", dest="workers", required=False, default=5, type=int, help="number of cores")
        cmdargs = p.parse_args()        
        return cmdargs
    
    
def add_orientation(is2_in_als_projected):
    df = is2_in_als_projected
    root_files = df['root_file'].unique()
    beams = ['gt1l', 'gt1r','gt2l', 'gt2r','gt3l', 'gt3r']
    for f in root_files:
        for beam in beams:
            tmp = df[ (df['root_file'] == f )&  (df['root_beam'] == beam)]
            if (len(tmp) < 2): continue
            orientation, b = np.polyfit(tmp['e'], tmp['n'], 1) # best fit direction. Polynomial coefficients
            df.loc[(df['root_file'] == f )&  (df['root_beam'] == beam), 'orientation'] = orientation
    return df
    

def get_rectangle (e = 570766.020196, n = 5.075631e+06, orient = -9.315458):
        orient_rad = math.atan(orient)   # orient = delta_y/delta_x
        # Half of the rectangle dimensions
        half_length = 10  # half of the length (20/2)
        half_width = 6.5  # half of the width (13/2)
        x_coords = [e + half_length * np.cos(orient_rad) - half_width * np.sin(orient_rad),
                    e + half_length * np.cos(orient_rad) + half_width * np.sin(orient_rad),
                    e - half_length * np.cos(orient_rad) + half_width * np.sin(orient_rad),
                    e - half_length * np.cos(orient_rad) - half_width * np.sin(orient_rad),
                    e + half_length * np.cos(orient_rad) - half_width * np.sin(orient_rad)]
        
        y_coords = [n + half_length * np.sin(orient_rad) + half_width * np.cos(orient_rad),
                    n + half_length * np.sin(orient_rad) - half_width * np.cos(orient_rad),
                    n - half_length * np.sin(orient_rad) - half_width * np.cos(orient_rad),
                    n - half_length * np.sin(orient_rad) + half_width * np.cos(orient_rad),
                    n + half_length * np.sin(orient_rad) + half_width * np.cos(orient_rad)]
        rectangle = Polygon(zip(x_coords, y_coords))
        return rectangle


# slope from ground points
def calculate_slope_from_points(xyz_ground):
    # Extract coordinates
    X = xyz_ground[:, 0]
    Y = xyz_ground[:, 1]
    Z = xyz_ground[:, 2]

    # Build design matrix for plane fit: z = a*x + b*y + c
    A = np.c_[X, Y, np.ones(X.shape)]

    # Solve for plane coefficients [a, b, c] via least squares
    coeff, _, _, _ = np.linalg.lstsq(A, Z, rcond=None)  # coeff = [a, b, c]

    a, b, _ = coeff

    # Plane normal vector = [-a, -b, 1]
    normal = np.array([-a, -b, 1])
    normal /= np.linalg.norm(normal)

    # Slope = angle between normal and vertical axis (0,0,1)
    slope_rad = np.arccos(np.dot(normal, [0, 0, 1]))
    slope_deg = np.degrees(slope_rad)

    return slope_deg



def merge_clipped_las(laz_files, rectangle):
    xyz_all = []
    ground_xyz_all = []
    for laz_path in laz_files:
        try:
            las = laspy.read(laz_path)
            x = las.x
            y = las.y
            poly_path = Path(rectangle.exterior.coords)
            points = np.column_stack((x, y))  # shape (N, 2)
            mask = poly_path.contains_points(points)
            clipped = laspy.create(point_format=las.header.point_format, file_version=las.header.version)
            clipped.points = las.points[mask]
            ground_xyz = clipped.xyz[clipped.classification == 2]
            xyz = clipped.xyz
            if len(clipped) > 0:
                xyz_all.append(xyz)
                ground_xyz_all.append(ground_xyz)
                #print(f"Clipped {len(clipped)} points from {os.path.basename(laz_path)}")
            else:
                pass
                #print(f"No points in {os.path.basename(laz_path)} inside polygon")
        except Exception as e:
            print(f"Error reading {laz_path}: {e}")
            return None, None
    if len(xyz_all) == 0 or len(ground_xyz_all) == 0:
        #print("No points found in any file. Output not written.")
        return None, None
    return np.concatenate(xyz_all, axis=0), np.concatenate(ground_xyz_all, axis=0)


@dask.delayed
def process_beam_group(region, name, f, res_out):
    out_b_t = f.replace('/is2_', '/df_')
    if os.path.exists(out_b_t): return
    b_t_df=gpd.read_parquet(f)
    laz_project = gpd.read_parquet(f"{ALS_BOUND_PATH}/{region}_{name}.parquet")# wgs84_coordinates. every laz = one row.
    epsg = int(laz_project.iloc[0]['epsg'])
    laz_project = laz_project.to_crs(epsg)# projected
    res = []
    for index_20m, row_20m in b_t_df.iterrows():
        rec_20m_13m = get_rectangle(row_20m['e'], row_20m['n'], row_20m['orientation'])       
        idx = laz_project.sindex.query(rec_20m_13m)
        if len(idx) == 0: continue
        files_list = laz_project.iloc[idx]['file'].unique()
        laz_list = []
        for f in files_list:
            bs = f.replace(region + '_' + name + '_', '').replace('.parquet' , '.laz')
            bs_laz_path = laz_PATH + '/' + region + '/' + name + '/LAZ_ground/' + bs
            laz_list.append(bs_laz_path)
        points_laz, points_laz_ground = merge_clipped_las(laz_list, rec_20m_13m) # return arrays
        if points_laz is None or points_laz.shape[0] == 0:
            continue
        if points_laz_ground is None or points_laz_ground.shape[0] == 0:
            continue
        z_ground = np.median( points_laz_ground[:,2])
        percentiles = np.arange(0, 101, 1)
        height_percentiles = np.percentile(points_laz[:,2] - z_ground, percentiles)
        column_names = ["als_rh" + str(i) for i in range(101)]  # Creates a list from rh0 to rh100
        ref = pd.DataFrame(columns=column_names)
        ref.loc[0] = height_percentiles
        ref['z_ground'] = z_ground
        percentiles = np.arange(0, 101, 25)
        height_percentiles = np.percentile(points_laz_ground[:,2], percentiles) 
        for i, percentile in zip(percentiles, height_percentiles):
            ref['ground_q' + str(i)] = percentile
        try:
            ref['als_slope_degrees'] = calculate_slope_from_points(points_laz_ground)
        except Exception as e:
            ref['als_slope_degrees'] = -999
        ref['land_segments/longitude_20m'] = row_20m['land_segments/longitude_20m']
        ref['land_segments/latitude_20m'] = row_20m['land_segments/latitude_20m']
        ref['land_segments/delta_time'] = row_20m['land_segments/delta_time']
        res.append(ref)
    if len(res) < 1: return None
    res = pd.concat(res, ignore_index=True)
    print('## -- writing als stats', out_b_t)
    res.to_parquet(out_b_t) 
    return out_b_t

@dask.delayed
def get_als_stats_per_project(region, name, out_folder): # per 20m-segment. 
    is2file=os.path.join(FOLDER_PROJ, f"{region}_{name}.parquet")
    if not os.path.exists(is2file): return # projection is done in pre-processing.
    gdf_als = gpd.read_parquet(is2file)
    res_out = os.path.join(out_folder, region, name)
    os.makedirs( res_out, exist_ok = True)
    for t in gdf_als['root_file'].unique(): 
        t_df = gdf_als[gdf_als['root_file'] == t]
        for b in  t_df['root_beam'].unique():
            b_t_df = t_df[t_df['root_beam'] == b]
            out_b_t = res_out+ '/is2_'+ t[:-3] + '_' + b + '.parquet'####o
            if os.path.exists(out_b_t): continue # skip this file.
            b_t_df.to_parquet(out_b_t) # output is2 segments per track per beam.

if __name__ == '__main__':
    args = getCmdArgs()  
    os.makedirs(args.out, exist_ok=True)
    regions=[]
    names=[]
    if args.region_name:
        parts = args.region_name.split("/")
        regions.append(parts[0])
        names.append(parts[1])
    else:
        gdf_als = gpd.read_parquet(ALS_SITES)
        gdf_als = gdf_als[gdf_als['name'].isin(VALID_SITES)]
        print('## -- read calval sites')
        regions = gdf_als['region'].tolist()
        names = gdf_als['name'].tolist()
    print('## -- start client')
    client = Client(n_workers=args.workers, threads_per_worker=1) 
    print(f'## -- dask client opened at: {client.dashboard_link}')
    print('## -- is2 project into per track per beam')
    project_tasks = [get_als_stats_per_project(region, name, args.out) for region, name in zip(regions, names)]  
    futures = dask.persist(*project_tasks)
    cmds = []
    for region, name in zip(regions, names):
        res_out = os.path.join(args.out, region, name)
        df_files = glob.glob(res_out+ '/is2_*.parquet')
        for f in df_files:
            cmds.append(process_beam_group(region, name, f, res_out))  
    print('## -- clip als per 20m segment and get metrics')
    futures2 = dask.persist(*cmds)
    progress(futures2)
    client.close()
    sys.exit("## -- DONE")