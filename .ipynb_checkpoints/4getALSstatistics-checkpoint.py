#!/usr/bin/env python
# coding: utf-8
# project to rerun 12.23.2023
# laz_path = 'europe/ucl_wytham'
# slope calculation is not right...

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

def getCmdArgs():
        p = argparse.ArgumentParser(description = "Get ALS statistics of IS2 over cal/val database")
        p.add_argument("-o", "--out", dest="out", required=True, type=str, help="Output folder, give full path")
        p.add_argument("-r", "--region", dest="region", required=False, type=str, help="region [africa , australia, centralamerica, europe, seasia, southamerica, usa]")
        p.add_argument("-n", "--name", dest="name", required=False, type=str, help="ALS project name [required if using region]")
        cmdargs = p.parse_args()        
        return cmdargs

IS2_20M_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023.parquet'
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet"
FOLDER_PROJ = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_projected_als'
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


def get_is2_in_laz(laz_path):
        als_name = laz_path.split('/')[-3]
        region = laz_path.split('/')[-4]
        out_projected = FOLDER_PROJ + '/' + region  + '_' + als_name  + '.parquet'
        is2_in_als_projected = gpd.read_parquet(out_projected)
        basename = os.path.basename(laz_path)
    ################ this project nsw_portmacuarie4, bound file not exist.
    ################ skip txt file version.
                    # bounds_file = '/gpfs/data1/vclgp/data/gedi/imported/lists/ground_bounds/boundGround.' + als_name+ '.txt'
                    # column_names = ['file', 'xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax']    
                    # df = pd.read_csv(bounds_file, header = None, sep = " ",  names=column_names , encoding='latin1')
                    # #print('# las file : \n', laz_path)
                    # #df.columns = ['file', 'xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax']
                    # basename = os.path.basename(laz_path)
                    # d_file = df[df['file'].str.contains(basename[:-1])] ### or_10_101.laz   
                    # #print( '# laz file bounds: ',d_file.iloc[:, 1:6])
                    # xmin = d_file['xmin'].iloc[0] 
                    # xmax = d_file['xmax'].iloc[0] 
                    # ymin = d_file['ymin'].iloc[0] 
                    # ymax = d_file['ymax'].iloc[0] 
                    # # get laz and nearby laz 
                    # # Create a Polygon geometry from the coordinates
                    # polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
####################use parquet file version. I convert all files.    
        try: 
            bounds_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/out_bounds/' + region+ '_' + als_name+ '_' + basename[:-4] + '.parquet'
            laz_gdf = gpd.read_parquet(bounds_file)
            polygon = laz_gdf.to_crs(laz_gdf.iloc[0]['epsg']).iloc[0]['geometry']
        except Exception as e:
            print(e)
            return
                
        # return is2_in_als -----
        is2_laz = is2_in_als_projected.clip(polygon)    
        return is2_laz

### get slope from als dem 
def calculate_slope_at_xy(dem_file, x, y):
    # Open the DEM file using rasterio
    with rasterio.open(dem_file) as dem:
        # Read the elevation data as a NumPy array
        elevations = dem.read(1)

        # Get the row and column indices corresponding to the (x, y) coordinates
        row, col = dem.index(x, y)

        # Calculate the gradients in both x and y directions at the specified position
        #  dem.res[0] and dem.res[1] are the pixel resolutions in the x and y directions.
        dx, dy = np.gradient(elevations, dem.res[0], dem.res[1], axis=(1, 0))

        # Calculate the slope magnitude at the specified position
        slope_magnitude = np.sqrt(dx[row, col]**2 + dy[row, col]**2)

        # Calculate the slope in degrees
        slope_deg = np.arctan(slope_magnitude) * (180.0 / np.pi)
    return slope_deg



@dask.delayed
def get_als_stats(laz_path, out_folder):
        als_name = laz_path.split('/')[-3]
        region = laz_path.split('/')[-4]
        bs = os.path.basename(laz_path)
        f_out = out_folder + '/' + region + '_' + als_name + '_' +  bs[:-4] + '.parquet'
        if os.path.exists(f_out): return # this file already processed.
        is2_laz = get_is2_in_laz(laz_path)
        if is2_laz is None: return  # no is2 shots in this laz
        ##### get canopy height percentile 
    #### edge case: laz file broken? no ground classification???
        try:
            las = laspy.read(laz_path)
            ground_file = laspy.create(point_format=las.header.point_format, file_version=las.header.version)
            ground_file.points = las.points[las.classification == 2]
            xyz_ground = ground_file.xyz
            xyz = las.xyz
            points_als = gpd.GeoSeries(gpd.points_from_xy(xyz[:,0], xyz[:,1]))
            points_ground = gpd.GeoSeries(gpd.points_from_xy(xyz_ground[:,0], xyz_ground[:,1]))
        except Exception as e:
            print(e)
            return
        res = []
        for index, row in is2_laz.iterrows():
            #is2_in_als_projected_add_ori.loc[index, 'geometry'] = get_rectangle (row['e'], row['n'], row['orientation'])
            rec_20m_13m = get_rectangle(row['e'], row['n'], row['orientation'])
            # points within geometry
            #print('# footprint geomtry:', rec_20m_13m)
            idx = points_als.sindex.query(rec_20m_13m, predicate="contains")
            points_laz = xyz[idx,:]
            if(len(points_laz) == 0): continue
            idx = points_ground.sindex.query(rec_20m_13m, predicate="contains")
            points_laz_ground = xyz_ground[idx,:]
            if(len(points_laz_ground) == 0): continue
            z_ground = np.mean( points_laz_ground[:,2])
            percentiles = np.arange(0, 101, 1)
            height_percentiles = np.percentile(points_laz[:,2]- z_ground, percentiles)
            # data frame from 0 to 100
            column_names = ["als_rh" + str(i) for i in range(101)]  # Creates a list from rh0 to rh100
            ref = pd.DataFrame(columns=column_names)
            # Add your array as a row to the DataFrame
            ref.loc[0] = height_percentiles
            ref['z_ground'] = z_ground
        #### ground 
            percentiles = np.arange(0, 101, 25)
            height_percentiles = np.percentile(points_laz_ground[:,2], percentiles)
            for i, percentile in zip(percentiles, height_percentiles):
                ref['ground_q' + str(i)] = percentile
        #### add als slope
            # /gpfs/data1/vclgp/data/gedi/imported/usa/neon_wref2021/dem
            
            dem_tif = '/gpfs/data1/vclgp/data/gedi/imported/' + region + '/' + als_name + '/dem/' + bs[:-4] + '_dem.tif'
            #if os.path.exists(dem_tif):
            try:
                ref['als_slope_degrees'] = calculate_slope_at_xy(dem_tif, row['e'], row['n'])
            #else:
            except Exception as e:
                ref['als_slope_degrees'] = -999
        #### add ID
            ref['land_segments/longitude_20m'] = row['land_segments/longitude_20m']
            ref['land_segments/latitude_20m'] = row['land_segments/latitude_20m']
            ref['land_segments/delta_time'] = row['land_segments/delta_time']
            res.append(ref)
        if len(res) < 1: return 
        res = pd.concat(res, ignore_index=True)
        res.to_parquet(out_folder + '/' + region + '_' + als_name + '_' +  bs[:-4] + '.parquet') #####???
        return res


def transform_coordinates(row, transformer):
    lat_c = row['land_segments/latitude_20m']
    lon_c = row['land_segments/longitude_20m']
    e, n = transformer.transform(lon_c, lat_c)
    return pd.Series({'e': e, 'n': n})


if __name__ == '__main__':
            args = getCmdArgs()  
            print('## read all is2 in cal/val sites ...')
            gdf_is2 = gpd.read_parquet(IS2_20M_FILE) # 18550387 IS2 20 m segment points
            #### need to reset index, not using h3 level 12 index
            gdf_is2 = gdf_is2.reset_index(drop=True)
            out_folder = args.out
            os.makedirs(out_folder, exist_ok=True)
            if (args.region and not args.name) or (args.name and not args.region):
                sys.exit("Error: Provide both --region and --name at the same time or neither.")
            gdf_als = gpd.read_parquet(ALS_SITES)
            print('## start client')
            client = Client(n_workers=15, threads_per_worker=1) # Client(n_workers=2, threads_per_worker=4) 
            print(f'## -- dask client opened at: {client.dashboard_link}')
            for index, row in gdf_als.iterrows(): ## all sites 
                    if args.region and not (row['region'] == args.region  and row['name']== args.name): continue
                    print('# get stats in valid als sites ...')
                    if row['name'] not in VALID_SITES: continue
                    print('# now processing als project: ', row['region'] + '_' + row['name'])

                    # Open 4getALSstatistics.txt
                    with open('Processing_4getALSstatistics.txt', 'w') as file:
                        # Write text to the file
                        file.write('# index: {}\n'.format(index))
                        file.write('# now processing als project: {}\n'.format(row['region'] + '_' + row['name']))
                    
                    out_projected = FOLDER_PROJ + '/' + row['region'] + '_' + row['name'] + '.parquet'
                    if not os.path.exists(out_projected):
                            als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
                            is2_in_als = gdf_is2.loc[als_index] 
                            is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
                            print("# is2 points in this als project: ", len(is2_in_als))
                            if (len(is2_in_als) <= 1): continue
                            print('# convert segments using EPSG of this project...', row['epsg'])
########################################################################################################
                            # let us assume each project has only one epsg. 
                            transformer = Transformer.from_crs(4326, row['epsg'], always_xy = True)
                            is2_in_als[['e', 'n']] = is2_in_als.apply(lambda row: transform_coordinates(row, transformer), axis=1)
                            geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
                            is2_in_als_projected = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry)
                            is2_in_als_projected_add_ori = add_orientation(is2_in_als_projected)
                            print('# filter NaN values in track orientation')
                            is2_in_als_projected_add_ori = is2_in_als_projected_add_ori.dropna(subset=['orientation'])
                            # just in 
                            is2_in_als_projected_add_ori.to_parquet(out_projected)
##########################################################################################################
                
                    data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground'
                    # convert all laz file to las files
                    files_path = data_path + '/*.laz' 
                    laz_files = glob.glob(files_path)
                    print('# number of laz files: ', len(laz_files))
                    cmds = [get_als_stats(laz_path, out_folder) for laz_path in laz_files]  
                    _ = dask.persist(*cmds)
                    progress(_)
                    del _
                    print('') 
                    print('# als project:', row['region'] + '_' + row['name'], ' is done!') 
            client.close()
            sys.exit("## -- DONE")
# example use 
# python 4getALSstatistics.py --out /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result_als_stat --region usa  --name neon_puum2020
# all sites
# python 4getALSstatistics.py --out /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result_als_stat