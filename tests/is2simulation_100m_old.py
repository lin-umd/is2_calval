#!/usr/bin/env python
# coding: utf-8
from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import geopandas as gpd 
import numpy as np
import os
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

TMP_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_in_als_projected_100m.parquet'
# processing file track 
LOG = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/processing_100m.txt'
SIM_LOG = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/is2_calval/running_100m.log'
# variables 
LAS_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
RES_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result_100m'
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse' # from Amy
# data files 
#IS2_100m_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_100m_calval_09252023.parquet'
IS2_100m_FILE = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_100m_misha_11302023.parquet'
#ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/sites_20221006.gpkg"
ALS_SITES = "/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/misha_bounds_epsg_20231128.gpkg"

@dask.delayed
def sim_is2_laz(laz_path):
                    #laz_path = '/gpfs/data1/vclgp/data/gedi/imported/usa/usda_or/LAZ_ground/or_10_101.laz'

                    # if 'tile_36920_-2716680_utm_0000000' not in laz_path: 
                    #     return None # only check this file
                    print('# processing laz file', laz_path)
                    is2_in_als_projected = gpd.read_parquet(TMP_FILE)
                    als_name = laz_path.split('/')[-3]
                    region = laz_path.split('/')[-4]
                    bounds_file = '/gpfs/data1/vclgp/data/gedi/imported/lists/ground_bounds/boundGround.' + als_name+ '.txt'
                    column_names = ['file', 'xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax']
                    df = pd.read_csv(bounds_file, header = None, sep = " ",  names=column_names , encoding='latin1')
                    #print('# las file : \n', laz_path)
                    #df.columns = ['file', 'xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax']
                    basename = os.path.basename(laz_path)
                    d_file = df[df['file'].str.contains(basename[:-1])] ### or_10_101.laz    
                    #print( '# laz file bounds: ',d_file.iloc[:, 1:6])
                    xmin = d_file['xmin'] 
                    xmax = d_file['xmax'] 
                    ymin = d_file['ymin'] 
                    ymax = d_file['ymax'] 
                    # get laz and nearby laz 
                    # Create a Polygon geometry from the coordinates
                    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
                    # return is2_in_als -----
                    is2_laz = is2_in_als_projected.clip(polygon)
                    #print('# is2_laz: \n', is2_laz)
                    if (len(is2_laz) == 0):
                        print('')
                        print('# no is2 20m segment is in this las file!', laz_path)
                        return None
                    # get footprints 
                    # get_footprint(e, n, slope): 
                    segment_footprints_list = []
                    for index_100m, row_100m in is2_laz.iterrows():
                        segment_footprints_list.append(get_footprint(row_100m['e'], row_100m['n'], row_100m['orientation']))
                    segment_footprints = pd.concat(segment_footprints_list, ignore_index=True)
                    # create result folder /region/name/
                    res_out = RES_PATH + '/' + region + '/' + als_name 
                    os.makedirs( res_out , exist_ok = True)
                    out_coor = res_out+ '/coordinates_'+ basename[:-4] + '.txt'####
                    out_wave = res_out+ '/wave_'+ basename[:-4] + '.h5'
                    # if os.path.isfile(out_wave):
                    #         print(f"The file {out_wave} exists.") 
                    #         return None
                    segment_footprints[['e', 'n']].to_csv(out_coor, sep=' ', header = False,  index = False)
                    
                    # /gpfs/data1/vclgp/data/gedi/imported/usa/usda_or/ALS_ground/or_10_101.las
                    laz_path = laz_path.replace('ALS_ground', 'LAZ_ground').replace('.las', '.laz')
                    las_out = LAS_PATH + '/' + region + '/' + als_name
                    os.makedirs( las_out , exist_ok = True)

                    bs = os.path.basename(laz_path)
                    bs_las = bs[:-1] + 's'
                    # check if las file exist:
                    bs_las_path = las_out+'/'+bs_las
                    if not os.path.isfile(bs_las_path):
                            #print('not exist')
                            print('# converting laz file: ', laz_path )
                            os.system(f'las2las -i {laz_path} -odir {las_out} -olas')
                    #print('bs_las_path', bs_las_path)
                    os.system(f'gediRat -fSigma 2.75 -readPulse {Pulse_PATH} -input {bs_las_path} -listCoord {out_coor}  -output {out_wave} -hdf  -ground > {SIM_LOG}')
                    # if out_wave is done. 
                    # remove this las file 
    
                    #os.remove(bs_las_path) # not remove yet 

                    if not os.path.isfile(out_wave): 
                        return None # edge case not exist. 
                        
                    else:
                        print("# get rh from waveform ...")
                        f_wave = h5py.File(out_wave, 'r')
                        byte_strings = f_wave['WAVEID'][()]
                        f_wave.close() 
                        wave_ids = []
                        for item in byte_strings:
                            # Convert each byte string to a regular string and join them
                            result_string = ''.join([byte.decode('utf-8') for byte in item])
                            wave_ids.append(result_string)
                        # Find common strings
                        df1 = pd.DataFrame(wave_ids)
                        df1.columns = ['id']
                        df1['wave_index'] = df1.index
                        #print("Keys in HDF5 file:", list(f_wave.keys()))
                        ########## loop every segment 
                        res_las = []
                        for index_100m, row_100m in is2_laz.iterrows():
                                    footprint = get_footprint(row_100m['e'], row_100m['n'], row_100m['orientation'])
                                    # get unique footprint ID like string. 
                                    # Concatenate elements with '.' separator
                                    footprint['id'] = footprint['e'].astype(str) + '.' + footprint['n'].astype(str)
                                    #is2_footprintID = [str(e) + '.' + str(n) for e, n in zip(e_array, n_array)]
                                    #print('# is2_footprintID', is2_footprintID)
                                    ##### I have utm now, return waveid  rowindex start and end. 
                                    # get wave id start index
                                    #df2 = pd.DataFrame(is2_footprintID)
                                    #df2.columns = ['id']
                                    res = pd.merge(df1, footprint, on='id', how='inner')
                                    if (len(res) == 0): continue   ##### why is there empty waveform for this is2 20m segment?
                                    # las files boundary is from [min, max]
                                    #print('res:' , res)
                                    start = res['wave_index'].iloc[0]
                                    end = res['wave_index'].iloc[-1]
                                    #print('start, end: ', start, end)
                                    rh = get_sim_rh(out_wave, start, end)
                                    if rh is not None: 
                                        # lon, lat, time ---> unique ID. 
                                        rh['land_segments/longitude'] = row_100m['land_segments/longitude']
                                        rh['land_segments/latitude'] = row_100m['land_segments/latitude']
                                        rh['land_segments/delta_time'] = row_100m['land_segments/delta_time']
                                        #print('# rh: ', rh)
                                        res_las.append(rh)
                        #print('rh list in each las file: ',res_las)  
                        if (len(res_las) ==0):
                            return None
                        res_las = pd.concat(res_las, ignore_index=True)
                        out_rh = res_out+ '/rh_'+ basename[:-4] + '.parquet' # each las file, give me rh.
                        #print('# write to parquet file ...')
                        res_las.to_parquet(out_rh) # Pandas will silently overwrite the file, if the file is already there
                        

# get rh from one waveform .

def get_sim_rh(filename, start, end):
                    # Canopy height, ch
                    # For a 100 m segment, the cumulative height distribution of canopy photons is 
                    # generated and the height at the 98th percentile.
                    with h5py.File(filename, "r") as f:
                        # Print all root level object names (aka keys) 
                        # these can be group or dataset names 
                        #print("Keys: %s" % f.keys())
                        N_foorprints = f['RXWAVECOUNT'].shape[0]
                        pho_w_ground = pd.DataFrame()

                        for i in range(start, end+1): # inclusive
                                    
                                    RXWAVECOUNT = f['RXWAVECOUNT'][i]
                                    GRWAVECOUNT = f['GRWAVECOUNT'][i]
                                    zStart = f["Z0"][i]
                                    zEnd = f["ZN"][i]
                                    zG = f["ZG"][i]
                                    wfCount = f["NBINS"][0]
                                    if (wfCount < 1): continue
                                    wfStart = 1
                                    # Calculate zStretch
                                    zStretch = zEnd + (np.arange(wfCount, 0, -1) * ((zStart - zEnd) / wfCount))
                                    #plt.plot(RXWAVECOUNT, zStretch,color='red' )
                                    #plt.plot(RXWAVECOUNT - GRWAVECOUNT, zStretch,color='black', linestyle='--' )
                                    #plt.show()
                                    # sampling 
                                    n = np.random.poisson(3,1)[0] # posson sample, how many photons?
                                    
                                    # canopy + ground photons
                                    rows = np.arange(RXWAVECOUNT.shape[0])
                                    total = sum(RXWAVECOUNT)
                                    
                                    # Normalize the data by dividing each value by the total
                                    p_data = [value / total for value in RXWAVECOUNT]
                                    photon_rows = np.random.choice(rows, size=n, p=p_data)
                                    df1 = zStretch[photon_rows] - zG
                                    df1 = pd.DataFrame(df1)
                                    # add a ground flag to df1 
                                    df1['ground_flag'] = GRWAVECOUNT[photon_rows] > 0 
                                    if (len(pho_w_ground) == 0):
                                        pho_w_ground = df1
                                    else:
                                        #print(df1)
                                        pho_w_ground = pd.concat([pho_w_ground, df1], ignore_index=True)

                    f.close()
                    percentiles = np.arange(1, 101, 1)
                    pho_no_ground = pho_w_ground[pho_w_ground['ground_flag'] == False]
                    if (len(pho_no_ground) == 0):
                        return None
                    else:
                        height_percentiles = np.percentile(pho_no_ground, percentiles)
                        ch_98 = height_percentiles[97] ### h_98
                    # rh 
                    percentiles = np.arange(0, 101, 1)
                    #0 --> min height
                    # 100 --> max height
                    height_percentiles = np.percentile(pho_w_ground, percentiles)
                    # data frame from 0 to 100
                    column_names = ["rh" + str(i) for i in range(101)]  # Creates a list from rh0 to rh100
                    sim = pd.DataFrame(columns=column_names)
                    # Add your array as a row to the DataFrame
                    sim.loc[0] = height_percentiles
                    sim['h_canopy_98'] = ch_98
                    return sim

def get_footprint(e, n, slope): 
    theta = math.atan(slope)
    #data = np.arange(-14, 15) # -14 --14# 20/0.7 = 28.6  (-72,72)   100/0.7=143
    data = np.arange(-72, 72) # -14 --14#  (-72,72)   100/0.7=143
    e_array = e + data * 0.7*math.cos(theta)
    n_array = n + data * 0.7*math.sin(theta)
    e_array = [f'{round(e, 6):.6f}' for e in e_array] # round
    n_array = [f'{round(e, 6):.6f}' for e in n_array] # round
    footprints = pd.DataFrame({'e': e_array, 'n': n_array})# .round(2).applymap(lambda x: f'{x:.2f}')  # keep 2 decimals.
    return footprints

#### get orientation 
'''
array(['orbit_info/sc_orient', 'root_file', 'root_beam',
       'land_segments/delta_time', 'land_segments/terrain/h_te_best_fit',
       'land_segments/canopy/n_ca_photons', 'land_segments/night_flag',
       'land_segments/canopy/h_canopy', 'land_segments/latitude',
       'land_segments/longitude', 'geometry'], dtype=object)
add e and n
'''
def add_orientation(is2_in_als_projected):
#def add_orientation (file, o_path): # need root_file, root_beam, 
    df = is2_in_als_projected
    # group by root file and beam
    root_files = df['root_file'].unique()
    beams = ['gt1l', 'gt1r','gt2l', 'gt2r','gt3l', 'gt3r']
    for f in root_files:
        for beam in beams:
            # get single beam track data
            tmp = df[ (df['root_file'] == f )&  (df['root_beam'] == beam)]
            if (len(tmp) < 1): continue
            #orientation, intercept, r_value, p_value, std_err = stats.linregress(en['east'], en['north'])
            #print(f, beam)
            # y=m*x+c  # numpy.polyfit( x , y , deg)
            orientation, b = np.polyfit(df['e'], df['n'], 1) # best fit direction. Polynomial coefficients
            
            # assign the orientation value to current track
            df.loc[(df['root_file'] == f )&  (df['root_beam'] == beam), 'orientation'] = orientation
    return df    





if __name__ == '__main__':

            print('## read is2 in cal/val sites ...')
            gdf_is2 = gpd.read_parquet(IS2_100m_FILE) # 18550387 IS2 20 m segment points
            #### need to reset index, not using h3 level 12 index
            gdf_is2 = gdf_is2.reset_index(drop=True)
            # Specify the path to your GeoPackage file
            # Read the GeoPackage file
            gdf_als = gpd.read_file(ALS_SITES)
            print('## start client')
            client = Client()
            print(f'## -- dask client opened at: {client.dashboard_link}')
            # case 2: given projt
            # case 1: all sites    
            for index, row in gdf_als.iterrows(): 
                    # Open a text file named "output.txt" in write mode
                    with open(LOG, 'w') as file:
                        # Write text to the file
                        file.write('# index: {}\n'.format(index))
                        file.write('# now processing als project: {}\n'.format(row['region'] + '_' + row['name']))
                    # if row['name'] not in ['csir_welverdient']:
                    #      continue
                    # if (index < 7): continue # start after csir_welverdient=6 # processing all sites
                
                    als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
                    is2_in_als = gdf_is2.loc[als_index] 
            
                    is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
            
                    print('# now processing als project: ', row['region'] + '_' + row['name'])
            
                    print("# is2 points in this als project: ", len(is2_in_als))
                    if (len(is2_in_als) <= 1): continue
                    print('# convert segments using EPSG of this project...', row['epsg'])
                    ##############################################################
                    # let us assume each project has only one epsg. 
                    for is2_index, is2_row in is2_in_als.iterrows():   
                            lat_c = is2_row['land_segments/latitude'] 
                            lon_c = is2_row['land_segments/longitude']                        
                            # Create a transformer object
                            transformer = Transformer.from_crs(4326, row['epsg'], always_xy = True) # projected coordinates. 
                            
                            # Now you can use this transformer to transform coordinates
                            # For example:
                            #lat, lon = -23.7129681741277, 30.93626135307411
                            e, n = transformer.transform(lon_c, lat_c) # alwasy x, y
                            #e, n , zone, letter = utm.from_latlon(lat_c, lon_c)
                            is2_in_als.loc[is2_index, 'e'] = e # add to df
                            is2_in_als.loc[is2_index, 'n'] = n # add to df
                            #is2_in_als.loc[is2_index, 'zone'] = zone
                    
                    geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
                    is2_in_als_projected = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry)
                    is2_in_als_projected_add_ori = add_orientation(is2_in_als_projected)
                    is2_in_als_projected_add_ori.to_parquet(TMP_FILE)
                    ########################################################### 
                    data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground'
                    # convert all laz file to las files
                    files_path = data_path + '/*.laz' 
                    laz_files = glob.glob(files_path)
                    print('# number of laz files: ', len(laz_files))
                    cmds = [sim_is2_laz(laz_f) for laz_f in laz_files]  ## if update merge to new shots 
                    _ = dask.persist(*cmds)
                    progress(_)
                    del _
                    print('') 
                    print('# als project:', row['region'] + '_' + row['name'], ' is done!')    
            client.close()
