# read is2_calval
import geopandas as gpd 
import numpy as np
import os
import glob
import time
import subprocess
import multiprocessing
from tqdm import tqdm
# read H5 file 
import h5py
########
import utm
import pandas as pd
import math
import matplotlib.pyplot as plt
from scipy.stats import poisson
from shapely.geometry import Point
# variables 
LAS_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las'
RES_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result'
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse' # from Amy
# CPU
N = 40

##########################################################################################################
# 
print('# read is2 in cal/val sites ...')
gdf_is2 = gpd.read_parquet('../result/is2_20m_calval_09252023.parquet') # 18550387 IS2 20 m segment points
# Specify the path to your GeoPackage file
als_sites = "../data/sites_20221006.gpkg"
# Read the GeoPackage file
gdf_als = gpd.read_file(als_sites)

# return all is2 points in laz boundary 
def sim_is2_laz(is2_in_als_utm , laz_path):
#laz_path = '/gpfs/data1/vclgp/data/gedi/imported/usa/usda_or/LAZ_ground/or_10_101.laz'
                    als_name = laz_path.split('/')[-3]
                    region = laz_path.split('/')[-4]
                    bounds_file = '/gpfs/data1/vclgp/data/gedi/imported/lists/ground_bounds/boundGround.' + als_name+ '.txt'
                    df = pd.read_csv(bounds_file, header = None, sep = " ")
                    #print('# las file : \n', laz_path)
                    df.columns = ['file', 'xmin', 'ymin', 'zmin', 'xmax', 'ymax', 'zmax']
                    basename = os.path.basename(laz_path)
                    d_file = df[df['file'].str.contains(basename[:-4])]
                    #print( '# laz file bounds: ',d_file.iloc[:, 1:6])
                    xmin = d_file['xmin'] 
                    xmax = d_file['xmax'] 
                    ymin = d_file['ymin'] 
                    ymax = d_file['ymax'] 
                    # get laz and nearby laz 
                    from shapely.geometry import Polygon
                    # Create a Polygon geometry from the coordinates
                    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
                    # return is2_in_als -----
                    is2_laz = is2_in_als_utm.clip(polygon)
                    #print('# is2_laz: \n', is2_laz)
                    if (len(is2_laz) == 0):
                        print('# no is2 20m segment is in this las file!')
                        return None
                    # get footprints 
                    ###########################################################
                    # should i get 1 las file  ---> 1 wave.h5?
                    # start from one segment -- one row 
                    
                    
                    segment_footprints_utm = get_footprint(is2_laz) # get all 
                        # create result folder /region/name/
                    res_out = RES_PATH + '/' + region + '/' + als_name 
                    #
                    ####

                    os.makedirs( res_out , exist_ok = True)



                    # if (len(is2_laz) > 300):
                    #     print('# number of is2 20m segments in las file: ', len(is2_laz))
                    #     is2_laz.to_csv(res_out+'/'+ basename[:-4] + '.csv' ) 
                    #     print('# las file: ', laz_path)
                    out_coor = res_out+ '/coordinates_'+ basename[:-4] + '.txt'####
                    #out_las_txt = res_out+ '/lasfiles_'+ basename[:-4] + '.txt'
                    out_wave = res_out+ '/wave_'+ basename[:-4] + '.h5'

                    ### if wave file exist, 
                    if os.path.isfile(out_coor):
                            print(f"The file {out_coor} exists.") 
                            return None
                    
                    segment_footprints_utm[['e', 'n']].to_csv(out_coor, sep=' ', header = False,  index = False)
                    
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
                    os.system(f'gediRat -fSigma 2.75 -readPulse {Pulse_PATH} -input {bs_las_path} -listCoord {out_coor}  -output {out_wave} -hdf  -ground > running.log')

                    # if out_wave is done. 
                    # remove this las file 
                    os.remove(bs_las_path)

                    if os.path.isfile(out_wave):
                        #print("# get rh from waveform ...")
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
                        for index_20m, row_20m in is2_laz.iterrows():
                                    
                                    lat_c = row_20m['land_segments/latitude_20m'] 
                                    lon_c = row_20m['land_segments/longitude_20m'] 
                                    slope = row_20m['slope']
                                    #print(out_wave, lat_c, lon_c, slope, '\n')
                                    e1, n1 , zone1, letter1 = utm.from_latlon(lat_c, lon_c)
                                    theta = math.atan(slope)
                                    data = np.arange(-14, 15) # -14 --14
                                    e_array = e1 + data * 0.7*math.cos(theta)
                                    n_array = n1 + data * 0.7*math.sin(theta)
                                    #print('# e_array:', e_array.dtype)
                                    e_array = [f'{round(e, 6):.6f}' for e in e_array]
                                    n_array = [f'{round(e, 6):.6f}' for e in n_array]  
                                    # get unique footprint ID like string. 
                                    # Concatenate elements with '.' separator
                                    is2_footprintID = [str(e) + '.' + str(n) for e, n in zip(e_array, n_array)]
                                    #print('# is2_footprintID', is2_footprintID)
                                    ##### I have utm now, return waveid  rowindex start and end. 
                                    # get wave id start index
                                    df2 = pd.DataFrame(is2_footprintID)
                                    df2.columns = ['id']
                                    res = pd.merge(df1, df2, on='id', how='inner')
                                    if (len(res) == 0): continue   ##### why is there empty waveform for this is2 20m segment?
                                    # las files boundary is from [min, max]
                                    #print('res:' , res)
                                    start = res['wave_index'].iloc[0]
                                    end = res['wave_index'].iloc[-1]
                                    #print('start, end: ', start, end)
                                    rh = get_sim_rh(out_wave, start, end)
                                    rh['fid'] = index_20m
                                    #print('# rh: ', rh)
                                    res_las.append(rh)
                        #print('rh list in each las file: ',res_las)          
                        res_las = pd.concat(res_las, ignore_index=True)
                        out_rh = res_out+ '/rh_'+ basename[:-4] + '.parquet' # each las file, give me rh.
                        #print('# write to parquet file ...')
                        res_las.to_parquet(out_rh)

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
                        pho_no_ground = pd.DataFrame()
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
                                    if (len(pho_w_ground) == 0):
                                        pho_w_ground = df1
                                    else:
                                        #print(df1)
                                        pho_w_ground = pd.concat([pho_w_ground, df1], ignore_index=True)
                                    # # canopy photons only
                                    canopy_wave = RXWAVECOUNT - GRWAVECOUNT
                                    
                                        
                                    total = sum(canopy_wave)
                                    if (total == 0 ): continue
                                    # fill NAs.
                                    #canopy_wave = np.nan_to_num(canopy_wave, nan=0.0)
                                    # Normalize the data by dividing each value by the total
                                    p_data = [value / total for value in canopy_wave]
                                    photon_rows = np.random.choice(rows, size=n, p=p_data)
                                    df1 = zStretch[photon_rows] - zG
                                    df1 = pd.DataFrame(df1)
                                    if (len(pho_no_ground) == 0):
                                        pho_no_ground = df1
                                    else:
                                        pho_no_ground = pd.concat([pho_no_ground, df1], ignore_index=True)
                    f.close()
                    percentiles = np.arange(1, 101, 1)
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


def get_footprint(is2_laz):
    res_footprints = []
    for is2_index, is2_row in is2_laz.iterrows():
                    lat_c = is2_row['land_segments/latitude_20m'] 
                    lon_c = is2_row['land_segments/longitude_20m'] 
                    slope = is2_row['slope']
                    e1, n1 , zone1, letter1 = utm.from_latlon(lat_c, lon_c)
                    theta = math.atan(slope)
                    data = np.arange(-14, 15) # -14 --14
                    e_array = e1 + data * 0.7*math.cos(theta)
                    n_array = n1 + data * 0.7*math.sin(theta)
                    e_array = [f'{round(e, 6):.6f}' for e in e_array]
                    n_array = [f'{round(e, 6):.6f}' for e in n_array] 
                    coordinates_utm = pd.DataFrame({'e': e_array, 'n': n_array})# .round(2).applymap(lambda x: f'{x:.2f}')  # keep 2 decimals.       
                    res_footprints.append(coordinates_utm)
    res_footprints = pd.concat(res_footprints, ignore_index=True)
    return res_footprints
     #coordinates_utm_sites_df #.to_csv('../wave/coordinates_utm_sites.txt', sep=' ', header = False,  index = False) # coordinates should be differnt. 

flag = 0

for index, row in reversed(list(gdf_als.iterrows())): 
        # Open a text file named "output.txt" in write mode
        with open('processing_reverse.txt', 'w') as file:
            # Write text to the file
            file.write('# index: {}\n'.format(index))
            file.write('# now processing als project: {}\n'.format(row['region'] + '_' + row['name']))

        if (row['region'] == 'centralamerica' and row['name'] == 'gliht_mexico_nfi_32616'):
            flag = index
        if (index > flag): continue # start only from this project.


        # The file will be automatically closed when you exit the 'with' block
        # the bounding box of each input geometry intersects the bounding box
        als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
        is2_in_als = gdf_is2.loc[als_index] 
        #print(len(is2_in_als))
        is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
        #out_name = '../result/is2_calval_region/is2_' + row['region'] + '_' + row['name'] + '.parquet'
        print('# now processing als project: ', row['region'] + '_' + row['name'])
        #is2_in_als.to_parquet(out_name)   # save is2 footprints in this als site
        print("# is2 points in this als project: ", len(is2_in_als))
        if (len(is2_in_als) <= 1): continue
        print('# convert segments in utm...')
        ##############################################################
        for is2_index, is2_row in is2_in_als.iterrows():   
                lat_c = is2_row['land_segments/latitude_20m'] 
                lon_c = is2_row['land_segments/longitude_20m']
                e, n , zone, letter = utm.from_latlon(lat_c, lon_c)
                is2_in_als.loc[is2_index, 'e'] = e
                is2_in_als.loc[is2_index, 'n'] = n
                is2_in_als.loc[is2_index, 'zone'] = zone
        # Create a GeoDataFrame with a Point geometry
        geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
        is2_in_als_utm = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry)
        ########################################################### 
        data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground'
        # convert all laz file to las files
        files_path = data_path + '/*.laz' 
        laz_files = glob.glob(files_path)
        print('# number of laz files: ', len(laz_files))
        nCPU = len(laz_files)
        if nCPU > 10 : 
           nCPU = N  # number of cores to use  
        print('# parallel processing...')
        pool = multiprocessing.Pool(nCPU) # Set up multi-processing
        progress_bar = tqdm(total=len(laz_files))
        def update_progress_bar(_):
              progress_bar.update()  
        for laz_f in laz_files:
            pool.apply_async(sim_is2_laz, (is2_in_als_utm, laz_f), callback=update_progress_bar)
        pool.close()
        pool.join()
        # Close the progress bar
        progress_bar.close()
        print('# als project:', row['region'] + '_' + row['name'], ' is done!')    
