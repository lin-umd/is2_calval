#!/usr/bin/env python
# coding: utf-8

# In[7]:
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


# In[23]:




# In

gdf_is2 = gpd.read_parquet('../result/is2_20m_calval_09252023.parquet') # 18550387 IS2 20 m segment points
# Specify the path to your GeoPackage file
als_sites = "../data/atl08_v6_icepyx_welverdient_20m_filter_wgs84.gpkg"
# Read the GeoPackage file
gdf_als = gpd.read_file(als_sites)
is2_in_als = gdf_is2.clip(gdf_als['geometry'])  # get points inside polygon.
# Now, gdf is a GeoDataFrame containing the data from the GeoPackage file
# In[9]:


#clipped_gdf # 480844 rows × 13 columns by clip
# tmp = gdf_is2[:10]
# # create a unique ID
# tmp ['root_file'].apply(lambda x: x[6:-9])+ tmp ['root_beam'] + '_' + tmp ['land_segments/segment_id_beg'].astype(str)
# gdf_is2['fid'] = gdf_is2 ['root_file'].apply(lambda x: x[6:-9])+ gdf_is2 ['root_beam'] + '_' + gdf_is2 ['land_segments/segment_id_beg'].astype(str)
# Copy the row index to a new column
gdf_is2['fid'] = gdf_is2.index
# In[10]:


# is2_in_als # rgt_id_beg --> only locate 100 m segments. # in the future merge 20 m, change segment_id. 
# for is2_index, is2_row in is2_in_als[10:20].iterrows(): 
#     print(is2_index)
#len(is2_in_als)
#is2_in_als.plot()
#is2_in_als.total_bounds


# In[11]:



def get_sim_rh(filename = "wave.h5"):
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
                        for i in range(0, N_foorprints):
                                    
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


# In[12]:


# lat lon to UTM 
# e1, n1 , zone1, letter1 = utm.from_latlon(lat_c, lon_c)


# In[ ]:





# In[33]:


# give a laz, create a 20m buffer zone, return all laz in the zone. 
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
                        #print('# no is2 20m segment is in this las file!')
                        return None
                    # get footprints 
                    ###########################################################
    ###########################################################################
                    # should i get 1 las file  ---> 1 wave.h5?
                    # start from one segment -- one row 
                    
                    print('# number of is2 20m segments in las file: ', len(is2_laz))
                    segment_footprints_utm = get_footprint(is2_laz) # get all 
                    out_coor = '../wave4test/coordinates_'+ basename[:-4] + '.txt'####
                    out_las = '../wave4test/lasfiles_'+ basename[:-4] + '.txt'
                    out_wave = '../wave4test/wave_'+ basename[:-4] + '.h5'
                    segment_footprints_utm[['e', 'n']].to_csv(out_coor, sep=' ', header = False,  index = False)
                    ##### laz boundary problem is not solved ....
                    ##### #######################################################################################
                    # geometries = [Polygon([(row['xmin'], row['ymin']), (row['xmin'], row['ymax']), 
                    #                                    (row['xmax'], row['ymax']), (row['xmax'], row['ymin'])])
                    #                           for _, row in df.iterrows()]
                    # # Create a GeoDataFrame with the rectangles
                    # gdf = gpd.GeoDataFrame(df, geometry=geometries)
                    # laz_want = gdf.sindex.query(polygon)
                    # #gdf.loc[laz_want].plot(facecolor='none', edgecolor='red')
                    # laz_nearby = gdf.loc[laz_want]
                    # #print( '# convert laz to las files if not exist...') 
                    # for laz_index, laz_row in laz_nearby.iterrows(): 
                    #      laz_path = laz_row['file']
                    #      # /gpfs/data1/vclgp/data/gedi/imported/usa/usda_or/ALS_ground/or_10_101.las
                    #      laz_path = laz_path.replace('ALS_ground', 'LAZ_ground').replace('.las', '.laz')
                    #      las_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las/' + region + '/' + als_name
                    #      os.makedirs( las_out , exist_ok = True)
                    #      bs = os.path.basename(laz_path)
                    #      bs_las = bs[:-1] + 's'
                    #      # check if las file exist:
                    #      bs_las_path = las_out+'/'+bs_las
                    #      if not os.path.isfile(bs_las_path):
                    #                 #print('not exist')
                    #                 print('# converting laz file: ', laz_path )
                    #                 os.system(f'las2las -i {laz_path} -odir {las_out} -olas')
                    # split_columns = laz_nearby['file'].str.split('/')
                    
                    # new_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las/' + split_columns.str[-4] + '/' + split_columns.str[-3] + '/' + split_columns.str[-1]
                    # new_path.to_csv(out_las, sep=' ', header = False,  index = False) 
                    #####################################################################################
                    # directly run 
                    # #######################################
                    #####
                    
                    # /gpfs/data1/vclgp/data/gedi/imported/usa/usda_or/ALS_ground/or_10_101.las
                    laz_path = laz_path.replace('ALS_ground', 'LAZ_ground').replace('.las', '.laz')
                    las_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las/' + region + '/' + als_name
                    os.makedirs( las_out , exist_ok = True)
                    bs = os.path.basename(laz_path)
                    bs_las = bs[:-1] + 's'
                    # check if las file exist:
                    bs_las_path = las_out+'/'+bs_las
                    #print('# bs_las_path: ', bs_las_path)
                    if not os.path.isfile(bs_las_path):
                            #print('not exist')
                            print('# converting laz file: ', laz_path )
                            os.system(f'las2las -i {laz_path} -odir {las_out} -olas')
                    
                    #new_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las/' + split_columns.str[-4] + '/' + split_columns.str[-3] + '/' + split_columns.str[-1]
                    #print('# bs_las_path : ', bs_las_path)
                    with open(out_las, 'w') as file:
                            file.write(bs_las_path)
                    #pd.DataFrame(bs_las_path).to_csv(out_las, sep=' ', header = False,  index = False) 
                    #######################################################################################
                    os.system(f'gediRat -fSigma 2.75 -readPulse 20190821.gt1l.pulse -inList {out_las} -listCoord {out_coor}  -output {out_wave} -hdf  -ground > running.log')
                    #print('# get ch98 , rh and fid ???')
                    # df_tmp = pd.DataFrame()
                    # try:
                    #      df_tmp = get_sim_rh(out_wave)
                    #      df_tmp['fid'] = fid
                    # #      print('# sim result length: ', len(df_tmp))
                    #      results.append(df_tmp)
                    # except:
                    #      pass
                    # if (len(results) > 0):
                    #             # write to parquet 
                    #             las_sim_result = pd.concat(results, ignore_index=True)
                    #             print('# write simulation result for this las file...')
                    #             las_sim_result.to_parquet('../wave/sim_las_'+ basename[:-3] + 'parquet')
                    #             #return pd.concat(results, ignore_index=True)
#laz_path = '/gpfs/data1/vclgp/data/gedi/imported/usa/neon_sawb/LAZ_ground/L26_2014_BURL_1_v01_2014053013_P01_r.laz'                        
#sim_is2_laz(is2_in_als_utm , laz_path)


# In[34]:



#segment2footprints(is2_in_als) # 480,744
#out_coor = '../wave/coordinates_utm_sites.txt'
#data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground'
#data_path
# /gpfs/data1/vclgp/data/gedi/imported/lists/ground_bounds
# gediRat -fSigma 2.75 -readPulse 20190821.gt1l.pulse -inList alslist.txt -listCoord ../wave/coordinates_utm_sites.txt  -output wave.h5 -hdf  -ground
#os.system(f'gediRat -fSigma 2.75 -readPulse 20190821.gt1l.pulse -inList alslist.txt -listCoord {out_coor}  -output wave.h5 -hdf  -ground')


# In[35]:



def las_list(data_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las'):
#data_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las'
                files_las = [os.path.join(data_path, file)
                         for file in os.listdir(data_path) 
                         if os.path.isfile(os.path.join(data_path, file))]
                # Open the output text file in write mode
                with open("../wave/alslist.txt", "w") as file: # als list should be different
                    # Write each file name to a new line in the text file
                    for file_name in files_las:
                        file.write(file_name + "\n")
                # Close the file
                file.close()
def is2_geo_utm(is2_in_als):
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
        return is2_in_als_utm

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
                    coordinates_utm = pd.DataFrame({'e': e_array, 'n': n_array})# .round(2).applymap(lambda x: f'{x:.2f}')  # keep 2 decimals.       
                    res_footprints.append(coordinates_utm)
    res_footprints = pd.concat(res_footprints, ignore_index=True)
    return res_footprints
     #coordinates_utm_sites_df #.to_csv('../wave/coordinates_utm_sites.txt', sep=' ', header = False,  index = False) # coordinates should be differnt. 



# Loop through every row in the GeoDataFrame


# In[36]:



def rm_files(data_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las'):
                    # List all files in the folder
                    file_list = os.listdir(data_path)
                    # keep large files to save time for next time
                    if (len(file_list) > 1000): 
                        return None
                    # Loop through the files and delete them
                    for filename in file_list:
                        file_path = os.path.join(data_path, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            #print(f"Deleted: {file_path}")
#rm_files()


# In[37]:


# myresult = pd.concat([result.get() for result in results], ignore_index=True)
# #myresult = [x for x in myresult if x is not None]
# #pd.concat(myresult, ignore_index=True)
# myresult
# for is2_index, is2_row in is2_in_als.iterrows():   
#         lat_c = is2_row['land_segments/latitude_20m'] 
#         lon_c = is2_row['land_segments/longitude_20m']
#         e, n , zone, letter = utm.from_latlon(lat_c, lon_c)
#         is2_in_als.loc[is2_index, 'e'] = e
#         is2_in_als.loc[is2_index, 'n'] = n
#         is2_in_als.loc[is2_index, 'zone'] = zone
# # Create a GeoDataFrame with a Point geometry
# geometry = [Point(x, y) for x, y in zip(is2_in_als['e'], is2_in_als['n'])]
# is2_in_als_utm = gpd.GeoDataFrame(data = is2_in_als, geometry=geometry)


# In[38]:


###### loop through every project  ---- return a is2_cal_va
# get possible las files for each segment. 
# function convert laz to las .... 
# Loop through every row in the GeoDataFrame
# 400 sites, parellel??? no need, may need!!! 
# Open a text file named "output.txt" in write mode
# /gpfs/data1/vclgp/data/gedi/imported/africa/csir_welverdient/LAZ_ground
region = 'africa'
name = 'csir_welverdient'


with open('processing_4mikhail.txt', 'w') as file:
    # Write text to the file
    file.write('# now processing als project: {}\n'.format(region + '_' + name))

# The file will be automatically closed when you exit the 'with' block
# the bounding box of each input geometry intersects the bounding box
# als_index = gdf_is2.sindex.query(row['geometry']) # super fast!!!!! # but only boundary box. 
# is2_in_als = gdf_is2.loc[als_index] 
# #print(len(is2_in_als))
# is2_in_als = is2_in_als.clip(row['geometry'])  # get points inside polygon.
#out_name = '../result/is2_calval_region/is2_' + row['region'] + '_' + row['name'] + '.parquet'
print('# now processing als project: ', region + '_' + name)
#is2_in_als.to_parquet(out_name)   # save is2 footprints in this als site
print("# is2 points in this als project: ", len(is2_in_als))
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
data_path = '/gpfs/data1/vclgp/data/gedi/imported/' + region + '/' + name + '/LAZ_ground'
# convert all laz file to las files
files_path = data_path + '/*.laz' 
laz_files = glob.glob(files_path)
print('# number of laz files: ', len(laz_files))
nCPU = len(laz_files)
if nCPU > 30 : 
    nCPU = 30  # number of cores to use  
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
# out las path 
# las_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/las/' + row['region'] + '/' + row['name']
# print('# convert laz files to las...')
# os.makedirs( las_out , exist_ok = True)
# for f_laz in laz_files: # parallel processing to convert files if over > 500.################################################################
#     bs = os.path.basename(f_laz)
#     bs_las = bs[:-1] + 's'
#     # check if las file exist:
#     bs_las_path = las_out+'/'+bs_las
#     if not os.path.isfile(bs_las_path):
#         #print('not exist')
#         os.system(f'las2las -i {f_laz} -odir {las_out} -olas') # convert data to las folder  # folder should be different when parallel. 
# # write to als list
# las_list(las_out)
# step: loop through every 20 m segment #  480844 points, parallel ???
# directly read ---las files and footprints. 
# out_coor = '../wave/coordinates_utm_sites.txt'
# als_list = '../wave/alslist.txt'
# out_wave = '../wave/wave_'+ row['region'] + '_' + row['name'] + '.h5'
# os.system(f'gediRat -fSigma 2.75 -readPulse 20190821.gt1l.pulse -inList {als_list} -listCoord {out_coor}  -output {out_wave} -hdf  -ground')
# # nCPU = len(is2_in_als)
# if nCPU > 20 : 
#    nCPU = 20  # number of cores to use  
# print('# parallel processing...')
# pool = multiprocessing.Pool(nCPU) # Set up multi-processing
# progress_bar = tqdm(total=len(is2_in_als))
# def update_progress_bar(_):
#       progress_bar.update()  
# results = []
# for is2_index, is2_row in is2_in_als.iterrows():   # no need for every footprint. 
#         #get_sim(is2_index,is2_row)
#         tmp = pool.apply_async(get_sim, (is2_index,is2_row), callback=update_progress_bar)
#         results.append(tmp)
# myresult = pd.concat([result.get() for result in results], ignore_index=True)
#                 #print(is2_row)
#                 # 29.053188, -110.62982
#                 # lat_c = is2_row['land_segments/latitude_20m'] 
#                 # lon_c = is2_row['land_segments/longitude_20m'] 
#                 # slope = is2_row['slope']
#                 # fid = is2_row['fid']
#                 # e1, n1 , zone1, letter1 = utm.from_latlon(lat_c, lon_c)
#                 # theta = math.atan(slope)
#                 # data = np.arange(-14, 15) # -14 --14
#                 # e_array = e1 + data * 0.7*math.cos(theta)
#                 # n_array = n1 + data * 0.7*math.sin(theta)
#                 # lat_, lon_ = utm.to_latlon(e_array, n_array, zone1, letter1)
#                 # coordinates = pd.DataFrame({'lat': lat_, 'lon': lon_})
#                 # coordinates_utm = pd.DataFrame({'e': e_array, 'n': n_array})
#                 # coordinates_utm.to_csv('coordinates.txt', sep=' ', header = False,  index = False) # coordinates should be differnt. 
#                 # # run simulator
#                 # os.system(f'gediRat -fSigma 2.75 -readPulse 20190821.gt1l.pulse -inList alslist.txt -listCoord coordinates.txt  -output wave.h5 -hdf  -ground > running.log')
#                 # # get ch_98 and rh_metrics
#                 # df_tmp = get_sim_rh()
#                 # df_tmp['fid'] = fid
#                 #df.append(df_tmp)
# # save the result 
# # Concatenate the DataFrames into a single DataFrame
# #combined_df = pd.concat(df, ignore_index=True)
# out_name = '../result/is2_calval_region/is2_sim_' + row['region'] + '_' + row['name'] + '.parquet'
# myresult.to_parquet(out_name)  
#rm_files(las_out) # remove las files when done
print('# als project:', region + '_' + name, ' is done!')
# gdf.to_parquet('data.parquet')  
#out_name = '../result/is2_calval_region/is2_' + row['region'] + '_' + row['name'] + '.parquet'
#is2_in_als.to_parquet(out_name)
