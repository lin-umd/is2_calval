import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import glob
import rasterio
import matplotlib.pyplot as plt
import subprocess
import multiprocessing 
from tqdm import tqdm
import sys
import argparse
# use
# python 8getALSchm_1km.py --arg1 plumasnf_20180707


def chm_grid_1km(row, index):
        reg = row['region']
        name =  row['name']
        epsg = row['epsg']
        chms_path = '/gpfs/data1/vclgp/data/gedi/imported/' + reg + '/' + name + '/chm/*.tif'
        #chms_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result_chm/usa/plumasnf_20180707/*.tif'
        chms = glob.glob(chms_path)
        # we need to check the chm file, remove band tif files. 
        print(index, reg, name, len(chms))
        print(chms[0])
        tiff_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/chm/'+ name + '_tiff_list.txt'
        chm_1km = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/chm/' + name + '.tif'
        with open(tiff_file, 'w') as f:
            for file_name in chms:
                f.write(file_name + '\n')
        # we should ignore nodata values. -n -9999
        command = f"gdal_merge.py -n -9999 -ps 1000 1000  -o {chm_1km} --optfile {tiff_file} "
        subprocess.run(command, shell=True, capture_output=True, text=True)
        chm_1km_wgs84 = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/chm/' + name + '_wgs84.tif'
        command = f"gdalwarp  -overwrite -s_srs EPSG:{epsg}  -t_srs EPSG:4326 {chm_1km} {chm_1km_wgs84}"
        subprocess.run(command, shell=True, capture_output=True, text=True)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get 1km gridded chm for each als plot.')
    parser.add_argument('--arg1', type=str, help='ALS plot name')
    # Parse the arguments
    args = parser.parse_args()
    als_name = args.arg1
    als_plots = gpd.read_parquet('../data/all_sites_20231218.parquet')
    if als_name is not None:
        print('als name', als_name)
        als_plots = als_plots[als_plots['name'] == als_name]
    #als_plots = als_plots[100:103]
    nCPU = len(als_plots)
    if nCPU > 15 : 
       nCPU = 15  # number of cores to use  
    print('# parallel processing...')
    pool = multiprocessing.Pool(nCPU) # Set up multi-processing
    progress_bar = tqdm(total=len(als_plots))
    def update_progress_bar(_):
          progress_bar.update()  
    for index, row in als_plots.iterrows():
        pool.apply_async(chm_grid_1km, (row, index), callback=update_progress_bar)
    pool.close()
    pool.join()
    # Close the progress bar
    progress_bar.close()
    sys.exit("## -- DONE")     