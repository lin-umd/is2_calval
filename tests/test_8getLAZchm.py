#!/usr/bin/env python
# coding: utf-8
# library
import glob
import os
from dask.distributed import Client, progress
import dask
import sys
@dask.delayed
def get_chm(laz_path, norm_out, chm_path):
    norm_laz = norm_out + '/' + os.path.basename(laz_path)
    #if os.path.exists(norm_laz): return 0
    os.system(f'wine $LASTOOLS/lasheight.exe -drop_above 150 -drop_below -3  -replace_z -i {laz_path} -odir {norm_out} -olaz ') # -cores 4
    os.system(f'wine $LASTOOLS/blast2dem.exe  -i {norm_laz} -odir {chm_path} -otif ') # -cores 4
    return 1
if __name__ == '__main__':
    file_path = "../data/als_sites_folder.txt"
    my_list = []
    with open(file_path, "r") as file:
        my_list = file.readlines()
    folder_sites = [item.strip() for item in my_list]
    print("Example folder name:", folder_sites[1])
    for folder in folder_sites:
            if folder == '/gpfs/data1/vclgp/data/gedi/imported/usa/plumasnf_20180707/':
                print('## preparing folder')
                region = folder.split('/')[7]
                name = folder.split('/')[8]
                norm_out = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las_norm/' + region + '/' + name
                os.makedirs(norm_out, exist_ok=True)   
                chm_path = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result_chm/' + region + '/' + name
                os.makedirs(chm_path, exist_ok=True)
                all_laz = glob.glob(folder + '/LAZ_ground/*.laz')
                all_laz=all_laz[:2]
                print('## processing ', name, ', files: ', len(all_laz))
                with Client(n_workers=2, threads_per_worker=1) as client:
                        print(f'## -- dask client opened at: {client.dashboard_link}')
                        cmds = [get_chm(laz_path, norm_out, chm_path) for laz_path in all_laz]  
                        _ = dask.persist(*cmds)
                        progress(_)
                        #client.close()
                sys.exit("## -- DONE")