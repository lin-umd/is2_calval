#!/usr/bin/env python
# coding: utf-8
'''
script to plot chm by ease 1km tiles in each site.
lin xiong
07/03/2024
TO do: fix edge effects.
'''
import os,glob
import geopandas
import sys
import numpy
from pyproj import Transformer
import argparse
from shapely.geometry import box
import pyarrow
import re
from dask.distributed import Client, progress
import subprocess


OUT='/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile'

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

def main():
    # get all sites
    gdf = geopandas.read_parquet('../data/all_sites_20240703.parquet')
    print(gdf)
    filtered_df = gdf[gdf['name'].isin(VALID_SITES)]
    print(filtered_df)
    
    client = Client(n_workers=30, threads_per_worker=1)
    print(client.dashboard_link)   
    #get_1kmtile_laz_list(grouped)
    
    if False:
        group_df_list = []
        for name in VALID_SITES:
            group_df_list.append(filtered_df[filtered_df['name'] == name]) 
        G = client.map(get_1kmtile_laz_list, group_df_list)
        progress(G)
        
    if False:
        print('-- get 1km tile')
        tiles = glob.glob(OUT+'/*/*/tile_gpkg/*.gpkg')
        print('-- number of 1km tiles: ', len(tiles))
        K = client.map(tile_1km, tiles)
        progress(K)
    
    if False:
        print('-- check 1km tiles')
        check_1km_laz()
    
    if False:
        print('-- tile data')
        files = glob.glob(OUT+'/*/*/laz_1km/*.laz')
        T = client.map(tile_500m, files)
        progress(T)
    
    if False:
        print('--ground classification')
        files = glob.glob(OUT+'/*/*/laz_500m/*.laz')
        G = client.map(ground, files)
        progress(G)
    if False:
        print('--normalization')
        files = glob.glob(OUT+'/*/*/classified/*.laz')
        N = client.map(get_norm, files)
        progress(N)
    if True:
        print('--thin laz generation')
        files = glob.glob(OUT+'/*/*/norm/*.laz')
        #print('africa jpl_rabi')
        #files = glob.glob(OUT+'/africa/jpl_rabi/norm/*.laz')
        I = client.map(thin, files)
        progress(I)
    if True:
        print('-- blast 1km chm generation')
        files = glob.glob(OUT+'/*/*/thin/*.laz')
        ids = get_easeids(files)
        B = client.map(chmby1km, ids)
        progress(B)
    client.shutdown()
    print('-- finish')
    
def get_tile_id(longitude, latitude, tilesize=72):
    ease2_origin = -17367530.445161499083042, 7314540.830638599582016
    ease2_nbins = int(34704 / tilesize), int(14616 / tilesize)
    ease2_binsize = 1000.895023349556141*tilesize, 1000.895023349562052*tilesize
    
    transformer = Transformer.from_crs('epsg:4326', 'epsg:6933', always_xy=True)
    x,y = transformer.transform(longitude, latitude)

    xidx = int( (x - ease2_origin[0]) / ease2_binsize[0]) + 1
    yidx = int( (ease2_origin[1] - y) / ease2_binsize[1]) + 1

    return xidx, yidx

def get_tile_bbox(x, y, tilesize=72, epsg=4326):
    ease2_origin = -17367530.445161499083042, 7314540.830638599582016
    ease2_nbins = int(34704 / tilesize), int(14616 / tilesize)
    ease2_binsize = 1000.895023349556141*tilesize, 1000.895023349562052*tilesize
    
    xmin = ease2_origin[0] + (x - 1) * ease2_binsize[0]
    xmax = xmin + ease2_binsize[0]
    ymax = ease2_origin[1] - (y - 1) * ease2_binsize[1]
    ymin = ymax - ease2_binsize[1]

    transformer = Transformer.from_crs('epsg:6933', f'epsg:{epsg:d}', always_xy=True)
    longitude,latitude = transformer.transform([xmin,xmax,xmax,xmin], [ymax,ymax,ymin,ymin])

    return longitude,latitude
    
def get_1kmtile_laz_list(group_df):
    region = group_df['region'].iloc[0]
    name = group_df['name'].iloc[0]
    out_dir = OUT + '/' + region + '/' + name
    os.makedirs(out_dir, exist_ok = True)
    print('-- make sub folders')
    # chm_1km  classified  laz_1km  laz_500m  norm  thin  tile_gpkg
    os.makedirs(out_dir + '/tile_gpkg', exist_ok = True)
    os.makedirs(out_dir + '/laz_1km', exist_ok = True)
    os.makedirs(out_dir + '/laz_500m', exist_ok = True)
    os.makedirs(out_dir + '/classified', exist_ok = True)
    os.makedirs(out_dir + '/norm', exist_ok = True)
    os.makedirs(out_dir + '/thin', exist_ok = True)
    os.makedirs(out_dir + '/chm_1km', exist_ok = True)
    minx, miny, maxx, maxy = group_df.total_bounds
    x_minidx, y_minidx = get_tile_id(minx, maxy, tilesize=1)
    x_maxidx, y_maxidx = get_tile_id(maxx, miny, tilesize=1)
    for x in range(x_minidx, x_maxidx+1):
        for y in range(y_minidx, y_maxidx+1):
            longitude,latitude = get_tile_bbox(x, y, tilesize=1, epsg=4326)
            # shapely.geometry.box(minx, miny, maxx, maxy, ccw=True)
            b = box(longitude[0], latitude[3], longitude[1], latitude[0])
            id  = group_df.sindex.query(b)
            gdf_laz = group_df.iloc[list(id)]
            if len(gdf_laz) >0:
                #print(gdf_laz)
                # 34,704	14,616
                file = out_dir + '/tile_gpkg/X' + str(x).zfill(5) + 'Y' + str(y).zfill(5) + '.gpkg'
                #gdf_laz.to_parquet(file)
                gdf_laz.to_file(file, driver='GPKG')  
    print()  # Add a blank line for readability

def tile_1km(t, overwrite=False):
    tinfo = geopandas.read_file(t)
    #print(tinfo)
    # get laz list 
    laz_list = []
    for index, row in tinfo.iterrows():
        laz_path = '/gpfs/data1/vclgp/data/gedi/imported/' + row['region'] + '/' + row['name'] + '/LAZ_ground/'
        laz_name = row['file'].replace(row['region'] + '_' + row['name'] + '_', '').replace('.parquet', '.laz')
        laz_list.append(laz_path + laz_name)
    #print(laz_list)
    # clip laz point cloud 
    lazs = ' '.join(laz_list)
    out_dir = os.path.split(os.path.dirname(t))[0] +  '/laz_1km'
    #out_dir = os.path.dirname(t).replace('/tile_gpkg', '/laz_1km')
    out_laz = out_dir+ '/' + os.path.basename(t)[:-5] + '.laz'
    if not os.path.exists(out_laz) or overwrite:
        match = re.search(r'X(\d+)Y(\d+)', t)
        x = match.group(1)
        y = match.group(2)
        longitude,latitude = get_tile_bbox(int(x), int(y), tilesize=1, epsg=int(tinfo.iloc[0]['epsg']))
        cmd=f'wine $LASTOOLS/las2las.exe -i {lazs} -o {out_laz} -merged -inside {longitude[0]} {latitude[3]} {longitude[1]} {latitude[0]}'
        #print(cmd)
        os.system(cmd)
        # index
        cmd=f'wine $LASTOOLS/lasindex.exe -i {out_laz} -append'
        os.system(cmd)

def check_1km_laz():
    files = glob.glob(OUT+ '/*/*/laz_1km/*.laz')
    n=0
    for laz in files:
        try:
            #cmd = f"wine $LASTOOLS/lasinfo.exe {laz}"
            #os.system(cmd)
            command = ['wine', '$LASTOOLS/lasinfo.exe', laz]  # Example command (listing files in the current directory with details)
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            #command = ['wine', '$LASTOOLS/lasindex.exe', laz, '-append']
            #result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except:
            n=n+1
            print('--error reading: ', laz)
    print('number of error files:', n)

def tile_500m(out_laz,  overwrite=False):
    out_dir = os.path.split(os.path.dirname(out_laz))[0]
    out_laz_tile =  out_dir  + '/laz_500m/' + os.path.basename(out_laz)[:-4] + 'tile.laz'
    tiles500m = glob.glob(out_laz_tile[:4] + '*.laz')
    if len(tiles500m) == 0 or overwrite:
        print('tile laz into 500m')
        cmd = f"wine $LASTOOLS/lastile.exe -i {out_laz}  -o {out_laz_tile} -tile_size 500"
        os.system(cmd)

# ground classification
def ground(f, overwrite=False):
    out_dir = os.path.split(os.path.dirname(f))[0]
    out_class = out_dir + '/classified/' + os.path.basename(f)[:-4] + '_classified.laz'
    if not os.path.exists(out_class) or overwrite:
        cmd=f'wine $LASTOOLS/lasground_new.exe -i {f} -o {out_class}'
        print(cmd)
        os.system(cmd)

# normalized height
def get_norm(f, overwrite=False):
    out_dir = os.path.split(os.path.dirname(f))[0]
    out_norm = out_dir + '/norm/' + os.path.basename(f)[:-4] + '_norm.laz'
    if not os.path.exists(out_norm) or overwrite:
        cmd=f'wine $LASTOOLS/lasheight.exe -i {f} -drop_above 150 -replace_z -o {out_norm} '
        print(cmd)
        os.system(cmd)

def thin(f, overwrite=True):
    out_dir = os.path.split(os.path.dirname(f))[0]
    thin_laz = out_dir + '/thin/' + os.path.basename(f)[:-4] + '_thin.laz'
    if not os.path.exists(thin_laz) or overwrite:
        # f: X21081Y07961tile_1129500_9430000_classified_norm.laz
        minx = os.path.basename(f).split('_')[1]
        miny =  os.path.basename(f).split('_')[2] 
        # -ll {minx} {miny} -ncols 20 -nrows 20
        cmd = f'wine $LASTOOLS/lasthin.exe -i {f} -step 1 -highest -subcircle 0.2 -o {thin_laz} ' # -remain_buffered
        print(cmd)
        os.system(cmd)


# get canopy height model
# not used
def chm(f, overwrite=False):
    out_tif = os.path.split(os.path.dirname(f))[0] + '/chm/' + os.path.basename(f)[:-4] + '.tif'
    thin_laz = os.path.split(os.path.dirname(f))[0] + '/thin/' + os.path.basename(f)[:-4] + '_thin.laz'
    if not os.path.exists(out_tif) or overwrite:
        # f: X21081Y07961tile_1129500_9430000_classified_norm.laz
        minx = os.path.basename(f).split('_')[1]
        miny =  os.path.basename(f).split('_')[2] 
        # -ll {minx} {miny} -ncols 20 -nrows 20
        cmd = f'wine $LASTOOLS/lasthin.exe -i {f} -step 25 -highest -subcircle 5 -o {thin_laz} ' # -remain_buffered
        os.system(cmd)
        cmd = f'wine $LASTOOLS/las2dem.exe -i {thin_laz} -step 25 -o {out_tif}'
        #cmd  = f'wine $LASTOOLS/lasgrid.exe -i {f} -highest -step 25  -o {out_tif}' # 500mby500m
        #cmd  = f'wine $LASTOOLS/blast2dem.exe -i {f} -first_only -step 25  -o {out_tif}' # 500mby500m
        print(cmd)
        os.system(cmd)

def get_easeids(files):
    
    ids = []
    for file in files:
        bs = os.path.basename(file) # X21080Y07964
        id = os.path.dirname(file) + '/'+bs[:12]
        if id not in ids:
            ids.append(id)
    return ids # 

def chmby1km(easeidx, overwrite=True):
    files = glob.glob(easeidx+'*.laz')
    out_tif = easeidx.replace('/thin/', '/chm_1km/') + '.tif'
    if not os.path.exists(out_tif) or overwrite:
        fs = ' '.join(files)
        cmd  = f'wine $LASTOOLS/blast2dem.exe -i {fs} -first_only -step 25 -merged  -o {out_tif}' # 
        print(cmd)
        os.system(cmd)    

if __name__ == "__main__":
    main()