#!/usr/bin/env python
# coding: utf-8
'''
give a boundary file, return atl03 files list.
# example use
# python is2query.py --input_file ../data/all_sites_20231130.parquet --output_folder ../result/atl03_list/
'''

from dask.distributed import Client, progress
import dask.dataframe as dd
import dask
import sys
import os
import shutil
import geopandas as gpd
from shapely.geometry import Polygon
import icepyx as ipx
import argparse
import subprocess

def order_vertices_counter_clockwise(polygon):
    if not polygon.is_empty and polygon.exterior.is_ccw:
        return polygon
    else:
        return Polygon(polygon.exterior.coords[::-1])    

@dask.delayed
def is2_list(f_in_polygon, folder, index):# output list  # file_path = "output/output_" + str(index) + ".txt"
    # ROI 
    f_name = 'atl03_list_'+str(index)+'.txt'
    f_out = os.path.join(folder, f_name)
    short_name = 'ATL03'
    date_range = ['2018-01-01','2023-12-31']
    file_list = []
    # Access the geometry of the first polygon
    polygon_geometry = f_in_polygon
    polygon_geometry = order_vertices_counter_clockwise(polygon_geometry)
    lon_lat_pairs = list(polygon_geometry.exterior.coords)
    #lon1, lat1, lon2, lat2, lon3, lat3, and so on.
    #print(lon_lat_pairs )
    mylist=[]
    try:
        region_a = ipx.Query(short_name, lon_lat_pairs, date_range)
        region_a.avail_granules()
        mylist = region_a.granules.avail
        for item in mylist:
                file_list.append(item.get('links')[0].get('href'))
        with open(f_out, "w") as file:
            # Write each item from the list to a new line in the file
            for item in file_list:
                file.write(f"{item}\n")
    except Exception as e:
            # Handle other exceptions that might occur
            print(f"No files in this polygon {index}")

if __name__ == "__main__":
    parse = argparse.ArgumentParser(description="Query a list of atl03 data files")
    parse.add_argument("--input_file", help="Input file to use [geoparquet file]", required=True)
    parse.add_argument("--output_folder", help="Output folder to write", required=True)
    #parse.add_argument("--tile", help="Tile number [integer]", required = True)
    args = parse.parse_args()
    os.makedirs(args.output_folder, exist_ok=True)
    gdf = gpd.read_parquet(args.input_file) #
    gdf_1_polygon = gdf.explode(index_parts=False)
    gdf_1_polygon = gdf_1_polygon.reset_index(drop=True)
    #### start dask client 
    print('## start client')
    client = Client()
    print(f'## -- dask client opened at: {client.dashboard_link}')
    cmds = [is2_list(row['geometry'], args.output_folder, index) for index, row in gdf_1_polygon.iterrows()]
    _ = dask.persist(*cmds)
    progress(_)
    del _
    print('') 
    client.close()
    sys.exit("## -- DONE")
