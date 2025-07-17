#!/usr/bin/env python
# coding: utf-8
import utm
import subprocess
import glob
import re
import pandas as pd
import os
from shapely.geometry import Polygon
import geopandas as gpd

def get_project_bounds(project ): # = 'africa/csir_agincourt'
        print('## processing:', project)
        laz_file_folder ="/gpfs/data1/vclgp/data/gedi/imported/" + project  + "/LAZ_ground"
        region = project.split('/')[0]
        name  =  project.split('/')[1]
        file_paths = glob.glob(laz_file_folder + '/*.laz')    
        # get laz boundary
        res = []
        for f in file_paths:
                #print('## processing: ', f)
                out_txt = f'../data/bounds/{region}_{name}_{os.path.basename(f)[:-4]}.txt'  # Replace with the path to your text file
                #print(out_txt)
                command = f"lasinfo -i {f}  -o  {out_txt}"  # Replace this with your Bash command
                # Run the command in the terminal
                result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                # extract bound, utm zone informatin 
                # Open the file in read mode
                flag = True
                letter = 'N'
                xmin, ymin, xmax, ymax, zone_number = 0,0,0,0,0
                with open(out_txt, 'r') as file:
                    # Read and print each line
                    for line in file:
                        if 'min x y z: ' in line:
                                #print(line.strip())  # The 'strip' method removes leading and trailing whitespaces, including the newline character
                                words = line.split()
                                xmin = words[4]
                                ymin = words[5]
                        if 'max x y z: ' in line:
                                #print(line.strip())
                                words = line.split()
                                xmax = words[4]
                                ymax = words[5]

                        if 'UTM zone ' in line: # 
                                #print(line.strip())
                                words = line.split('UTM zone ')
                                #print(words[1][:3])
                                zone_number = words[1][:2] # 18N what if 8N???? 1 -60
                            #   [0,2)
                                letter = words[1][2:3]   
                        elif 'UTM ' in line: # if UTM zone exist, no need this step.
                                words = line.split('UTM ')
                                #print(words)
                                zone_number = words[1][:2]
                                letter = words[1][2:3]
                file.close()
            
                if letter == 'S':
                                flag = False
                if (zone_number == 0):
                                print('this file can not find zone number!!!', out_txt)
                                return None
                # UTM eastings range from 167,000 meters to 833,000 meters at the equator. These ranges narrow toward the poles.
                # but points can be across the boundary. 
                if (float(xmin) < 167000 or float(xmin)  > 833000):
                    print('this file has out range of Easting!!!', out_txt)
                    return None
                #print('## processing:  ' , f)
                # The syntax is utm.to_latlon(EASTING, NORTHING, ZONE_NUMBER, ZONE_LETTER).
                #The return has the form (LATITUDE, LONGITUDE).
                lat_min, lon_min = utm.to_latlon(float(xmin),float( ymin ), int(zone_number), northern = flag) # north or south 
                lat_max, lon_max = utm.to_latlon(float(xmax),float( ymax ), int(zone_number), northern = flag) # north or south 
                # geometry 
                # Create a Polygon geometry from the coordinates
                geometry = Polygon([(lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max), (lon_min, lat_max)])
                # Create a DataFrame
                data = pd.DataFrame({'region': [region],
                        'name': [name],
                        'f': [f],
                        'xmin': [xmin],
                        'ymin': [ymin],
                        'xmax': [xmax],
                        'ymax': [ymax],
                        'zone_Number': [zone_number],
                        'letter': [letter],
                        'lon_min': [lon_min],
                        'lat_min': [lat_min],
                        'lon_max': [lon_max],
                        'lat_max': [lat_max],
                        'geometry': [geometry]})
                gdf = gpd.GeoDataFrame(data, geometry = 'geometry', crs = "EPSG:4326" )
                res.append(gdf)
        if len(res) == 0: return None
        gdf_combined = gpd.GeoDataFrame( pd.concat(res, ignore_index=True) )
        #gdf_combined.plot()
        # Merge all polygons into a single polygon
        merged_geometry = gdf_combined['geometry'].unary_union
        # Create a DataFrame
        data = pd.DataFrame({'region': [region], 'name': [name]})
        result_gdf = gpd.GeoDataFrame(data, geometry=[merged_geometry], crs=gdf_combined.crs)
        result_gdf['area_ha'] = result_gdf.to_crs("EPSG:3395").area/ 10000 
        # ['region', 'name', 'area_ha', 'geometry']
        result_gdf = result_gdf[['region', 'name', 'area_ha', 'geometry']]
        #result_gdf.plot()
        return result_gdf

laz_file_folder ="/gpfs/data1/vclgp/data/gedi/imported/*/*/LAZ_ground"
folders = glob.glob(laz_file_folder)
# Define a regular expression pattern to extract the desired string
regex_pattern = r'/imported/(.*?)/LAZ_ground'
# Extract the string from each folder path
matches = [re.search(regex_pattern, folder).group(1) for folder in folders]
print('# how many als sites ? ', len(matches))

all_sites = []
# current processing: europe/froscham 
flag = 1
for als in matches:
        if als == 'europe/froscham':
            print('Start from previous site: ', als)
            flag = 1
        if flag == 0: continue
        project_res = get_project_bounds(als)
        all_sites.append(project_res)
all_sites_gdf = gpd.GeoDataFrame(pd.concat(all_sites, ignore_index=True), geometry='geometry', crs="EPSG:4326")
all_sites_gdf.to_file('../data/als_bounds_all_updates_20231123.gpkg', driver='GPKG')