sites_list_path = "/gpfs/data1/vclgp/xiongl/cluster/cluster07/valid_als_sites.txt"
import os
import pandas as pd
import glob
import sys
import argparse

parser = argparse.ArgumentParser(description='Check for missing wave or rh files.')

parser.add_argument('-t', '--file_type', type=str, choices=['wave', 'rh'], required=True,
                    help='Type of file to check for (wave or rh).')
args = parser.parse_args()



# function give a txt file, return number of lines 
def count_lines(file_path):
    with open(file_path, 'r') as file:
        return sum(1 for line in file if line.strip())



# read txt and convert to list    
with open(sites_list_path, 'r') as file:
    sites = [line.strip() for line in file if line.strip()]
print(sites)

# get the list of coordinates files 

ftype = args.file_type

for s in sites:
    files = glob.glob(f"/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3/*/{s}/coordinates*.txt")
    for f in files:
        if ftype == 'wave':
            target_f = f.replace('coordinates', 'wave').replace('.txt', '.h5')
        if ftype == 'rh':
            target_f = f.replace('coordinates', 'rh').replace('.txt', '.parquet')
        if not os.path.exists(target_f):
            
            N = count_lines(f)
            if N > 1000:
                print(f"Missing file: {target_f}")
                print('Number of lines in coordinates file (>1000):', count_lines(f))