import h5py
import os

import h5py
import numpy as np
import glob
import os

def append_h5_file(src_path, dest_path):
    with h5py.File(src_path, 'r') as src, h5py.File(dest_path, 'a') as dest:
        _append_group(src, dest)

def _append_group(src_group, dest_group):
    for key in src_group:
        src_item = src_group[key]

        if isinstance(src_item, h5py.Dataset):
            data_to_append = src_item[()]
            if key in dest_group:
                dest_item = dest_group[key]

                # Check compatibility
                if dest_item.shape[1:] != data_to_append.shape[1:]:
                    raise ValueError(f"Shape mismatch for dataset '{key}'.")

                # Resize and append
                dest_item.resize(dest_item.shape[0] + data_to_append.shape[0], axis=0)
                dest_item[-data_to_append.shape[0]:] = data_to_append

            else:
                # Create resizable dataset
                maxshape = (None,) + data_to_append.shape[1:]
                dest_group.create_dataset(
                    key,
                    data=data_to_append,
                    maxshape=maxshape,
                    chunks=True,
                    compression="gzip"
                )

        elif isinstance(src_item, h5py.Group):
            if key not in dest_group:
                dest_group.create_group(key)
            _append_group(src_item, dest_group[key])

# files = glob.glob('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/splitV3/africa/csir_welverdient/wave*.h5')

# files.sort()
# print(files)
# destination_file = f'/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3/africa/csir_welverdient/wave_ATL08_20220113163740_03421408_006_01_gt3l.h5'
# if os.path.exists(destination_file):
#     print(f"Destination file '{destination_file}' already exists. Skipping...")
#     os.system.exit(0)
# else:
#     # Append all source files into the destination file
#     for source_file in files:
#         append_h5_file(source_file, destination_file)

#     print(f"All files have been merged into '{destination_file}'.")

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-m","--merge_list", type=str, required=True,
                    help="Path to merge_list.txt")
args = parser.parse_args()

dic_list  = []
# give a file and read to list 
#merge_list = '/gpfs/data1/vclgp/xiongl/cluster/cluster14/merge_list.txt'
# read the file and append to source_files
with open(args.merge_list, 'r') as f:
    for line in f:
        line = line.strip()
        if 'gediRat' in line:  # Check if the line is not empty
            #source_files.append(line)
            parts = line.split('/')
            bs = parts[-1]
            # remote the conten of last _ and .h5 like gt1r_2.h5
            bs = bs.rsplit('_', 1)[0] + '.h5'
            name = parts[-2]
            region = parts[-3]
            dic_list.append(region + '--' + name + '--' + bs)
print(len(dic_list))
# list to set to remove duplicates
dic_unique = list(set(dic_list))
print(len(dic_unique))
for d in dic_unique:

    region = d.split('--')[0]
    name = d.split('--')[1]
    bs = d.split('--')[2]
    files= glob.glob(f'/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/splitV3/{region}/{name}/{bs[:-3]}*.h5')
    # sort the files list by name
    files.sort()
    if len(files) ==0: 
        print('no files found in ', bs[:-3])
        continue
    print(files)
    #destination_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/wave_ATL08_20230128061946_05911806_006_01_gt3r.h5'
    destination_file = f'/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3/{region}/{name}/{bs}'
    #destination_file = f'/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/{bs}'
    # Create or clear the destination file
    if os.path.exists(destination_file):
        print(f"Destination file '{destination_file}' already exists. Skipping...")
        continue
    else:
        # Append all source files into the destination file
        for source_file in files:
            append_h5_file(source_file, destination_file)

        print(f"All files have been merged into '{destination_file}'.")