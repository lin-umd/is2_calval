import h5py
import os

def append_h5_files(source_file, destination_file):
    with h5py.File(source_file, 'r') as src, h5py.File(destination_file, 'a') as dest:
        for key in src.keys():
            if key in dest:
                # If the dataset exists, append the data
                if isinstance(src[key], h5py.Dataset):
                    src_data = src[key][:]
                    dest_data = dest[key]
                    
                    # Ensure the datasets are compatible for appending
                    if src_data.shape[1:] != dest_data.shape[1:]:
                        raise ValueError(f"Shape mismatch: Cannot append {src_data.shape} to {dest_data.shape}")
                    
                    # Resize the destination dataset to accommodate the new data
                    dest_data.resize(dest_data.shape[0] + src_data.shape[0], axis=0)
                    dest_data[-src_data.shape[0]:] = src_data
            else:
                # If the dataset doesn't exist, create it and copy the data
                if isinstance(src[key], h5py.Dataset):
                    src_data = src[key][:]
                    maxshape = (None,) + src_data.shape[1:]  # Allow unlimited growth along the first dimension
                    dest.create_dataset(key, data=src_data, maxshape=maxshape, chunks=True)
                elif isinstance(src[key], h5py.Group):
                    # If it's a group, recursively copy its contents
                    dest.create_group(key)
                    append_h5_files(src[key], dest[key])

    print(f"Appended data from '{source_file}' to '{destination_file}'.")

# Example usage for multiple files
source_files = [
    '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/splitV3/wave_ATL08_20220213041638_08081402_006_01_gt1l_4.h5',
    '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/splitV3/wave_ATL08_20211227185523_00841406_006_01_gt2l_01.h5'
]
destination_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/merged.h5'

# Create or clear the destination file
if os.path.exists(destination_file):
    os.remove(destination_file)
with h5py.File(destination_file, 'w') as _:
    pass

# Append all source files into the destination file
for source_file in source_files:
    append_h5_files(source_file, destination_file)

print(f"All files have been merged into '{destination_file}'.")