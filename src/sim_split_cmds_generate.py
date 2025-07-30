'''
this script split coordinates.txt file by every seg_threshold  e.g. 28*200 = 5600 footprints. and output gediRat commands.
'''
import glob,os
import pandas as pd
import shutil

seg_threshold = 5600
# 28*40= 1120 footprints
output_dir = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result'
simV3_dir = output_dir + '/simV3'
files = glob.glob(simV3_dir +"/*/*/coordinates*.txt")
# split and save files 
split_path = output_dir + '/splitV3'
os.makedirs(split_path, exist_ok=True)

# Build the data list using a list comprehension
data = [
    {
        'file': file,
        'footprint_count': sum(1 for _ in open(file, 'r')),
        'region': os.path.basename(os.path.dirname(os.path.dirname(file))),  # Extract region
        'name': os.path.basename(os.path.dirname(file))  # Extract name
    }
    for file in files
]
# Convert the list to a DataFrame
df = pd.DataFrame(data)

df_thresh = df[df['footprint_count'] >= seg_threshold]

print(f'get files list with more than {seg_threshold} footprints')


for row in df_thresh.itertuples():
    f = row.file
    # copy alslist file to the split path
    out_path = split_path + '/' + row.region + '/' + row.name
    os.makedirs(out_path, exist_ok=True)
    bs =os.path.basename(f)
    
    als_list_file = bs.replace('coordinates', 'alslist')
    als_list_file = os.path.join(simV3_dir, row.region, row.name, als_list_file)
    #shutil.copy(als_list_file, out_path) 
    #split coordinates file
    with open(f, 'r') as infile:
        lines = infile.readlines()
    basename = os.path.basename(f)
    # do i need to read line coordinates and sort by y?
    # in version 1 simulation, coordinates are not sorted.
    # but it is sorted by time , default. when i genearate h3 database.

    # Split the file into chunks of seg_threshold lines
    chunk_size = seg_threshold
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i + chunk_size]
        output_file = f"{out_path}/{basename[:-4]}_{i // chunk_size + 1}.txt"
        with open(output_file, 'w') as outfile: # overwrite?
            outfile.writelines(chunk)
        print(f"Saved {output_file}")

# generate gediRat commands and save to file
cmd_path = output_dir + '/gediRat_commands'
cmds = []
Pulse_PATH = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/20190821.gt1l.pulse'
# get all files of split txt 
coor_files = []
for f in files:
    bs = os.path.basename(f)
    # got to split path and find the file 
    coor_files.extend(glob.glob(f"{split_path}/*/*/{bs[:-4]}_*.txt"))
for c in coor_files:
    als_files = c.replace('coordinates' , 'alslist')
    # remove the content between last _ and .txt
    als_files = als_files.rsplit('_', 1)[0] + '.txt'
    # remove the content between last _ and .txt
    als_files = als_files.rsplit('_', 1)[0] + '.txt'
    out_wave =  c.replace('coordinates', 'wave')[:-4] + '.h5'
    cmd = f'gediRat -fSigma 2.75 -readPulse {Pulse_PATH} -inList {als_files} -listCoord {c}  -output {out_wave} -hdf  -ground'
    cmds.append(cmd)
# wrtie cmds to file
os.makedirs(cmd_path, exist_ok=True)
cmd_file = os.path.join(cmd_path, 'gediRat_commands_split.txt')
with open(cmd_file, 'w') as f:
    for cmd in cmds:
        f.write(cmd + '\n')