'''
this script is used to run the splitted simulation commands from gediRat_commands.txt
'''

import os
import dask
from dask.diagnostics import ProgressBar
import re
import argparse
simV3 = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/simV3/'
# File paths
cmd_file = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/gediRat_commands/gediRat_commands.txt'
#name_file = '/gpfs/data1/vclgp/xiongl/cluster/cluster04/valid_als_sites.txt'


def read_commands(file):
    """Read commands from a file."""
    cmds = []
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line:
                cmds.append(line)
    return cmds

def read_project_names(f_name):
    """Read project names from a file."""
    with open(f_name, 'r') as f:
        names = f.readlines()
        names = [name.strip() for name in names if name.strip()]
    return names

def split_commands_by_name(cmds, names):
    """Split commands by project name."""
    cmds_name = []
    for c in cmds:
        if any(name in c for name in set(names)): # check if command contains the project name
            # check if wave file already exists?
            
            match = re.search(r"-output\s+(\S+)", c)
            output_file = match.group(1)
            # check if output file is in simV3 directory
            output_file = output_file.replace('splitV3', 'simV3')  # Replace 'splitV3' with 'simV3'
            output_file = re.sub(r'_[^_]+(?=\.h5$)', '', output_file) # remove the last part before .h5
            if os.path.exists(output_file):
                pass
                #print(f"Skipping {output_file}, already exists.")
            else:
                cmds_name.append(c)
    return cmds_name

def run_cmd(cmd):
    """Run a single command."""
    match = re.search(r"-output\s+(\S+)", cmd)
    output_file = match.group(1)
    if os.path.exists(output_file):
        print(f"Skipping {output_file}, already exists in split simulation folder.")
    else:
        os.system(cmd)

def main():


    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run simulation split by name.")
    parser.add_argument(
        "--name_file",
        type=str,
        required=True,
        help="Path to the valid ALS sites file."
    )
    parser.add_argument(
    "--num_workers",
    type=int,
    default=35,  # Default number of workers
    help="Number of workers to use for parallel processing."
    )
    args = parser.parse_args()
    name_file = args.name_file
    # Read commands and project names
    cmds = read_commands(cmd_file)
    names = read_project_names(name_file)

    # Split commands by project name
    cmds_name = split_commands_by_name(cmds, names)
    cmds_name = [item for item in cmds_name if item is not None]

    print('number of split running commands: ', len(cmds_name))
    # Run commands in parallel
    futures = [dask.delayed(run_cmd)(cmd) for cmd in cmds_name]

    # Use a progress bar and compute
    with ProgressBar():
        dask.compute(*futures, scheduler="threads", num_workers=args.num_workers)  # or scheduler="processes"

if __name__ == "__main__":
    main()