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
            
            output_file = output_file.replace('splitV3', 'simV3')  # Replace 'splitV3' with 'simV3'
            output_file = re.sub(r'_[^_]+(?=\.h5$)', '', output_file)
            if os.path.exists(output_file):
                pass
                #print(f"Skipping {output_file}, already exists in simulation folder.")
            else:
                cmds_name.append(c)
    return cmds_name


def main():


    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run simulation split by name.")
    parser.add_argument(
        "--name_file",
        type=str,
        required=True,
        help="Path to the valid ALS sites file."
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
    for c in cmds_name:
        print(c)

if __name__ == "__main__":
    main()