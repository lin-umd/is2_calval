# What is this repository for?
This calval repository is used for simulating on-orbit ICESat-2 segments against ALS datasets. 


# Steps for getting cal/val database. 
# Activate environment
conda activate /gpfs/data1/vclgp/xiongl/env/linpy
# check als sites epsg
python check_als_epsg.py
### 

# get all bounds of als las files. 
python 0_get_als_region.py --merge --update # update all laz bounds.

# get all IS2 beams, including strong/weak beams, save in a folder
conda activate /gpfs/data1/vclgp/xiongl/env/ih3

bash 0_extract.sh

# is2 data projected to each als site by epsg

python 1_getProjectProjection.py

# run simulation
python 2_is2simulation_20m.py --lamda 1 --output ../result/lamda1 --test --ratiopvpg 0.75

# get canopy/ground ratio.
code from John. 

cp /gpfs/data1/vclgp/armstonj/git/gedipy/notebooks/icesat2_atl03_atl08_canopy_cover.ipynb ./icesat2_atl03_atl08_canopy_cover.ipynb






