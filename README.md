# What is this repository for?
This calval repository is used for simulating on-orbit ICESat-2 segments against ALS datasets. The ALS calval sites include over 140 sites across a variety of forest types. ICESat-2 has a geolocation accuracy of 6.5 meters (1 σ) . We don't collocate ICESat-2 segments with ALS data. The simulation is adapted from GEDI simulator. 
The simulator is adapted from the GEDI simulator and utilizes the ICESat-2 pulse shape, with a footprint size of 11 m (Purslow et al., 2023). For every 20-m on-orbit segment across ALS sites, we simulated 28 waveforms along the track, given a laser footprint interval of 0.7 m on the ground. The number of signal photons over vegetated surfaces is about 140 for a 100-m segment (A. Neuenschwander & Pitts, 2019). Consequently, a 20 m segment contains roughly 28 signal photons, averaging one photon per waveform. We applied a Poisson distribution with a mean value of one to determine the number of photons sampled from each waveform. Empirically, the canopy-to-ground reflectance ratio (ρv/ρg) was set to an average value of 0.86 across five study sites (Purslow et al., 2023). Scaling the simulated canopy waveforms using the ρv/ρg ratio helps mitigate bias introduced by the difference in laser wavelengths between ICESat-2 and ALS. This assumption is appropriate for our study, as ρv/ρg has a stronger effect on lower relative height (RH) metrics than on RH98 (Armston et al., 2013). 

# Steps for getting cal/val database. 
## Activate conda environment
conda activate gedih3
# step 1: check epsg of all als sites
python src/check_als_epsg.py 
# step 2: extract bounds of als las files and merge into one 
python src/get_als_bound.py --merge --update # update all laz bounds.
# step 3: use h3 tool to extract all IS2 segments across als sites. 
conda activate /gpfs/data1/vclgp/xiongl/env/ih3

bash extract.sh

# step 4: is2 data projected to each als site by epsg
python src/is2_rojection.py

# step 5: run simulation
python src/is2_simulation_20m.py --output ../result/lamda1 --test --ratiopvpg 0.85

# step 6: get ALS rh metrics and slope.
python src/get_segment_als_statistics.py --out /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/result_als_stat

# step 7: comnine is2, simulation, and als metrics. make plots.
python src/export_metrics.py

# step 8: make reports.



# get canopy/ground ratio.
code from John.

cp /gpfs/data1/vclgp/armstonj/git/gedipy/notebooks/icesat2_atl03_atl08_canopy_cover.ipynb ./icesat2_atl03_atl08_canopy_cover.ipynb



# Troubleshooting
If you encounter any issues/bugs when running the code, please contact lxiong@umd.edu.


