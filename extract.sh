#!/bin/bash
# 11/27/2023
# may need to update output folder and als boundary file.

#ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_11272023 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/als_bounds_20231122.gpkg --atl08 land_segments/canopy/h_canopy_20m orbit_info/sc_orient root_file root_beam  land_segments/delta_time  land_segments/terrain/h_te_best_fit land_segments/canopy/n_ca_photons land_segments/night_flag --geo -q '`land_segments/canopy/n_ca_photons` < 140 and `land_segments/terrain/h_te_best_fit` > -999 and `land_segments/terrain/h_te_best_fit` < 3.402823e+23' -q_20m '`land_segments/canopy/h_canopy_20m` > 0 and `land_segments/canopy/h_canopy_20m` < 200'  -n 20  --merge

#### Mikhai sites
# do not use any filter.
echo 'Extract is2 points in Mikhai sites'
#ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_misha_11282023 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/misha_bounds_20231128.gpkg --atl08 land_segments/canopy/h_canopy_20m orbit_info/sc_orient root_file root_beam  land_segments/delta_time  land_segments/terrain/h_te_best_fit land_segments/canopy/n_ca_photons land_segments/night_flag --geo -q '`land_segments/canopy/n_ca_photons` < 140 and `land_segments/terrain/h_te_best_fit` > -999 and `land_segments/terrain/h_te_best_fit` < 3.402823e+23' -q_20m '`land_segments/canopy/h_canopy_20m` > 0 and `land_segments/canopy/h_canopy_20m` < 200'  -n 15  --merge
echo "No filters..."
# ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_misha_11302023 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/misha_bounds_20231128.gpkg --atl08 land_segments/canopy/h_canopy_20m orbit_info/sc_orient root_file root_beam  land_segments/delta_time  land_segments/terrain/h_te_best_fit land_segments/canopy/n_ca_photons land_segments/night_flag land_segments/solar_elevation land_segments/canopy/h_canopy_uncertainty land_segments/terrain/h_te_median land_segments/terrain/h_te_uncertainty  --geo -n 15  --merge

echo '100m segments'
# ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_100m_misha_11302023 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/misha_bounds_20231128.gpkg --atl08 land_segments/canopy/h_canopy orbit_info/sc_orient root_file root_beam  land_segments/delta_time  land_segments/terrain/h_te_best_fit land_segments/canopy/n_ca_photons land_segments/night_flag land_segments/solar_elevation land_segments/canopy/h_canopy_uncertainty land_segments/terrain/h_te_median land_segments/terrain/h_te_uncertainty  --geo -n 15  --merge

#cal/val 
echo 'cal/val 20m segments, 12262023, canopy height < 200m '
#ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_12262023 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet --atl08 land_segments/canopy/h_canopy_20m orbit_info/sc_orient root_file root_beam  land_segments/delta_time  land_segments/terrain/h_te_best_fit land_segments/canopy/n_ca_photons land_segments/night_flag land_segments/solar_elevation land_segments/canopy/h_canopy_uncertainty land_segments/terrain/h_te_median land_segments/terrain/h_te_uncertainty -q_20m '`land_segments/canopy/h_canopy_20m` < 200' --geo -n 15  --merge

echo "get snr in cal/val"
#ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/is2_20m_cal_val_snr_20240305 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet --atl08 land_segments/canopy/h_canopy_20m land_segments/delta_time  land_segments/snr  -q_20m '`land_segments/canopy/h_canopy_20m` < 200' --geo -n 15  --merge

echo "get loss year"
ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/is2_20m_cal_val_disturbed_20240402 -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet --atl08 land_segments/canopy/h_canopy_20m land_segments/delta_time -a "{'glad_forest_loss':['lossyear']}"  -q_20m '`land_segments/canopy/h_canopy_20m` < 200' --geo -n 15  --merge 

# Note
#[h_canopy_20m, land_segments/terrain/h_te_best_fit, land_segments/canopy/n_ca_photons, orbit_info/sc_orient, root_beam, land_segments/delta_time, land_segments/night_flag]
#terrain height is above WGS84 Ellipsoid. 
