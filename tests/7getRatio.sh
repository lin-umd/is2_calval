#!/bin/bash
echo "Extracting IS2 for ratio!"
ih3_extract_shots -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ratio_100m -r /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/all_sites_20231218.parquet --atl08 land_segments/canopy/h_canopy land_segments/canopy/photon_rate_can land_segments/terrain/photon_rate_te root_file -q '`land_segments/canopy/h_canopy` < 200' -n 20  --strong_beam  --merge