#!/bin/bash
# https://urs.earthdata.nasa.gov/documentation/for_users/data_access/curl_and_wget
# -- Processing  inpe_brazil31981
# -- Bounding box: [-60.04077445600972, -13.405348861024516, -54.00026328348745, -0.01735541859341852]
# We are already authenticated with NASA EDL
# Total number of data order requests is  1  for  1898  granules.
# Data request  1  of  1  is submitting to NSIDC
# order ID:  5000004803002
wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies -nc -i https://n5eil02u.ecs.nsidc.org/esir/5000004825858.txt  -P /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/atl03_data/W688S49