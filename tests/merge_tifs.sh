#!/bin/bash
echo 'remove previous result'
rm ../result_chm/amani.tif
echo 'merge tif files'
gdal_merge.py -o ../result_chm/amani.tif $(ls -1 /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/chm_1km/*.tif)