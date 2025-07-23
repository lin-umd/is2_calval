#!/bin/bash
echo 'merge tifs'
ls -1 /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/chm/*_wgs84.tif > tiff_list.txt
gdal_merge.py -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/report/global_calval_gridded_1km.tif --optfile tiff_list.txt