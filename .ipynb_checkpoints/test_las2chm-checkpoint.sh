#!/bin/sh
echo "Hello world"
myfile="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/norm/X21080Y07963tile_1128500_9428500_classified_norm.laz"
myfile="/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/norm/X21080Y07963tile_1128500_9429000_classified_norm.laz"
wine $LASTOOLS/lasthin.exe -i $myfile \
        -step 25 \
        -highest \
        -subcircle 5\
        -o ../tmp_test/temp.laz
wine $LASTOOLS/las2dem -i ../tmp_test/temp.laz \
        -step 25 \
        -o ../tmp_test/chm_tin_3.tif
