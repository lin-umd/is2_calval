#!/bin/bash
echo "Hello, World!"
# module load lastools
# module load wine
# export LASTOOLS="/gpfs/data1/vclgp/software/lastools/bin"
#wine $LASTOOLS/lascanopy.exe -h
# normalized

#echo 'normalized laz files'
#wine $LASTOOLS/lasheight.exe -drop_above 150 -drop_below -3  -replace_z -i /gpfs/data1/vclgp/data/gedi/imported/africa/amani/LAZ_ground/0154*laz -merged -o ../tmp_test/test_norm.laz -olaz

#echo 'lasgrid to several laz files'
#wine $LASTOOLS/lasgrid.exe -i ../tmp_test/test_norm.laz -merged -o ../tmp_test/testchm_lasgrid_1m.tif -step 1 -highest

#echo 'blast2dem'
#wine $LASTOOLS/blast2dem.exe  -i ../tmp_test/test_norm.laz  -otif  -o ../tmp_test/testchm_blast_1m.tif


#echo "get boundary files"
#wine $LASTOOLS/lasboundary.exe -i /gpfs/data1/vclgp/data/gedi/imported/usa/neon_stei2022/LAZ_ground/NEON_D05_STEI_DP1_L045-1_2022060913_unclassified_point_cloud_0000001.laz -o ../tmp_test/test_neon_1.shp

# echo "get boundary files"
# wine $LASTOOLS/lasboundary.exe -i /gpfs/data1/vclgp/data/gedi/imported/usa/neon_stei2022/LAZ_ground/*.laz -odir /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/lasboundary/usa/neon_stei2022 -oshp  -cores 4


echo "tile data test"
wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/X21074Y07968.laz -o /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/X21074Y07968tile.laz  -tile_size 500