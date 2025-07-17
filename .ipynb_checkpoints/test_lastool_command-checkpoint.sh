#!/bin/bash
echo 'grid data test'
wine $LASTOOLS/lasgrid.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/ease1km_tile/classified/X21080Y07960tile_1128500_9431500_classified.laz -o ../tmp_test/dem.tif -step 25 -highest


# -odir /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/lazReTile/usa/neon_stei2022
# wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/data/gedi/imported/usa/neon_stei2022/LAZ_ground/NEON_*.laz -tile_size 1024 -buffer 5 -refine_tiling 10000000  -o neon_stei2022.laz          

# wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/lazReTile/usa/neon_stei2022/neon_stei2022*_1024.laz \
#     -refine_tiling 10000000 \
#     -olaz \
#     -cores 4  

# wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/lazReTile/usa/neon_stei2022/neon_stei2022*_512.laz \
#     -refine_tiling 10000000 \
#     -olaz \
#     -cores 4  

# wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/lazReTile/usa/neon_stei2022/neon_stei2022*_256.laz \
#     -refine_tiling 10000000 \
#     -olaz \
#     -cores 4          

# wine $LASTOOLS/lastile.exe -i /gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/lazReTile/usa/neon_stei2022/neon_stei2022*_128.laz \
#     -refine_tiling 10000000 \
#     -olaz \
#     -cores 4  