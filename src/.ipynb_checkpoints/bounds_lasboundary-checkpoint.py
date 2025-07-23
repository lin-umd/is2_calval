#!/usr/bin/env python
"""
python script to get accurate laz file boudnaries
lin xiong
"""
import os, glob
lazfiles = glob.glob('/gpfs/data1/vclgp/data/gedi/imported/usa/neon_stei2022/LAZ_ground/*.laz')
for f in lazfiles:
    bs = os.path.basename(f)
    outshp = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result/lasboundary/usa/neon_stei2022/' + bs[:-3] + 'shp'
    command = f"wine $LASTOOLS/lasboundary.exe -i {f} -o {outshp}"
    os.system(command)
    print('ouput bound:', outshp)