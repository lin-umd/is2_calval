'''
use this script to add greeness of calval data.
'''

import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

df = pd.read_parquet('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/report/is2_calval_stat_v20240129.parquet')
df = df[['laz' ,'land_segments/canopy/h_canopy_20m' ,'land_segments/latitude', 'land_segments/longitude', 'h_canopy_98']]
df_green_p1= pd.read_parquet('../result/greeness_calval_p1_02282024.parquet')
df_green_p2= pd.read_parquet('../result/greeness_calval_p2_02282024.parquet')
#  meta_df = {'land_segments/longitude_20m':'float64',
# 'land_segments/latitude_20m':'float64',
# 'land_segments/delta_time': 'float64',
# 'greeness_max_p2': 'uint16',
# 'greeness_dec_p2': 'uint16'}
# use join 
result = pd.concat([df, df_green_p1], axis=1)
df_green_p2 = df_green_p2[['greeness_max_p2','greeness_dec_p2']]
result = pd.concat([result, df_green_p2], axis=1)
print(len(result))
print(result.columns.values)
print('make sure common cols have same data type to float32')
# convert float 
numeric_cols = ['land_segments/canopy/h_canopy_20m' ,'land_segments/delta_time','land_segments/latitude', 'land_segments/longitude', 'h_canopy_98']
result[numeric_cols] = result[numeric_cols].astype('float32')

df_gee = pd.read_parquet('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/cal_cal_add_anci_gee_v20240229.parquet')
# Drop the 'B' column
df_gee = df_gee.drop('.geo', axis=1)
df_gee[numeric_cols] = df_gee[numeric_cols].astype('float32')
# use what ?
# 'laz' ,'land_segments/canopy/h_canopy_20m' ,'land_segments/delta_time','land_segments/latitude', 'land_segments/longitude', 'h_canopy_98'
# Merge the dataframes on three common columns
final = pd.merge(df_gee, result, on=['laz' ,'land_segments/canopy/h_canopy_20m' ,'land_segments/delta_time','land_segments/latitude', 'land_segments/longitude', 'h_canopy_98'], how='inner')
print('final data :', len(final))
print(final.head())
final.to_parquet('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/is2_calval_add_anci_v20240301.parquet')