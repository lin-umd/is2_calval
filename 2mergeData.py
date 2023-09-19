import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import os

input_directory = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/out'
output_directory = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/result'
parquet_files = [f for f in os.listdir(input_directory) if f.endswith('.parquet')]
dfs = []
for file in parquet_files[:10]:
    file_path = os.path.join(input_directory, file)
    print(file_path)
    df =gpd.read_parquet(file_path)
    dfs.append(df)

merged_df = gpd.GeoDataFrame(pd.concat(dfs, ignore_index=True), crs=df.crs)
#merged_df = gpd.concat(dfs, ignore_index=True)
f_out = os.path.join(output_directory, 'is2_calval.parquet')
merged_df.to_parquet(f_out)
