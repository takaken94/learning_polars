import polars as pl
import pandas as pd
import time

# --- 設定 ---
# データファイル
csv_data_file_path = "data/22_shizuoka_all.csv"
parquet_data_file_path  = "data/22_shizuoka_all.parquet"

start_time = time.time()
# Parquet
parquet_df = pl.read_parquet(parquet_data_file_path)
#
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Polars Parquetファイルの読み込み時間: {elapsed_time}秒")
print(parquet_df.shape)

start_time = time.time()
# CSV
csv_df =pl.read_csv(csv_data_file_path)
#
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Polars CSVファイルの読み込み時間: {elapsed_time}秒")
print(csv_df.shape)

start_time = time.time()
# CSV
pd_csv_df =pd.read_csv(csv_data_file_path)
#
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Pandas CSVファイルの読み込み時間: {elapsed_time}秒")
print(pd_csv_df.shape)
