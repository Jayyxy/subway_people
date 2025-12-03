import sqlite3
import pandas as pd

conn = sqlite3.connect("database/subway.db")

print("--- [1] 특징 매핑 데이터 (Meta) ---")
df_meta = pd.read_sql("SELECT * FROM meta_station_feature LIMIT 5", conn)
print(df_meta)

print("\n--- [2] 승하차 히스토리 (Raw) ---")
df_raw = pd.read_sql("SELECT * FROM raw_station_history LIMIT 5", conn)
print(df_raw)

conn.close()