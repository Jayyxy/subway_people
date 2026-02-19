import duckdb
import pandas as pd

print("🕵️‍♂️ [1단계] CSV 원본 데이터 정밀 검사...")
try:
    df_csv = pd.read_csv('data/station_passenger.csv', encoding='cp949')
except Exception:
    df_csv = pd.read_csv('data/station_passenger.csv', encoding='utf-8')

df_csv.columns = [
    '연번', '날짜', '호선', '역번호', '역명', '구분', 
    '06시 이전', '06시-07시', '07시-08시', '08시-09시', '09시-10시', 
    '10시-11시', '11시-12시', '12시-13시', '13시-14시', '14시-15시', 
    '15시-16시', '16시-17시', '17시-18시', '18시-19시', '19시-20시', 
    '20시-21시', '21시-22시', '22시-23시', '23시-24시', '24시 이후'
]

con = duckdb.connect()
con.execute("CREATE TABLE csv_data AS SELECT * FROM df_csv")

# 1. CSV 데이터 강제 추출 (서울역 기준)
print("\n[CSV 데이터] '서울역' 하차 데이터 샘플:")
csv_sample = con.execute("""
    SELECT 
        TRIM(역명) AS station_name, 
        TRIM(구분) AS type,
        "15시-16시" AS count_15,
        "16시-17시" AS count_16
    FROM csv_data 
    WHERE 역명 LIKE '%서울%' AND 구분 LIKE '%하차%'
    LIMIT 1
""").fetchdf()
print(csv_sample)

# 2. JSON 데이터 강제 추출 (서울역 기준)
print("\n[JSON 데이터] '서울' 실시간 데이터 샘플:")
json_sample = con.execute("""
    SELECT 
        TRIM(arr.station_name) AS station_name,
        CAST(strftime(CAST(collected_at AS TIMESTAMP), '%H') AS INTEGER) AS current_hour
    FROM read_json_auto('data/raw/arrivals_*.json', ignore_errors=true), 
    UNNEST(arrivals) AS arr
    WHERE arr.station_name LIKE '%서울%'
    LIMIT 1
""").fetchdf()
print(json_sample)

print("\n🚨 체크리스트:")
if csv_sample.empty:
    print("❌ CSV에 '서울역 하차' 데이터가 없습니다. (CSV 인코딩/파싱 문제)")
elif json_sample.empty:
    print("❌ JSON에 '서울' 데이터가 없습니다. (JSON 수집/시간 문제)")
else:
    print("✅ 양쪽 다 데이터가 있습니다! 둘의 'station_name'이나 시간(hour) 값을 직접 눈으로 비교해보세요.")
    