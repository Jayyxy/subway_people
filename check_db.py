import duckdb
import pandas as pd
import json
import glob

print("🔎 조인(Join) 실패 원인 분석 스크립트 가동 중...\n")

# 1. 인코딩 뚫고 CSV 가져오기
df_csv = None
for enc in ['cp949', 'utf-8-sig', 'utf-8', 'euc-kr']:
    try:
        df = pd.read_csv('data/station_passenger.csv', encoding=enc)
        # '서울'이라는 글자가 제대로 읽혔는지 확인
        if df.astype(str).apply(lambda x: x.str.contains('서울')).any().any():
            df_csv = df
            print(f"✅ CSV 로드 성공! (발견된 인코딩: {enc})")
            break
    except: pass

if df_csv is None:
    print("🚨 CSV에서 한글 데이터를 찾지 못했습니다. 파일이 깨졌습니다!")
    exit()

df_csv.columns = [
    '연번', '날짜', '호선', '역번호', '역명', '구분', 
    '06시 이전', '06시-07시', '07시-08시', '08시-09시', '09시-10시', 
    '10시-11시', '11시-12시', '12시-13시', '13시-14시', '14시-15시', 
    '15시-16시', '16시-17시', '17시-18시', '18시-19시', '19시-20시', 
    '20시-21시', '21시-22시', '22시-23시', '23시-24시', '24시 이후'
]

# 2. JSON 가져오기
all_data = []
for f in glob.glob('data/raw/arrivals_*.json'):
    with open(f, 'r', encoding='utf-8') as file:
        data = json.load(file)
        c_at = data.get('collected_at')
        for arr in data.get('arrivals', []):
            arr['collected_at'] = c_at
            all_data.append(arr)
df_json = pd.DataFrame(all_data)

# 3. DuckDB로 양쪽 데이터 직접 비교
con = duckdb.connect()
con.execute("CREATE TABLE csv_data AS SELECT * FROM df_csv")
con.execute("CREATE TABLE json_data AS SELECT * FROM df_json")

print("\n--- [용의자 1] CSV의 진짜 '역명'과 '구분'은 어떻게 생겼는가? ---")
print(con.execute("SELECT DISTINCT 역명, 구분 FROM csv_data WHERE 역명 LIKE '%서울%'").fetchdf())

print("\n--- [용의자 2] JSON의 '현재 시간'은 숫자로 잘 변환되었는가? ---")
print(con.execute("SELECT DISTINCT collected_at, CAST(strftime(CAST(collected_at AS TIMESTAMP), '%H') AS INTEGER) AS json_hour FROM json_data LIMIT 3").fetchdf())

print("\n--- [용의자 3] 합쳐질 준비가 된 CSV 베이스라인 샘플 (시간대별 하차 인원) ---")
res3 = con.execute('''
    WITH unpivoted AS (
        UNPIVOT (SELECT * FROM csv_data)
        ON "06시 이전", "06시-07시", "07시-08시", "08시-09시", "09시-10시", 
           "10시-11시", "11시-12시", "12시-13시", "13시-14시", "14시-15시", 
           "15시-16시", "16시-17시", "17시-18시", "18시-19시", "19시-20시", 
           "20시-21시", "21시-22시", "22시-23시", "23시-24시", "24시 이후"
        INTO NAME time_slot VALUE passenger_count
    )
    SELECT REPLACE(TRIM(역명), '역', '') as st_name, 
           CAST(CASE WHEN time_slot='06시 이전' THEN '5' WHEN time_slot='24시 이후' THEN '24' ELSE SUBSTRING(time_slot, 1, 2) END AS INTEGER) as h,
           TRIM(구분) as typ,
           AVG(CAST(REPLACE(CAST(passenger_count AS VARCHAR), ',', '') AS INTEGER))::INTEGER as cnt
    FROM unpivoted
    WHERE 역명 LIKE '%서울%' AND 구분 LIKE '%하차%'
    GROUP BY 1, 2, 3
    ORDER BY h
    LIMIT 5
''').fetchdf()
print(res3)