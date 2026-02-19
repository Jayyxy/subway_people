"""
DuckDB 기반 모던 데이터 파이프라인 (Hybrid 구조)
- 역할: Pandas로 안전하게 데이터를 로드하고, DuckDB로 초고속 인메모리 SQL 조인
"""
import duckdb
import pandas as pd
import glob
import json
import os

def load_csv_safely():
    print("📂 CSV 파일 로드 및 한글 복구 중...")
    csv_path = 'data/station_passenger.csv'
    
    # Mac 환경에서는 utf-8일 확률이 높으므로 먼저 시도!
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding='cp949')
        
    df.columns = [
        '연번', '날짜', '호선', '역번호', '역명', '구분', 
        '06시 이전', '06시-07시', '07시-08시', '08시-09시', '09시-10시', 
        '10시-11시', '11시-12시', '12시-13시', '13시-14시', '14시-15시', 
        '15시-16시', '16시-17시', '17시-18시', '18시-19시', '19시-20시', 
        '20시-21시', '21시-22시', '22시-23시', '23시-24시', '24시 이후'
    ]
    
    # 🔍 한글이 제대로 복구되었는지 확인
    print(f"   -> [확인] 첫 번째 역명: {df['역명'].iloc[0]}")
    return df

def load_json_safely():
    print("📂 JSON 파일 읽는 중 (빈 파일 필터링)...")
    json_files = glob.glob('data/raw/arrivals_*.json')
    all_data = []
    
    for f in json_files:
        with open(f, 'r', encoding='utf-8') as file:
            data = json.load(file)
            collected_at = data.get('collected_at')
            arrivals = data.get('arrivals', [])
            
            # 도착 정보가 있는(빈 배열이 아닌) 데이터만 쏙쏙 뽑아냅니다.
            for arr in arrivals:
                arr['collected_at'] = collected_at
                all_data.append(arr)
                
    if not all_data:
        raise ValueError("모든 JSON 파일에 도착 정보가 없습니다 (API 한도 초과 등)")
        
    return pd.DataFrame(all_data)

def process_data_with_duckdb():
    # 1. Pandas로 안전하게 메모리에 올리기
    df_csv = load_csv_safely()
    df_json = load_json_safely()
    
    print("\n🦆 DuckDB 엔진 가동 (인메모리 초고속 조인)...")
    con = duckdb.connect(database=':memory:')
    
    # 2. SQL 쿼리로 단번에 합치기 (df_csv, df_json 변수를 그대로 테이블처럼 씁니다!)
    join_query = """
    WITH baseline_view AS (
        WITH unpivoted AS (
            UNPIVOT (SELECT * FROM df_csv)
            ON "06시 이전", "06시-07시", "07시-08시", "08시-09시", "09시-10시", 
               "10시-11시", "11시-12시", "12시-13시", "13시-14시", "14시-15시", 
               "15시-16시", "16시-17시", "17시-18시", "18시-19시", "19시-20시", 
               "20시-21시", "21시-22시", "22시-23시", "23시-24시", "24시 이후"
            INTO NAME time_slot VALUE passenger_count
        ),
        cleaned AS (
            SELECT 
                TRIM(역명) AS station_name,
                TRIM(구분) AS type,
                CAST(
                    CASE 
                        WHEN time_slot = '06시 이전' THEN '5'
                        WHEN time_slot = '24시 이후' THEN '24'
                        ELSE SUBSTRING(time_slot, 1, 2)
                    END 
                AS INTEGER) AS hour_int,
                CAST(REPLACE(CAST(passenger_count AS VARCHAR), ',', '') AS INTEGER) AS p_count
            FROM unpivoted
        )
        SELECT station_name, hour_int, type, AVG(p_count)::INTEGER AS avg_passenger
        FROM cleaned
        GROUP BY station_name, hour_int, type
    ),
    real_time AS (
        SELECT 
            TRIM(station_name) AS station_name,
            train_line,
            arrival_message,
            arrival_time_sec,
            CAST(strftime(CAST(collected_at AS TIMESTAMP), '%H') AS INTEGER) AS current_hour_int
        FROM df_json
    )
    SELECT 
        r.station_name AS "역명",
        r.train_line AS "행선지",
        r.arrival_message AS "실시간_상태",
        COALESCE(b.avg_passenger, 0) AS "현재시간_예상하차인원(명)",
        CASE 
            WHEN b.avg_passenger > 5000 AND CAST(r.arrival_time_sec AS INTEGER) <= 60 
            THEN '🚨 혼잡 위험'
            ELSE '✅ 정상'
        END AS "플랫폼_위험도"
    FROM real_time r
    LEFT JOIN baseline_view b 
        ON REPLACE(r.station_name, '역', '') = REPLACE(b.station_name, '역', '') 
        AND r.current_hour_int = b.hour_int
        AND b.type = '하차'
    ORDER BY "현재시간_예상하차인원(명)" DESC NULLS LAST;
    """
    
    return con.execute(join_query).fetchdf()

if __name__ == "__main__":
    try:
        result_df = process_data_with_duckdb()
        print("\n✨ [결과 리포트: 실시간 열차 도착 및 하차 인원 예측]")
        print("="*70)
        print(result_df.head(20).to_string(index=False))
        print("="*70)
        
        result_df.to_csv("data/realtime_congestion_report.csv", index=False, encoding="utf-8-sig")
        print("💾 분석 결과가 data/realtime_congestion_report.csv 로 저장되었습니다.")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")