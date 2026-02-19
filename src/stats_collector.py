"""
지하철 승하차 인원 통계 수집기 (CSV 기반)
- 용도: 제공된 CSV 파일(시간대별 승하차 인원)을 읽어 정제된 JSON으로 변환
- 실행 주기: 배치(Batch) 실행 (데이터 업데이트 시)
"""
import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

def process_csv_data(csv_path: str):
    """
    CSV 파일을 읽어 분석하기 좋은 형태(Long Format)로 변환합니다.
    """
    print(f"📂 CSV 파일 로딩 중: {csv_path}")
    
    try:
        # 1. CSV 읽기 (인코딩은 상황에 따라 'utf-8', 'cp949', 'euc-kr' 확인 필요)
        # 제공해주신 데이터 예시를 볼 때 utf-8 또는 cp949일 가능성이 높음
        try:
            df = pd.read_csv(csv_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding='cp949')

        print(f"   - 원본 데이터 크기: {len(df)} 행")

        # 2. 불필요한 컬럼 제거 및 정리
        # '연번' 등 분석에 필요 없는 컬럼 제외
        # 시간대 컬럼들을 식별하기 위해 고정 컬럼을 정의
        id_vars = ['날짜', '호선', '역번호', '역명', '구분']
        
        # 시간대 컬럼만 추출 (나머지 컬럼)
        value_vars = [c for c in df.columns if c not in id_vars and c != '연번']

        # 3. Melt: 가로로 긴 데이터를 세로로 변환 (Unpivot)
        # 변경 전: [역명, 06시, 07시 ...]
        # 변경 후: [역명, 시간대, 인원수]
        melted_df = df.melt(
            id_vars=id_vars, 
            value_vars=value_vars,
            var_name='시간대', 
            value_name='인원수'
        )

        # 4. 시간대 데이터 정제 (숫자로 변환)
        # "06시-07시" -> "06", "06시 이전" -> "05", "24시 이후" -> "24" 등 매핑
        def clean_time(t_str):
            if "이전" in t_str:
                return "05" # 편의상 05시로 처리
            elif "이후" in t_str:
                return "24"
            else:
                return t_str.split("시")[0] # "07시-08시" -> "07"

        melted_df['hour'] = melted_df['시간대'].apply(clean_time)
        
        # 5. 데이터 타입 변환 (인원수에 콤마가 있을 수 있음)
        # 숫자형 변환
        melted_df['인원수'] = pd.to_numeric(melted_df['인원수'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)

        # 6. 컬럼명 영문 변환 (Spark/DB 호환성을 위해)
        final_df = melted_df.rename(columns={
            '날짜': 'date',
            '호선': 'line_num',
            '역번호': 'station_code',
            '역명': 'station_name',
            '구분': 'type', # 승차/하차
            '인원수': 'count'
        })

        # 필요한 컬럼만 선택
        final_df = final_df[['date', 'line_num', 'station_code', 'station_name', 'type', 'hour', 'count']]
        
        # 딕셔너리 리스트로 변환
        result_list = final_df.to_dict(orient='records')
        
        print(f"✅ 데이터 변환 완료: 총 {len(result_list)}건의 시간대별 데이터 생성")
        return result_list

    except Exception as e:
        print(f"❌ CSV 처리 중 오류 발생: {e}")
        return []

def save_stats_to_json(data_list: list, output_dir: str = None) -> str:
    """변환된 데이터를 JSON 파일로 저장"""
    if output_dir is None:
        output_dir = os.environ.get("DATA_DIR", "./data")
    
    stats_dir = Path(output_dir) / "stats"
    stats_dir.mkdir(parents=True, exist_ok=True)
    
    # 파일명 생성 (오늘 날짜 기준)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"passenger_stats_csv_{timestamp}.json"
    filepath = stats_dir / filename
    
    final_data = {
        "source": "csv_file",
        "collected_at": datetime.now().isoformat(),
        "total_rows": len(data_list),
        "data": data_list
    }
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
        
    print(f"💾 저장 완료: {filepath}")
    return str(filepath)

if __name__ == "__main__":
    # 1. CSV 파일 경로 지정 (data 폴더 안에 파일을 넣어주세요)
    # 예: data/station_passenger.csv
    csv_file_path = "data/station_passenger.csv" 
    
    if not os.path.exists(csv_file_path):
        print(f"⚠️ 파일을 찾을 수 없습니다: {csv_file_path}")
        print("CSV 파일을 'data/station_passenger.csv'로 저장 후 다시 실행해주세요.")
    else:
        # 2. 처리 및 저장
        processed_data = process_csv_data(csv_file_path)
        if processed_data:
            save_stats_to_json(processed_data)