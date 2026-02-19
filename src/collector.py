"""
실시간 지하철 도착정보 수집기 (File 기반)
- 역할: API에서 실시간 데이터를 가져와 JSON 파일로 저장 (DuckDB가 읽을 용도)
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path
from api_client import SeoulMetroAPI

def collect_and_save_realtime_data():
    api = SeoulMetroAPI()
    
    # [수정] 수집할 역 목록 (필요한 역 추가 가능)
    stations = ["서울", "강남", "홍대입구", "신도림", "잠실"]
    
    # 데이터 저장 폴더 생성 (data/raw)
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    print("📡 실시간 데이터 수집 및 JSON 저장 시작...")
    
    while True:
        collected_at = datetime.now().isoformat()
        all_arrivals = []
        
        for station in stations:
            # API 호출
            data = api.get_arrival_info(station)
            
            if "realtimeArrivalList" in data:
                count = 0
                for item in data["realtimeArrivalList"]:
                    # DuckDB에서 읽기 편한 구조로 정리
                    all_arrivals.append({
                        # '강남역' -> '강남'으로 통일시켜 CSV 통계와 조인되게 만듦
                        "station_name": item.get("statnNm").replace("역", ""), 
                        "train_line": item.get("trainLineNm"),
                        "arrival_message": item.get("arvlMsg2"),
                        "arrival_time_sec": item.get("barvlDt", "0") # 남은 초
                    })
                    count += 1
                print(f" -> [수집 완료] {station}: {count}대 열차 대기 중")
            else:
                print(f" -> [데이터 없음] {station}")
        
        # 모은 데이터를 하나의 JSON 파일로 저장
        if all_arrivals:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = raw_dir / f"arrivals_{timestamp_str}.json"
            
            final_data = {
                "collected_at": collected_at,
                "arrivals": all_arrivals
            }
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
                
            print(f"💾 JSON 파일 저장 완료: {filepath} (총 {len(all_arrivals)}건)")
        
        # API 한도(1000회) 관리를 위해 10분 대기
        print("⏳ 10분 대기 중... (종료하려면 Ctrl+C)")
        time.sleep(600)

if __name__ == "__main__":
    collect_and_save_realtime_data()