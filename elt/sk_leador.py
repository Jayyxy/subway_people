import os
import requests
import json
import time
from dotenv import load_dotenv

load_dotenv()

class SKCongestionLoader:
    def __init__(self):
        self.sk_key = os.getenv("SK_API_KEY")
        self.base_path = "./data/raw"
        os.makedirs(self.base_path, exist_ok=True)
        # SK API 호선 코드 (필요시 수정)
        self.target_lines = ["1호선", "2호선", "3호선", "4호선"]

    def _save_json(self, data, filename):
        filepath = os.path.join(self.base_path, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 [저장 완료] {filename}")

    def get_all_station_codes(self):
        """SK API에서 1~4호선 모든 역의 메타정보(코드) 가져오기"""
        print("\n--- [SK 1단계] 1~4호선 역 리스트 확보 ---")
        
        url = "https://apis.openapi.sk.com/puzzle/subway/meta/stations"
        headers = {"accept": "application/json", "appKey": self.sk_key}
        
        all_stations = []
        
        for line in self.target_lines:
            try:
                params = {"subwayLine": line}
                resp = requests.get(url, headers=headers, params=params)
                
                if resp.status_code == 200:
                    data = resp.json()
                    if 'contents' in data:
                        stations = data['contents']
                        print(f"  >> {line}: {len(stations)}개 역 발견")
                        all_stations.extend(stations)
                else:
                    print(f"[FAIL] {line} 조회 실패: {resp.status_code}")
                
            except Exception as e:
                print(f"[ERROR] {line} 메타 조회 중 에러: {e}")
            
            time.sleep(1) # 호선별 조회 간격
            
        self._save_json(all_stations, "sk_meta_stations.json")
        return all_stations

    def collect_congestion_data(self, station_list):
        """확보된 모든 역에 대해 혼잡도 조회 (천천히)"""
        print(f"\n--- [SK 2단계] {len(station_list)}개 역 혼잡도 상세 수집 ---")
        print("⚠️ 주의: API 호출 제한을 피하기 위해 천천히 진행합니다. 멈춘 것이 아닙니다.")
        
        base_url = "https://apis.openapi.sk.com/puzzle/subway/congestion/stat/car/stations"
        headers = {"accept": "application/json", "appKey": self.sk_key}
        
        results = []
        
        for idx, station in enumerate(station_list):
            code = station['stationCode']
            name = station['stationName']
            line = station['subwayLine']
            
            # 요청 URL 및 파라미터 (평일 08시 기준 예시)
            req_url = f"{base_url}/{code}"
            params = {"subwayLine": line, "dow": "WED", "hh": "08"}
            
            # 재시도 로직 (429 에러 대응)
            success = False
            for attempt in range(3):
                try:
                    resp = requests.get(req_url, headers=headers, params=params)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        data['meta_info'] = station # 메타 정보 합쳐서 저장
                        results.append(data)
                        success = True
                        break # 성공시 탈출
                    
                    elif resp.status_code == 429:
                        print(f"  [WAIT] {name}({line}) - 429 에러. 5초 대기 후 재시도 ({attempt+1}/3)")
                        time.sleep(5)
                    
                    else:
                        # 404 등은 데이터가 없는 역일 수 있으므로 패스
                        # print(f"  [SKIP] {name}: {resp.status_code}") 
                        break
                        
                except Exception as e:
                    print(f"  [ERROR] {name}: {e}")
                    break
            
            if success:
                # 진행 상황 출력 (10개 단위)
                if len(results) % 10 == 0:
                    print(f"  >> 진행중: {idx+1}/{len(station_list)} (확보: {len(results)}건)")
            
            # **중요** 성공 여부와 상관없이 무조건 대기 (안전제일)
            time.sleep(2.0) 
            
        self._save_json(results, "sk_congestion_all_1to4.json")

if __name__ == "__main__":
    loader = SKCongestionLoader()
    
    # 1. 역 리스트 가져오기
    stations = loader.get_all_station_codes()
    
    # 2. 모든 역 혼잡도 가져오기 (시간 소요됨)
    if stations:
        loader.collect_congestion_data(stations)