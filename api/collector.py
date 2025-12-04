import os
import requests
import json
import time
from dotenv import load_dotenv

load_dotenv()

class BaseCollector:
    def __init__(self):
        self.seoul_key = os.getenv("SEOUL_API_KEY")
        self.kakao_key = os.getenv("KAKAO_API_KEY")
        self.base_path = "./data/raw"
        os.makedirs(self.base_path, exist_ok=True)
        self.target_lines = ["1호선", "2호선", "3호선", "4호선"]

        # [분석용] 카카오 카테고리 전체 정의
        self.kakao_categories = {
            "MT1": "mart",          # 대형마트
            "CS2": "store",         # 편의점
            "PS3": "kindergarten",  # 어린이집, 유치원
            "SC4": "school",        # 학교
            "AC5": "academy",       # 학원
            "PK6": "parking",       # 주차장
            "OL7": "gas_station",   # 주유소
            "SW8": "subway",        # 지하철역
            "BK9": "bank",          # 은행
            "CT1": "culture",       # 문화시설
            "AG2": "agency",        # 중개업소
            "PO3": "public",        # 공공기관
            "AT4": "attraction",    # 관광명소
            "AD5": "accommodation", # 숙박
            "FD6": "restaurant",    # 음식점
            "CE7": "cafe",          # 카페
            "HP8": "hospital",      # 병원
            "PM9": "pharmacy"       # 약국
        }

    def _save_json(self, data, filename):
        filepath = os.path.join(self.base_path, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 [저장 완료] {filename} (건수: {len(data)})")

    def collect_seoul_ridership_1year(self):
        """
        [수정됨] 2024.11 ~ 2025.10 (1년치) 데이터 수집
        """
        print(f"\n--- [1] 서울시 승하차 데이터 수집 (1년치: 2024.11 ~ 2025.10) ---")
        
        # 수집할 월 리스트 생성
        months = []
        # 2024년 11월 ~ 12월
        for m in range(11, 13): months.append(f"2024{m:02d}")
        # 2025년 1월 ~ 10월
        for m in range(1, 11): months.append(f"2025{m:02d}")
        
        total_data = []

        for month in months:
            print(f" >> [진행중] {month} 데이터 수집 시작...")
            start_idx = 1
            end_idx = 1000
            month_data_count = 0
            
            while True:
                url = f"http://openapi.seoul.go.kr:8088/{self.seoul_key}/json/CardSubwayTime/{start_idx}/{end_idx}/{month}"
                
                try:
                    resp = requests.get(url)
                    data = resp.json()
                    
                    # 데이터 존재 여부 체크
                    if 'CardSubwayTime' not in data:
                        # 에러 메시지 확인 (데이터 없음 등)
                        if 'RESULT' in data and data['RESULT']['CODE'] != 'INFO-000':
                            # 데이터가 없는 달일 수 있음 (미래 날짜 등)
                            pass 
                        break
                    
                    rows = data['CardSubwayTime']['row']
                    if not rows:
                        break
                    
                    # 1~4호선 필터링하여 저장
                    for row in rows:
                        line = row.get('SBWY_ROUT_LN_NM') # 혹은 API 키값에 따라 HO_SEON 등
                        if line in self.target_lines:
                            total_data.append(row)
                            month_data_count += 1
                    
                    # 다음 페이지 (1000건 단위)
                    if len(rows) < 1000: # 마지막 페이지
                        break
                    start_idx += 1000
                    end_idx += 1000
                    
                except Exception as e:
                    print(f"  [ERROR] {month} 처리 중 에러: {e}")
                    break
            
            print(f"    ㄴ {month} 완료 ({month_data_count}건 확보)")
        
        # 최종 저장
        if total_data:
            filename = "seoul_ridership_202411_202510_1to4.json"
            self._save_json(total_data, filename)
            return total_data
        else:
            print("[WARN] 수집된 데이터가 없습니다.")
            return []

    def collect_kakao_poi_all_categories(self, station_data):
        """
        [유지] 서울시 데이터 기반 역 주변 시설 상세 수집
        """
        print(f"\n--- [2] 카카오 역 주변 모든 시설 정보 수집 (Smart Search) ---")
        
        # 1. 역별 호선 정보 매핑 (Station -> Lines)
        station_line_map = {}
        
        for row in station_data:
            line = row.get('SBWY_ROUT_LN_NM')
            name = row.get('STTN')
            
            if line in self.target_lines and name:
                if name not in station_line_map:
                    station_line_map[name] = set()
                station_line_map[name].add(line)
        
        station_list = sorted(list(station_line_map.keys()))
        print(f" >> 총 {len(station_list)}개 역 분석 시작")
        
        headers = {"Authorization": f"KakaoAK {self.kakao_key}"}
        poi_results = []
        
        k_keyword_url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        k_category_url = "https://dapi.kakao.com/v2/local/search/category.json"

        for idx, name in enumerate(station_list):
            try:
                lines_str = ", ".join(sorted(list(station_line_map[name])))
                
                # 검색 로직
                search_query = name if name.endswith('역') else name + '역'
                
                def fetch_coordinates(query):
                    params = {"query": query, "category_group_code": "SW8"}
                    return requests.get(k_keyword_url, headers=headers, params=params).json()

                k_data = fetch_coordinates(search_query)
                
                # Fallback: 괄호 제거 후 재시도
                if not k_data.get('documents') and '(' in name:
                    clean_name = name.split('(')[0].strip()
                    retry_query = clean_name if clean_name.endswith('역') else clean_name + '역'
                    print(f"  [RETRY] '{search_query}' 실패 -> '{retry_query}'로 재시도")
                    k_data = fetch_coordinates(retry_query)
                    if k_data.get('documents'):
                        search_query = retry_query

                if k_data.get('documents'):
                    target = k_data['documents'][0]
                    x, y = target['x'], target['y']
                    
                    station_info = {
                        "station_name": name,
                        "search_name": search_query,
                        "line_filters": lines_str,
                        "x": x, "y": y,
                        "poi_counts": {} 
                    }
                    
                    for code, alias in self.kakao_categories.items():
                        c_params = {"category_group_code": code, "x": x, "y": y, "radius": 500}
                        c_resp = requests.get(k_category_url, headers=headers, params=c_params)
                        count = c_resp.json().get('meta', {}).get('total_count', 0)
                        station_info['poi_counts'][alias] = count
                        time.sleep(0.05) 
                    
                    poi_results.append(station_info)
                    print(f"  [OK] {name} 완료 ({idx+1}/{len(station_list)})")
                else:
                    print(f"  [SKIP] {name} 좌표 검색 실패")

                time.sleep(0.2)
                
            except Exception as e:
                print(f"  [ERROR] {name} 처리 중 에러: {e}")

        self._save_json(poi_results, "kakao_station_poi_1to4_full.json")

if __name__ == "__main__":
    collector = BaseCollector()
    
    # 1. 서울시 1년치 데이터 수집
    seoul_data = collector.collect_seoul_ridership_1year()
    
    # 2. 카카오 정밀 수집 (서울시 데이터가 존재할 때만 실행)
    if seoul_data:
        # 데이터가 많아도 역 목록(Key)은 동일하므로 중복 없이 역 정보만 추출해서 실행됨
        collector.collect_kakao_poi_all_categories(seoul_data)