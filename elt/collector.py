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
        # 타겟 호선 (서울시 API 표기 기준)
        self.target_lines = ["1호선", "2호선", "3호선", "4호선"]

    def _save_json(self, data, filename):
        filepath = os.path.join(self.base_path, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"💾 [저장 완료] {filename}")

    def collect_seoul_ridership(self, month="202409"):
        """
        서울시 시간대별 승하차 인원 (1~4호선 필터링하여 수집)
        """
        print(f"\n--- [1] 서울시 승하차 데이터 수집 ({month}) ---")
        
        start_idx = 1
        end_idx = 1000
        all_data = []
        
        while True:
            url = f"http://openapi.seoul.go.kr:8088/{self.seoul_key}/json/CardSubwayTime/{start_idx}/{end_idx}/{month}"
            
            try:
                resp = requests.get(url)
                data = resp.json()
                
                # 데이터 유무 확인
                if 'CardSubwayTime' not in data:
                    break
                
                rows = data['CardSubwayTime']['row']
                if not rows:
                    break
                
                # 1~4호선만 필터링해서 담기
                for row in rows:
                    line_name = row.get('SBWY_ROUT_LN_NM', '') # 호선명
                    if line_name in self.target_lines:
                        all_data.append(row)
                
                print(f" >> {start_idx}~{end_idx} 구간 조회 완료 (현재 누적: {len(all_data)}개)")
                
                # 다음 페이지
                start_idx += 1000
                end_idx += 1000
                
            except Exception as e:
                print(f"[ERROR] 서울시 API 수집 중 오류: {e}")
                break
        
        if all_data:
            self._save_json(all_data, f"seoul_ridership_{month}_1to4.json")
            return all_data # 카카오 수집을 위해 데이터 반환
        else:
            print("[WARN] 수집된 데이터가 없습니다.")
            return []

    def collect_kakao_poi(self, station_data):
        """
        수집된 서울시 데이터에 있는 역 이름을 기준으로 카카오 POI 수집
        """
        print(f"\n--- [2] 카카오 역 주변 시설 정보 수집 ---")
        
        # 역 이름 중복 제거 (서울역 1호선, 서울역 4호선 등 중복 방지)
        unique_stations = set()
        for row in station_data:
            unique_stations.add(row['STTN']) # 역명
            
        station_list = sorted(list(unique_stations))
        print(f" >> 총 {len(station_list)}개 역에 대해 조회 시작")
        
        headers = {"Authorization": f"KakaoAK {self.kakao_key}"}
        poi_results = []
        
        for idx, name in enumerate(station_list):
            try:
                # 1. 좌표 검색
                k_url = "https://dapi.kakao.com/v2/local/search/keyword.json"
                # 검색 정확도를 위해 '역' 글자가 없으면 붙임 + '지하철' 키워드 추가
                search_query = name if name.endswith('역') else name + '역'
                
                resp = requests.get(k_url, headers=headers, params={"query": search_query})
                k_data = resp.json()
                
                if k_data['documents']:
                    target = k_data['documents'][0]
                    x, y = target['x'], target['y']
                    
                    info = {"station_name": name, "x": x, "y": y, "poi": {}}
                    
                    # 2. 카테고리별 개수 (학교SC4, 공공기관PO3, 주거/편의시설OL7)
                    c_url = "https://dapi.kakao.com/v2/local/search/category.json"
                    for cat in ['SC4', 'PO3', 'OL7']:
                        c_resp = requests.get(c_url, headers=headers, params={
                            "category_group_code": cat, "x": x, "y": y, "radius": 800
                        })
                        cnt = c_resp.json().get('meta', {}).get('total_count', 0)
                        info['poi'][cat] = cnt
                    
                    poi_results.append(info)
                    
                # 카카오 API 제한 고려 (너무 빠르면 누락됨)
                if idx % 10 == 0:
                    print(f"  진행중: {idx}/{len(station_list)}")
                time.sleep(0.3) 
                
            except Exception as e:
                print(f"[SKIP] {name} 조회 실패: {e}")
        
        self._save_json(poi_results, "kakao_station_poi.json")

if __name__ == "__main__":
    collector = BaseCollector()
    
    # 1. 서울시 데이터 수집 (2024년 9월)
    seoul_data = collector.collect_seoul_ridership("202409")
    
    # 2. 카카오 데이터 수집 (서울시 데이터가 있을 때만)
    if seoul_data:
        collector.collect_kakao_poi(seoul_data)