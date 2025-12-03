import requests
import pandas as pd
import os
import sqlite3
import logging
import time
from dotenv import load_dotenv

# 1. 환경변수 및 로깅 설정
load_dotenv() # .env 파일 로드
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class KakaoLandmarkMapper:
    def __init__(self):
        # .env에서 MAP_API_KEY 가져오기
        self.api_key = os.getenv("MAP_API_KEY")
        
        if not self.api_key:
            raise ValueError("❌ .env 파일에 'MAP_API_KEY'가 없습니다.")
            
        self.headers = {"Authorization": f"KakaoAK {self.api_key}"}
        self.base_url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        self.db_path = "database/subway.db"
        
        # 검색 우선순위 및 키워드 정의 (우선순위가 높은 순서대로 적용)
        self.search_priorities = [
            {'type': 'UnivStudent', 'keyword': '대학교', 'code': 'SC4'}, # 학교
            {'type': 'Transfer', 'keyword': '환승센터', 'code': ''},     # 교통
            {'type': 'Senior', 'keyword': '종합병원', 'code': 'HP8'},    # 병원
            {'type': 'Senior', 'keyword': '전통시장', 'code': ''},       # 시장
            {'type': 'Office', 'keyword': '구청', 'code': 'PO3'},        # 관공서
            {'type': 'Office', 'keyword': '산업단지', 'code': ''},       # 오피스
            {'type': 'Hotplace', 'keyword': '먹자골목', 'code': 'FD6'}   # 음식점 밀집
        ]

    def get_stations_from_db(self):
        """
        [핵심] 실제 DB에 적재된 역 리스트를 조회 (가정 X)
        """
        if not os.path.exists(self.db_path):
            logging.error(f"❌ DB 파일이 없습니다: {self.db_path}")
            return []

        conn = sqlite3.connect(self.db_path)
        try:
            # raw_station_history 테이블에서 중복 없이 역 이름 추출
            # (만약 raw 테이블이 비어있으면 loader.py를 먼저 실행해야 함)
            query = "SELECT DISTINCT station_name FROM raw_station_history"
            df = pd.read_sql(query, conn)
            
            stations = df['station_name'].tolist()
            logging.info(f"📂 DB에서 총 {len(stations)}개의 역 정보를 가져왔습니다.")
            return stations
            
        except Exception as e:
            logging.error(f"DB 조회 실패: {e}")
            return []
        finally:
            conn.close()

    def search_station_landmark(self, station_name):
        """
        특정 역 주변 500m 반경의 시설물 검색
        """
        best_target = 'General'
        found_feats = []

        # 역 이름 보정 (ex: '서울' -> '서울역', '강남' -> '강남역')
        search_query_base = station_name if station_name.endswith('역') else f"{station_name}역"

        for item in self.search_priorities:
            # 검색어: "강남역 대학교", "종로3가역 종합병원" 등
            query = f"{search_query_base} {item['keyword']}"
            params = {
                'query': query, 
                'category_group_code': item['code'], 
                'radius': 500, # 반경 500m 내
                'size': 1      # 1개만 있어도 해당 특징 보유로 인정
            }
            
            try:
                response = requests.get(self.base_url, headers=self.headers, params=params)
                
                if response.status_code == 200:
                    documents = response.json().get('documents')
                    if documents:
                        # 특징 발견
                        feat_name = item['keyword']
                        found_feats.append(feat_name)
                        
                        # 가장 우선순위 높은 타겟 설정 (아직 General일 경우에만)
                        if best_target == 'General':
                            best_target = item['type']
                            
            except Exception as e:
                logging.error(f"API 요청 중 에러: {e}")
            
            time.sleep(0.05) # API 부하 방지 (초당 20회 제한 고려)

        # 아무 특징도 없으면 기본값
        return best_target, "|".join(found_feats)

    def run(self):
        # 1. DB에서 역 목록 가져오기
        stations = self.get_stations_from_db()
        
        if not stations:
            logging.warning("분석할 역이 없습니다. etl/loader.py를 먼저 실행하세요.")
            return

        results = []
        total = len(stations)
        
        logging.info(f"🚀 카카오 API 기반 랜드마크 분석 시작 (총 {total}개 역)")
        
        for idx, station in enumerate(stations):
            target, features = self.search_station_landmark(station)
            
            results.append({
                'station_name': station,
                'main_target': target,
                'feature_list_kakao': features
            })
            
            # 진행상황 로깅 (10개 단위)
            if (idx + 1) % 10 == 0:
                print(f"[{idx + 1}/{total}] 처리 중... ({station}: {target})")

        # 결과 DataFrame 생성
        df_result = pd.DataFrame(results)
        
        # 2. 결과 저장 (CSV) -> 추후 DB 적재용
        os.makedirs("data", exist_ok=True)
        save_path = "data/station_kakao_feature.csv"
        df_result.to_csv(save_path, index=False, encoding="utf-8-sig")
        
        logging.info(f"✅ 분석 완료. 결과 저장됨: {save_path}")
        print(df_result.head())

if __name__ == "__main__":
    try:
        mapper = KakaoLandmarkMapper()
        mapper.run()
    except Exception as e:
        logging.error(f"실행 오류: {e}")