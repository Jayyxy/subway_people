import pandas as pd
import os
import logging
import re 

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class StationFeatureMapper:
    def __init__(self):
        # 1. 도메인 지식 기반 데이터셋 정의 (Line 1~4 Focus)
        # 유지보수를 위해 각 카테고리별 역 리스트를 Set으로 관리 (중복 허용 및 검색 속도 향상)
        self.features_db = {
            '터미널': {
                '서울역', '용산', '청량리', '영등포', '수서', '광명', # 기차/KTX
                '강변', '고속터미널', '남부터미널', '상봉' # 버스터미널
            }, 
            '환승역': {
                '서울역', '시청', '종로3가', '동대문', '동대문역사문화공원', '충무로', 
                '신도림', '사당', '교대', '강남', '고속터미널', '노원', '창동', 
                '이수', '삼각지', '동작', '금정', '오이도', '가산디지털단지'
            },
            '대학가': {
                '혜화', '성신여대입구', '한성대입구', '신촌', '이대', '홍대입구', 
                '건대입구', '동대입구','충무로', '숙대입구', '한양대', '고려대', '안암',  
                '회기', '외대앞', '서울대입구' ,'광운대','미아사거리'
            },
            '회사': {
                '종각', '광화문', '을지로입구', '을지로3가', '역삼', '선릉', '삼성', 
                '서초', '구로디지털단지', '양재', '남영', '용산', '여의도', '시청'
            },
            '학원가': {
                '노량진', '대치', '노원', '목동', '중계', '교대'
            },
            '중고등학교': {
                '한성대입구', '미아', '길음', '상계', '명일', '한티', '도곡', 
                '방배', '서초', '안국' # 학교 밀집 지역 혹은 셔틀버스 운행 지역
            },
            '핫플레이스': {
                '성수', '홍대입구', '이태원', '을지로3가', '종로3가', '신용산', 
                '삼각지', '혜화', '합정', '상수', '망원', '압구정', '신사'
            }
        }

        # 2. 특징 -> 타겟 코드 매핑 (다중 선택용)
        self.target_code_map = {
            '터미널': 'Traveler',    # [New] 여행객 (캐리어/짐)
            '환승역': 'Transfer',    # 환승객
            '대학가': 'UnivStudent', # 대학생
            '중고등학교': 'TeenStudent', # 중고생
            '학원가': 'TeenStudent',     # 중고생(수험생)
            '회사': 'Office',      # 직장인
            '핫플레이스': 'Hotplace'   # 놀러온 사람
        }
        
    def load_stations_from_history(self, csv_path="data/station_history.csv"):
        if not os.path.exists(csv_path):
            logging.error(f"❌ 데이터 파일이 없습니다: {csv_path}")
            return 

        try:
            df = pd.read_csv(csv_path)
            
            # 컬럼명 찾기
            target_col = None
            possible_cols = ['STTN']
            for col in possible_cols:
                if col in df.columns:
                    target_col = col
                    break
            
            if not target_col:
                return []

            unique_stations = df[target_col].dropna().unique().tolist()
            cleaned_stations = []
            for st in unique_stations:
                clean_name = re.sub(r'\([^)]*\)', '', str(st)).strip()
                cleaned_stations.append(clean_name)
            
            return list(set(cleaned_stations))
            
        except Exception as e:
            logging.error(f"로드 실패: {e}")
            return []

    def get_station_features(self, station_list):
        results = []
        
        for station in station_list:
            found_features = []
            target_set = set() # 중복 방지를 위해 Set 사용
            
            # 특징 태깅 및 타겟 매핑
            for feature_name, stations_set in self.features_db.items():
                if station in stations_set:
                    found_features.append(feature_name)
                    
                    # 타겟 코드 변환하여 추가
                    target_code = self.target_code_map.get(feature_name)
                    if target_code:
                        target_set.add(target_code)
            
            # [기본값 처리] 아무 특징도 없으면 'General'
            if not target_set:
                target_set.add('General')
            
            # [다중 타겟 포맷팅] set -> sorted list -> string join
            # 예: "Transfer|Traveler|Office"
            main_target_str = "|".join(sorted(list(target_set)))
            
            results.append({
                'station_name': station,
                'feature_list': ','.join(found_features) if found_features else '일반',
                'main_target': main_target_str
            })
            
        return pd.DataFrame(results)

    def generate_csv(self):
        target_stations = self.load_stations_from_history()
        
        # 만약 히스토리 파일에 터미널 역들이 없으면 강제로 추가 (테스트 확인용)
        force_include = ['서울역', '강변', '남부터미널', '고속터미널', '용산']
        for st in force_include:
            if st not in target_stations:
                target_stations.append(st)

        df = self.get_station_features(target_stations)
        
        os.makedirs("data", exist_ok=True)
        save_path = "data/station_feature.csv"
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        
        logging.info(f"✅ 역 특징 매핑 완료. 저장 경로: {save_path}")
        
        # 결과 샘플 확인 (다중 타겟이 잘 들어갔는지)
        print("\n[다중 타겟 매핑 결과]")
        # Traveler(터미널)가 포함된 역만 필터링해서 출력
        print(df[df['main_target'].str.contains('Traveler')].head(10))

if __name__ == "__main__":
    mapper = StationFeatureMapper()
    mapper.generate_csv()