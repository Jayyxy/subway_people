import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class DataLoader:
    def __init__(self, db_path="database/subway.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def create_tables(self):
        # 1. 승하차 히스토리 (Raw Data)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_station_history (
                use_mon TEXT,
                line_num TEXT,
                station_name TEXT,
                time_08_09_off INTEGER,
                time_18_19_on INTEGER
            );
        """)
        
        # 2. 역 특징 매핑 정보 (Meta Data)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta_station_feature (
                station_name TEXT,
                feature_list TEXT,
                main_target TEXT,
                PRIMARY KEY (station_name)
            );
        """)
        self.conn.commit()
        logging.info("테이블 스키마 준비 완료")

    def load_data(self):
        # A. 히스토리 적재 (API 출력명 -> DB 컬럼명 매핑 수정)
        try:
            hist_path = "data/station_history.csv"
            if os.path.exists(hist_path):
                df_hist = pd.read_csv(hist_path)
                
                # [수정됨] 이미지 기반 컬럼 매핑 정의
                # Key: CSV(API) 컬럼명, Value: DB 컬럼명
                col_map = {
                    'USE_MM': 'use_mon',              # 사용월
                    'SBWY_ROUT_LN_NM': 'line_num',    # 호선명
                    'STTN': 'station_name',           # 지하철역
                    
                    # 분석에 필요한 핵심 시간대 (이미지 패턴 기반)
                    'HR_8_GET_OFF_NOPE': 'time_08_09_off', # 08-09시 하차 (등교/출근)
                    'HR_18_GET_ON_NOPE': 'time_18_19_on'   # 18-19시 승차 (퇴근/하교)
                }
                
                # 데이터프레임에 해당 컬럼들이 실제로 있는지 확인 후 선택
                # (API 응답에 따라 모든 시간대 컬럼이 다 있을 것이므로 필요한 것만 뽑음)
                avail_cols = [c for c in col_map.keys() if c in df_hist.columns]
                
                if not avail_cols:
                    logging.warning(f"매핑할 컬럼이 CSV에 없습니다. 현재 컬럼: {df_hist.columns.tolist()}")
                else:
                    # 컬럼 선택 및 이름 변경
                    df_selected = df_hist[avail_cols].rename(columns=col_map)
                    
                    # DB 적재
                    df_selected.to_sql('raw_station_history', self.conn, if_exists='replace', index=False)
                    logging.info(f"히스토리 데이터 적재 완료: {len(df_selected)}행")
                    logging.info(f"사용된 컬럼: {list(col_map.keys())}")
                    
            else:
                logging.error(f"파일 없음: {hist_path}")

        except Exception as e:
            logging.error(f"히스토리 적재 실패: {e}")

        # B. 특징 매핑 적재 (station_feature.csv) - 기존 유지
        try:
            feat_path = "data/station_feature.csv"
            if os.path.exists(feat_path):
                df_feat = pd.read_csv(feat_path)
                df_feat.to_sql('meta_station_feature', self.conn, if_exists='replace', index=False)
                logging.info(f"특징 매핑 데이터 적재 완료: {len(df_feat)}행")
            else:
                logging.warning("특징 매핑 파일이 없습니다.")
        except Exception as e:
            logging.error(f"특징 데이터 적재 실패: {e}")

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    loader = DataLoader()
    loader.create_tables()
    loader.load_data()
    loader.close()