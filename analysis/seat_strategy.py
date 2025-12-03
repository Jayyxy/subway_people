import sqlite3
import pandas as pd

class SeatStrategy:
    def __init__(self, db_path="database/subway.db"):
        self.db_path = db_path
        
        # [Rule Base] 타겟별 공략/회피 칸 규칙
        # 1-1 ~ 10-4 칸을 1~10번 칸으로 단순화하여 점수 부여
        self.target_rules = {
            'Traveler': { # 여행객 (짐 많음 -> 회전율 최악)
                'avoid': [1, 10], # 보통 끝칸에 짐 싣고 서있음
                'msg': '🧳 짐이 많은 여행객/터미널 이용객이 많습니다. 회전율이 낮으니 피하세요!'
            },
            'Transfer': { # 환승객 (우르르 내림 -> 기회!)
                'target': [1, 4, 7, 10], # 환승 계단이 많은 위치 (가정)
                'msg': '🏃 환승객이 대거 하차하는 역입니다. 환승 계단 근처 칸을 노리세요!'
            },
            'UnivStudent': { # 대학생
                'target': [3, 5, 8], 
                'msg': '🎓 대학생들이 수업 들으러 많이 내립니다. 책가방 멘 학생 앞을 노리세요.'
            },
            'TeenStudent': { # 중고생/학원
                'target': [2, 9], 
                'msg': '🏫 학원/학교 가는 학생들이 내립니다.'
            },
            'Office': { # 직장인
                'target': [2, 3, 8, 9], 
                'msg': '💼 직장인들이 출근하러 내립니다. 문 근처에 서보세요.'
            },
            'Hotplace': { # 핫플
                'target': [5, 6], 
                'msg': '✨ 약속 장소로 가는 사람들이 내립니다.'
            }
        }

    def get_station_data(self, station_name):
        conn = sqlite3.connect(self.db_path)
        
        # 1. 특징 정보 조회
        query_meta = "SELECT * FROM meta_station_feature WHERE station_name = ?"
        df_meta = pd.read_sql(query_meta, conn, params=(station_name,))
        
        # 2. 하차 인원 조회 (혼잡도 가중치용)
        query_raw = "SELECT time_08_09_off FROM raw_station_history WHERE station_name = ?"
        df_raw = pd.read_sql(query_raw, conn, params=(station_name,))
        
        conn.close()
        
        if df_meta.empty:
            return None, 0
            
        # 하차량 정보가 없으면 0 처리
        off_count = df_raw.iloc[0]['time_08_09_off'] if not df_raw.empty else 0
        
        return df_meta.iloc[0], off_count

    def analyze(self, station_name):
        info, off_count = self.get_station_data(station_name)
        
        if info is None:
            return {'error': '데이터가 없는 역입니다.'}

        # "Transfer|Office" -> ['Transfer', 'Office'] 로 분리
        main_targets = info['main_target'].split('|')
        
        # 1. 기본 점수판 (1~10번 칸, 50점에서 시작)
        car_scores = {i: 50 for i in range(1, 11)}
        strategies = []

        # 2. 타겟별 점수 계산 (Rule Engine)
        for target in main_targets:
            rule = self.target_rules.get(target)
            if not rule:
                continue
                
            # 공략(Target) 칸 점수 증가
            if 'target' in rule:
                for car in rule['target']:
                    car_scores[car] += 15
                strategies.append(rule['msg'])
                
            # 회피(Avoid) 칸 점수 감소
            if 'avoid' in rule:
                for car in rule['avoid']:
                    car_scores[car] -= 20
                strategies.append(rule['msg'])

        # 3. 하차 인원(Volume) 가중치 반영
        # 하차 인원이 많을수록 "기회"가 많으므로 전체 점수 상향
        volume_bonus = 0
        if off_count > 3000: volume_bonus = 20
        elif off_count > 1000: volume_bonus = 10
        
        for car in car_scores:
            car_scores[car] += volume_bonus

        # 4. 최고 점수 칸 선정
        best_car = max(car_scores, key=car_scores.get)
        
        return {
            'station': station_name,
            'features': info['feature_list'],
            'targets': main_targets,
            'best_car': best_car,
            'score': car_scores[best_car],
            'strategy_msg': list(set(strategies)), # 중복 메시지 제거
            'off_count': off_count
        }