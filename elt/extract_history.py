
#pip install requests pandas python-dotenv
import os
import requests
import pandas as pd
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta # 날짜 계산용 라이브러리
from dotenv import load_dotenv

# 1. 환경설정 로드
load_dotenv()
API_KEY = os.getenv("SEOUL_API_KEY")

# 2. 수집 설정 (최근 6개월)
# 예: 지금이 12월이면 6월~11월 데이터 수집
END_DATE = datetime.now() - relativedelta(months=1) # 지난달
START_DATE = END_DATE - relativedelta(months=5)     # 6개월 전

BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "CardSubwayTime" # 시간대별 승하차 인원

def get_monthly_subway_history(month_str):
    """
    특정 월(YYYYMM)의 1~4호선 시간대별 승하차 인원을 수집
    """
    # URL 포맷: /{KEY}/json/{SERVICE}/1/1000/{YYYYMM}
    url = f"{BASE_URL}/{API_KEY}/json/{SERVICE_NAME}/1/1000/{month_str}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if SERVICE_NAME not in data:
            # 에러 메시지 확인
            msg = data.get('RESULT', {}).get('MESSAGE', 'Unknown Error')
            # 'INFO-100'은 데이터 없음, 'INFO-000'은 정상
            if 'INFO-000' not in msg and 'INFO-100' not in msg:
                 print(f"⚠️ {month_str}: API 응답 확인 필요 - {msg}")
            return None
        
        rows = data[SERVICE_NAME]['row']
        
        # 데이터 프레임 변환
        df = pd.DataFrame(rows)
        
        # 1~4호선 필터링 (데이터 정제)
        target_lines = ['1호선', '2호선', '3호선', '4호선']
        df_filtered = df[df['SBWY_ROUT_LN_NM'].isin(target_lines)].copy()
        
        return df_filtered
        
    except Exception as e:
        print(f"⚠️ {month_str}: 연결 실패 - {e}")
        return None

def main():
    # 날짜 포맷 YYYYMM으로 변환
    start_str = START_DATE.strftime("%Y%m")
    end_str = END_DATE.strftime("%Y%m")
    
    print(f"🚀 월별 데이터 수집 시작: {start_str} ~ {end_str}")
    
    all_data = []
    current_date = START_DATE
    
    while current_date <= END_DATE:
        month_str = current_date.strftime("%Y%m")
        print(f"📡 수집 중: {month_str}...", end=" ")
        
        df = get_monthly_subway_history(month_str)
        
        if df is not None and not df.empty:
            df['USE_MM'] = month_str # 기준월 컬럼 추가
            all_data.append(df)
            print(f"✅ 완료 ({len(df)}개 역)")
        else:
            print("데이터 없음 (Pass)")
            
        # 다음 달로 이동
        current_date += relativedelta(months=1)
        time.sleep(0.5) 

    # 3. 데이터 병합 및 저장
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        os.makedirs("data", exist_ok=True)
        save_path = "data/station_history.csv" # 기존 파일 덮어쓰기
        
        final_df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 수집 완료! 총 {len(final_df)}행 저장됨.")
        print(f"📂 저장 경로: {save_path}")
        
        # 컬럼 확인 (08시, 09시 등 시간대 컬럼이 있는지 확인 중요)
        print("\n[데이터 샘플]")
        print(final_df[['USE_MM', 'SBWY_ROUT_LN_NM', 'STTN', 'HR_8_GET_OFF_NOPE']].head())
        
    else:
        print("\n❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()