import pandas as pd

# 📌 주요 지하철역 위경도 하드코딩 (필요한 역은 구글 맵에서 복사해서 추가 가능!)
STATION_COORDS = {
    '서울': (37.554648, 126.972559),
    '강남': (37.497942, 127.027621),
    '홍대입구': (37.556761, 126.923612),
    '신도림': (37.508725, 126.891295),
    '잠실': (37.513261, 127.100133),
    '시청': (37.563588, 126.977156),
    '종각': (37.570161, 126.982923),
    '종로3가': (37.570406, 126.991847),
    '여의도': (37.521574, 126.924340)
}

def add_coordinates_to_report():
    print("🗺️ 실시간 리포트에 위경도 데이터를 매핑합니다...")
    
    # 1. 아까 만든 실시간 혼잡도 결과 파일 읽기
    try:
        df = pd.read_csv('data/realtime_congestion_report.csv')
    except FileNotFoundError:
        print("🚨 realtime_congestion_report.csv 파일을 먼저 생성해주세요!")
        return

    # 2. 위도(Latitude), 경도(Longitude) 매핑 함수
    def get_lat(station):
        return STATION_COORDS.get(station, (None, None))[0]

    def get_lng(station):
        return STATION_COORDS.get(station, (None, None))[1]

    # 3. 데이터프레임에 새로운 컬럼 2개 추가
    df['위도(Latitude)'] = df['역명'].apply(get_lat)
    df['경도(Longitude)'] = df['역명'].apply(get_lng)

    # 4. 태블로용 최종 파일로 저장
    output_path = 'data/realtime_report_for_tableau.csv'
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"✅ 매핑 완료! 태블로용 파일이 저장되었습니다: {output_path}")
    print("-" * 60)
    # 데이터가 잘 들어갔는지 샘플 출력
    print(df[['역명', '현재시간_예상하차인원(명)', '위도(Latitude)', '경도(Longitude)']].head(5))

if __name__ == "__main__":
    add_coordinates_to_report()