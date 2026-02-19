"""
서울시 지하철 API 통합 클라이언트
- 실시간 도착 정보 (Real-time)
- 시간대별 승하차 인원 통계 (Statistics)
"""
import os
import requests
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()


class SeoulMetroAPI:
    """서울시 지하철 데이터 수집을 위한 통합 API 클라이언트"""

    # 실시간 도착 정보 API (swopenAPI)
    BASE_URL_REALTIME = "http://swopenAPI.seoul.go.kr/api/subway"
    
    # 서울시 열린데이터 광장 공통 URL (통계 데이터용)
    BASE_URL_STATS = "http://openapi.seoul.go.kr:8088"

    def __init__(self):
        # 1. 실시간 도착 정보용 API 키
        self.api_key = os.getenv("SEOUL_API_KEY")
        
        # 2. 통계 데이터용 API 키 (별도로 없으면 SEOUL_API_KEY 공용 사용)
        self.stat_api_key = os.getenv("STAT_API_KEY", self.api_key)

        if not self.api_key:
            raise ValueError("SEOUL_API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")

    def get_arrival_info(self, station_name: str) -> dict:
        """
        특정 역의 실시간 도착 정보를 조회합니다.
        """
        clean_name = station_name.replace("역", "")
        url = f"{self.BASE_URL_REALTIME}/{self.api_key}/json/realtimeStationArrival/0/10/{clean_name}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # [디버깅 코드 추가] 
            # API가 정확히 뭐라고 응답하는지 눈으로 확인해야 원인을 압니다!
            print(f"🔍 DEBUG [{station_name}] 응답 전체: {data}") 

            if "errorMessage" in data:

                code = data["errorMessage"].get("code")
                msg = data["errorMessage"].get("message")
                
                # 정상이 아니면 에러 내용을 출력
                if code != "INFO-000":
                    print(f"⚠️ [API 에러] {station_name}: {code} - {msg}")
                    return {}
                

            # 데이터가 아예 없는 경우 (realtimeArrivalList 키가 없음)
            if "realtimeArrivalList" not in data:
                print(f"⚠️ [데이터 없음] {station_name}: 서버 응답에 도착 정보가 없습니다.")
                return {}
            
            return data
        
        

        except requests.exceptions.RequestException as e:
            print(f"❌ [네트워크 오류] {station_name}: {e}")
            return {}

    def get_passenger_stats(self, date: str, start_index: int = 1, end_index: int = 100) -> dict:
        # 사용자가 제공한 URL에 따르면 서비스명은 CardSubwayTime 입니다.
        service_name = "CardSubwayTime" 
        
        # date 인자는 YYYYMM 형식이어야 함 (예: 202401)
        url = f"{self.BASE_URL_STATS}/{self.stat_api_key}/json/{service_name}/{start_index}/{end_index}/{date}"
        
        try:
            print(f"📡 통계 데이터 요청: {date} ({start_index}~{end_index})")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ [통계 수집 오류] {date}: {e}")
            return {}

    def get_multiple_stations(self, station_names: list) -> list:
        """
        여러 역의 실시간 도착 정보를 한 번에 조회합니다.
        """
        results = []
        for station in station_names:
            data = self.get_arrival_info(station)
            if data:
                results.append({
                    "station": station,
                    "data": data
                })
        return results


# 테스트 코드
if __name__ == "__main__":
    api = SeoulMetroAPI()

    # 1. 실시간 도착 정보 테스트
    print("\n=== 실시간 도착 정보 테스트 ===")
    arrival_data = api.get_arrival_info("서울역") # '역' 포함해도 처리됨
    if "realtimeArrivalList" in arrival_data:
        print(f"서울역 도착 정보 수신 완료: {len(arrival_data['realtimeArrivalList'])}건")
    else:
        print("도착 정보가 없거나 에러가 발생했습니다.")

    # 2. 통계 데이터 테스트 (어제 날짜 기준 등)
    print("\n=== 승하차 통계 데이터 테스트 ===")
    # 테스트용 날짜 (실제 유효한 과거 날짜여야 데이터가 나옴)
    test_date = "20231201" 
    stats_data = api.get_passenger_stats(test_date, 1, 5)
    
    if "CardSubwayTime" in stats_data:
        rows = stats_data["CardSubwayTime"]["row"]
        print(f"통계 데이터 수신 완료 ({test_date}): {len(rows)}건")
        if rows:
            print(f"예시: {rows[0]['SUB_STA_NM']} ({rows[0]['LINE_NUM']})")
    else:
        print("통계 데이터 응답이 없거나 서비스명이 다를 수 있습니다.")