import streamlit as st
import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# -----------------------------------------------------------------------------
# 0. 설정 및 API 키
# -----------------------------------------------------------------------------
st.set_page_config(page_title="지하철 앉아가기 프로젝트 (Final)", page_icon="🚇", layout="wide")

SK_API_KEY = os.getenv("SK_API_KEY")
if not SK_API_KEY:
    SK_API_KEY = st.sidebar.text_input("SK API Key", type="password")

# -----------------------------------------------------------------------------
# 1. 데이터 로드 (분석 데이터 + SK 역 코드 메타데이터)
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("./data/processed/subway_final_data.csv")
    except:
        return pd.DataFrame(), {}

    sk_meta_path = "./data/raw/sk_meta_stations.json"
    
    code_map = {}
    if os.path.exists(sk_meta_path):
        with open(sk_meta_path, 'r', encoding='utf-8') as f:
            meta_list = json.load(f)
            
        for item in meta_list:
            name = item.get('stationName')
            code = item.get('stationCode')
            line = item.get('subwayLine')
            
            if line in ['1호선', '2호선', '3호선', '4호선']:
                code_map[name] = code
                if not name.endswith('역'):
                    code_map[name + '역'] = code
    else:
        # 파일이 없을 경우 Fallback
        code_map = {
            "서울역": "133", "시청": "132", "종각": "131", "강남": "222", 
            "홍대입구": "239", "신도림": "234", "사당": "226", "건대입구": "212"
        }
    
    return df, code_map

df, STATION_CODE_MAP = load_data()

if df.empty:
    st.error("❌ 분석 데이터가 없습니다. STEP 1 (CSV 저장)을 먼저 실행해주세요.")
    st.stop()

# -----------------------------------------------------------------------------
# 2. SK API 연동 (실시간 혼잡도)
# -----------------------------------------------------------------------------
def get_realtime_congestion(station_name, line, hour):
    station_code = STATION_CODE_MAP.get(station_name)
    if not station_code and station_name.endswith('역'):
        station_code = STATION_CODE_MAP.get(station_name[:-1])
        
    if not station_code or not SK_API_KEY:
        return None

    url = f"https://apis.openapi.sk.com/puzzle/subway/congestion/stat/car/stations/{station_code}"
    headers = {"accept": "application/json", "appKey": SK_API_KEY, "Content-Type": "application/json"}
    params = {"subwayLine": line, "dow": "WED", "hh": hour}
    
    try:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            if 'contents' in data and 'stat' in data['contents']:
                return data['contents']['stat'][0]['data'][0]['congestionCar']
    except:
        pass
    return None

# -----------------------------------------------------------------------------
# 3. 시각화 Helper (HTML 생성)
# -----------------------------------------------------------------------------
def draw_train_cars(congestion_list):
    if not congestion_list or sum(congestion_list) == 0:
        return "<div style='color:#aaa; font-size:0.8em;'>📡 실시간 정보 없음</div>"

    # HTML 들여쓰기 문제 해결을 위해 한 줄로 작성하거나 들여쓰기 제거
    html_parts = ["<div style='display:flex; gap:3px; align-items:center;'>"]
    for idx, val in enumerate(congestion_list):
        if val < 34: color, label = "#00D26A", "여유"
        elif val < 70: color, label = "#FFC400", "보통"
        else: color, label = "#F63C3C", "혼잡"
        
        div = f"""<div style="width: 28px; height: 40px; background-color: {color}; border-radius: 4px; display: flex; flex-direction: column; justify-content: center; align-items: center; font-size: 10px; color: white; font-weight: bold; box-shadow: 1px 1px 3px rgba(0,0,0,0.2);" title="{idx+1}호차: {val}% ({label})"><span>{idx+1}</span><span style="font-size:8px; opacity:0.8;">{val}</span></div>"""
        html_parts.append(div)
    html_parts.append("</div>")
    return "".join(html_parts)

# -----------------------------------------------------------------------------
# 4. 메인 UI
# -----------------------------------------------------------------------------
st.sidebar.title("🚇 지하철 착석 예측 Pro")
st.sidebar.markdown("---")

lines = sorted(df['SBWY_ROUT_LN_NM'].dropna().unique())
selected_line = st.sidebar.selectbox("호선", lines)
line_stations = df[df['SBWY_ROUT_LN_NM'] == selected_line]['지하철역명'].unique()
start_station = st.sidebar.selectbox("출발역", line_stations)
end_station = st.sidebar.selectbox("도착역", line_stations, index=min(len(line_stations)-1, 1))
selected_time = st.sidebar.select_slider("시간대 (평일 기준)", options=[f"{h:02d}" for h in range(5, 24)], value="08")

def get_path(start, end, stations):
    try:
        lst = list(stations)
        s, e = lst.index(start), lst.index(end)
        return lst[s:e+1] if s < e else lst[e:s+1][::-1]
    except: return [start, end]

path = get_path(start_station, end_station, line_stations)

def get_recommendation(infra, cong_list):
    if cong_list and sum(cong_list) > 0:
        best_idx = cong_list.index(min(cong_list)) + 1
        return f"📊 데이터 추천: <b>{best_idx}호차</b> (혼잡도 {min(cong_list)}%)"
    if "환승" in str(infra): return "팁: 🔀 환승 통로 근처 (중앙)"
    if "오피스" in str(infra): return "팁: 🏃‍♂️ 빠른 하차 (양 끝)"
    if "대학" in str(infra) or "상권" in str(infra): return "팁: 🛍️ 에스컬레이터 근처"
    return "팁: 🍀 운에 맡기세요 (랜덤)"

st.header(f"{selected_line} {start_station} ➡ {end_station}")
st.caption(f"기준 시간: {selected_time}시 | 경로: {len(path)}개 역 이동")
st.markdown("---")

for station in path:
    row = df[df['지하철역명'] == station].iloc[0]
    board = row.get(f"{selected_time}시_승차", 0)
    alight = row.get(f"{selected_time}시_하차", 0)
    infra = row.get('통계적_지역특성', '일반')
    landmark = row.get('랜드마크', '')
    if pd.isna(landmark): landmark = ""
    
    net_flow = board - alight
    status_color = "#FFFFFF"
    msg = ""
    border_style = "border: 1px solid #eee;"
    
    if net_flow > 1500: 
        status_color = "#fff0f0" # 승차 많음 배경
        border_style = "border-left: 5px solid #FF4B4B;"
    elif net_flow < -1500: 
        status_color = "#f0f8ff" # 하차 많음 배경
        border_style = "border-left: 5px solid #1E90FF;"
        msg = f"✨ <b>{int(alight):,}명</b> 하차! 자리가 날 확률이 높습니다."

    cong_data = get_realtime_congestion(station, selected_line, selected_time)
    rec_text = get_recommendation(infra, cong_data)

    # [수정됨] HTML 들여쓰기를 완전히 제거하여 Markdown 파싱 오류 방지
    html_content = f"""
<div style="background-color: {status_color}; padding: 15px; border-radius: 10px; margin-bottom: 10px; {border_style} box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
    <div style="display:flex; justify-content:space-between; align-items:center;">
        <div>
            <h3 style="margin:0;">{station}</h3>
            <div style="color:#666; font-size:0.9em;">
                {infra} <span style="color:#888;">{f'({landmark})' if landmark else ''}</span>
            </div>
        </div>
        <div style="text-align:right;">
            <div style="font-weight:bold; font-size:1.2em;">{int(board):,} ▲ / {int(alight):,} ▼</div>
        </div>
    </div>
    <div style="margin-top:10px;">
        {draw_train_cars(cong_data)}
    </div>
    <div style="margin-top:8px; font-size:0.9em; color:#333;">
        {msg if msg else ""} <br>
        <span style="background-color:white; padding:2px 6px; border-radius:4px; border:1px solid #ccc;">
            {rec_text}
        </span>
    </div>
</div>
"""
    st.markdown(html_content, unsafe_allow_html=True)