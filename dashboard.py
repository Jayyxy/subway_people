import streamlit as st
import pandas as pd
import sqlite3
from analysis.seat_strategy import SeatStrategy

# 페이지 설정
st.set_page_config(page_title="Seat Hunter", layout="wide")

# 헤더 디자인
st.title("🚇 Seat Hunter: 앉아서 가는 지하철 전략")
st.markdown("당신의 목적지(혹은 환승역)를 입력하면, **누가 내리고 어디에 서야 하는지** 빅데이터가 알려드립니다.")

# 1. 역 리스트 로드 (DB에서)
conn = sqlite3.connect("database/subway.db")
try:
    df_stations = pd.read_sql("SELECT station_name FROM meta_station_feature ORDER BY station_name", conn)
    station_list = df_stations['station_name'].tolist()
except:
    station_list = []
conn.close()

# 2. 사용자 입력 UI
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("🔍 검색")
    if station_list:
        target_station = st.selectbox("어느 역에서 사람들이 내릴 것 같나요?", station_list)
        run_btn = st.button("전략 분석하기", type="primary")
    else:
        st.error("DB에 역 데이터가 없습니다. etl/loader.py를 실행했는지 확인하세요.")
        run_btn = False

with col2:
    if run_btn:
        brain = SeatStrategy()
        result = brain.analyze(target_station)
        
        if 'error' in result:
            st.error("분석 중 오류가 발생했습니다.")
        else:
            # --- 결과 리포트 화면 ---
            st.success("✅ 분석 완료! 데이터 엔지니어가 추천하는 전략입니다.")
            
            st.divider()
            st.markdown(f"## 🎯 **{target_station}역** 공략 리포트")
            
            # 메트릭 표시 (하차량, 점수)
            m1, m2, m3 = st.columns(3)
            m1.metric("예상 하차 규모(08시)", f"{result['off_count']:,}명")
            m2.metric("추천 탑승 위치", f"{result['best_car']}번 칸", "Best Choice")
            m3.metric("착석 성공 점수", f"{result['score']}점")
            
            # 타겟 정보
            st.markdown("### 🕵️ 이 역의 주요 등장인물 (Target)")
            # 태그 형태로 보여주기
            tags_html = ""
            for tag in result['targets']:
                color = "#ff4b4b" if tag == 'Traveler' else "#4caf50" # 여행객은 빨강, 나머진 초록
                tags_html += f"<span style='background-color:{color}; padding:5px 10px; border-radius:15px; color:white; margin-right:5px;'>{tag}</span>"
            st.markdown(tags_html, unsafe_allow_html=True)
            
            st.write("") # 여백
            
            # 상세 전략 메시지
            st.info("💡 **전문가 코멘트**")
            if result['strategy_msg']:
                for msg in result['strategy_msg']:
                    st.write(f"- {msg}")
            else:
                st.write("- 특별한 특징이 없는 일반적인 역입니다. 하차 인원이 많은 칸을 노리세요.")