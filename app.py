import streamlit as st
import pandas as pd
import sqlite3
import requests
import os

# 1. 페이지 및 브랜딩 설정
st.set_page_config(
    page_title="Project HEARTBEAT | 에버온",
    page_icon="💓",
    layout="wide"
)

# 2. OneDrive 직링크 설정 (팀장님이 주신 링크에 download=1 파라미터 강제 결합)
# 이 링크는 브라우저에 넣었을 때 바로 '다운로드'가 시작되어야 코드가 읽을 수 있습니다.
ONEDRIVE_URL = "https://1drv.ms/u/c/bf9f49139356d2bc/IQB_0lgA5yr9TJbCEDknO6fUAf4Kb26AQ0bO3UBjlLSwANw?e=iS1LOa"

LOCAL_DB_NAME = "temp_ev_management.db"

@st.cache_data(ttl=3600) # 1시간마다 OneDrive에서 새 파일을 받아옴 (필요시 시간 조절)
def refresh_database():
    try:
        with st.spinner('🔄 OneDrive에서 최신 Heartbeat 데이터를 동기화 중...'):
            response = requests.get(ONEDRIVE_URL)
            response.raise_for_status()
            with open(LOCAL_DB_NAME, "wb") as f:
                f.write(response.content)
        return True
    except Exception as e:
        st.error(f"❌ 데이터 동기화 실패: {e}")
        return False

# DB 연결 및 데이터 로딩 함수들
def get_connection():
    return sqlite3.connect(LOCAL_DB_NAME)

@st.cache_data(ttl=600)
def load_data(query):
    with get_connection() as conn:
        return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def get_station_list():
    with get_connection() as conn:
        df = pd.read_sql("SELECT DISTINCT station_name, address1 FROM charger_master WHERE station_name IS NOT NULL", conn)
    df['display_name'] = df['station_name'] + " [" + df['address1'].fillna('') + "]"
    return ["🔍 충전소를 검색/선택하세요..."] + df['display_name'].tolist()

def categorize_status(row):
    status = str(row['status'])
    error = str(row['error_state'])
    if '미수신' in status or '통신' in status: return '⚫ 미수신'
    elif error != '이상없음' or status in ['고장', '점검중']: return '🔴 점검중'
    elif '충전중' in status or '충전완료' in status: return '🔵 충전중'
    elif '대기' in status or '정상' in status: return '🟢 충전대기'
    else: return '⚪ 기타'

def color_status(val):
    if pd.isna(val): return ''
    colors = {
        '⚫ 미수신': 'background-color: #444444; color: white;',
        '🔴 점검중': 'background-color: #EF553B; color: white;',
        '🔵 충전중': 'background-color: #1F77B4; color: white;',
        '🟢 충전대기': 'background-color: #00CC96; color: black;'
    }
    return colors.get(val, 'color: gray;')

# ---------------------------------------------------------
# 메인 실행 로직
if refresh_database():
    st.title("💓 Project HEARTBEAT")
    st.caption("에버온 충전 네트워크 실시간 관제 시스템 (OneDrive Live Connect)")

    # 사이드바
    st.sidebar.header("📡 관제 설정")
    
    if 'station_select' not in st.session_state:
        st.session_state.station_select = "🔍 충전소를 검색/선택하세요..."

    def reset_search():
        st.session_state.station_select = "🔍 충전소를 검색/선택하세요..."

    all_stations = get_station_list()
    selected_option = st.sidebar.selectbox("충전소 검색/선택", all_stations, key="station_select")

    if st.sidebar.button("🔄 검색 초기화"):
        reset_search()
        st.rerun()

    st.sidebar.divider()
    time_view = st.sidebar.radio("로그 간격", ['3시간별 (최근 72시간)', '일간 (최근 14일)', '주간 (최근 12주)'])

    if selected_option == "🔍 충전소를 검색/선택하세요...":
        st.info("👈 왼쪽 사이드바에서 관제할 **충전소**를 선택해 주세요.")
    else:
        # 데이터 처리
        target_address = selected_option.split(" [")[-1].replace("]", "")
        
        current_df = load_data(f"""
            SELECT m.charger_id, m.station_name, h.status, h.error_state, h.collected_at
            FROM charger_master m
            JOIN status_history h ON m.charger_id = h.charger_id
            WHERE h.id IN (SELECT MAX(id) FROM status_history GROUP BY charger_id)
            AND m.address1 = '{target_address}'
        """)

        if not current_df.empty:
            current_df['상태분류'] = current_df.apply(categorize_status, axis=1)
            
            # 1. 요약 정보
            st.markdown(f"#### 📍 {selected_option.split(' [')[0]}")
            cols = st.columns(6)
            states = ['🟢 충전대기', '🔵 충전중', '🔴 점검중', '⚫ 미수신', '⚪ 기타']
            cols[0].metric("총 장비", f"{len(current_df):,}대")
            for i, s in enumerate(states):
                count = len(current_df[current_df['상태분류'] == s])
                cols[i+1].metric(s, f"{count:,}대")
            
            st.divider()

            # 2. 상세 리스트
            st.subheader("📋 실시간 상세 상태 (Heartbeat)")
            display_df = current_df[['charger_id', 'status', 'error_state', 'collected_at']].copy()
            display_df.columns = ['충전기 ID', '현재 상태', '에러 코드', '최종 수신 일시']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            st.divider()
            
            # 3. 타임라인 로그
            st.subheader(f"🎛️ {time_view} 상태 변화 이력")
            hist_df = load_data(f"""
                SELECT h.charger_id, h.status, h.error_state, h.collected_at
                FROM status_history h
                JOIN charger_master m ON h.charger_id = m.charger_id
                WHERE m.address1 = '{target_address}'
            """)
            
            if not hist_df.empty:
                hist_df['상태그룹'] = hist_df.apply(categorize_status, axis=1)
                hist_df['날짜'] = pd.to_datetime(hist_df['collected_at'], errors='coerce')
                hist_df = hist_df.dropna(subset=['날짜']).sort_values('날짜')
                
                pivot_df = hist_df.pivot_table(index='charger_id', columns='날짜', values='상태그룹', aggfunc='last')
                
                if '3시간별' in time_view: freq, fmt, limit = '3h', '%m-%d %H시', 24
                elif '일간' in time_view: freq, fmt, limit = 'D', '%Y-%m-%d', 14
                else: freq, fmt, limit = 'W-MON', '%Y-%m-%d(주)', 12
                    
                resampled_df = pivot_df.T.resample(freq).last().ffill().T
                resampled_df = resampled_df.fillna('⚪ 기타').iloc[:, -limit:]
                resampled_df.columns = [col.strftime(fmt) for col in resampled_df.columns]
                
                st.dataframe(resampled_df.style.map(color_status), use_container_width=True, height=400)
else:
    st.error("데이터베이스를 동기화할 수 없습니다. OneDrive 링크 설정을 확인해주세요.")
