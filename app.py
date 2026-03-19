import streamlit as st
import pandas as pd
import sqlite3
import os

# 1. 페이지 설정 및 브랜딩
st.set_page_config(
    page_title="Project HEARTBEAT | 에버온",
    page_icon="💓",
    layout="wide"
)

# 2. DB 경로 설정 (GitHub 리포지토리에 함께 올린 파일명)
DB_PATH = 'ev_management.db'

# DB 파일 존재 여부 체크
if not os.path.exists(DB_PATH):
    st.error(f"⚠️ '{DB_PATH}' 파일을 찾을 수 없습니다.")
    st.info("💡 해결 방법: GitHub 저장소에 'ev_management.db' 파일을 업로드했는지 확인해주세요.")
    st.stop()

# 데이터 로딩 함수 (캐시 적용으로 성능 최적화)
@st.cache_data(ttl=600)
def load_data(query):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def get_station_list():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT DISTINCT station_name, address1 FROM charger_master WHERE station_name IS NOT NULL", conn)
    # 검색 편의를 위해 '충전소명 [주소]' 형태로 가공
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
# UI 메인 레이아웃
st.title("💓 Project HEARTBEAT")
st.caption("에버온 충전 네트워크 실시간 관제 시스템")

# 사이드바 설정
st.sidebar.header("📡 관제 타겟 설정")

if 'station_select' not in st.session_state:
    st.session_state.station_select = "🔍 충전소를 검색/선택하세요..."

def reset_search():
    st.session_state.station_select = "🔍 충전소를 검색/선택하세요..."

all_stations = get_station_list()
selected_option = st.sidebar.selectbox("충전소 검색", all_stations, key="station_select")

if st.sidebar.button("🔄 검색 초기화"):
    reset_search()
    st.rerun()

st.sidebar.divider()
time_view = st.sidebar.radio("로그 모니터링 간격", ['3시간별 (최근 72시간)', '일간 (최근 14일)', '주간 (최근 12주)'])

# ---------------------------------------------------------
if selected_option == "🔍 충전소를 검색/선택하세요...":
    st.info("👈 왼쪽 사이드바에서 관제할 **충전소를 선택**하시면 분석 리포트가 생성됩니다.")
else:
    # 선택된 주소 추출
    target_address = selected_option.split(" [")[-1].replace("]", "")
    
    # 1. 최신 상태 요약 데이터 로드
    current_df = load_data(f"""
        SELECT m.charger_id, m.station_name, h.status, h.error_state, h.collected_at
        FROM charger_master m
        JOIN status_history h ON m.charger_id = h.charger_id
        WHERE h.id IN (SELECT MAX(id) FROM status_history GROUP BY charger_id)
        AND m.address1 = '{target_address}'
    """)

    if not current_df.empty:
        current_df['상태분류'] = current_df.apply(categorize_status, axis=1)
        
        # 사이트 헤더 정보
        st.markdown(f"#### 📍 {selected_option.split(' [')[0]}")
        
        # 요약 메트릭 (상단 카드)
        cols = st.columns(6)
        states = ['🟢 충전대기', '🔵 충전중', '🔴 점검중', '⚫ 미수신', '⚪ 기타']
        cols[0].metric("총 장비 수", f"{len(current_df):,}대")
        for i, s in enumerate(states):
            count = len(current_df[current_df['상태분류'] == s])
            cols[i+1].metric(s, f"{count:,}대")
        
        st.divider()

        # 실시간 리스트 테이블
        st.subheader("📋 실시간 Heartbeat 상태")
        display_df = current_df[['charger_id', 'status', 'error_state', 'collected_at']].copy()
        display_df.columns = ['충전기 ID', '현재 상태', '장비 에러코드', '최종 수신 일시']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.divider()
        
        # 상태 변화 이력 (타임라인)
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
            
            # 주기에 따른 샘플링 설정
            if '3시간별' in time_view: freq, fmt, limit = '3h', '%m-%d %H시', 24
            elif '일간' in time_view: freq, fmt, limit = 'D', '%Y-%m-%d', 14
            else: freq, fmt, limit = 'W-MON', '%Y-%m-%d(주)', 12
                
            resampled_df = pivot_df.T.resample(freq).last().ffill().T
            resampled_df = resampled_df.fillna('⚪ 기타').iloc[:, -limit:]
            resampled_df.columns = [col.strftime(fmt) for col in resampled_df.columns]
            
            # 스타일 적용하여 출력
            st.dataframe(resampled_df.style.map(color_status), use_container_width=True, height=400)
