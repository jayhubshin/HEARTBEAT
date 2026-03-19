import streamlit as st
import pandas as pd
import sqlite3

# 페이지 기본 설정
st.set_page_config(page_title="에버온 상태 타임라인 대시보드", layout="wide")

DB_PATH = 'ev_management.db'

@st.cache_data(ttl=600)
def load_data(query):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 충전소 리스트 캐싱 (속도 최적화)
@st.cache_data(ttl=3600)
def get_station_list():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT DISTINCT station_name, address1 FROM charger_master WHERE station_name IS NOT NULL", conn)
    conn.close()
    df['display_name'] = df['station_name'] + " [" + df['address1'].fillna('') + "]"
    return ["🔍 충전소를 검색/선택하세요..."] + df['display_name'].tolist()

# 타임라인 및 KPI용 그룹화 상태 분류 함수
def categorize_status(row):
    status = str(row['status'])
    error = str(row['error_state'])
    
    if '미수신' in status or '통신' in status: return '⚫ 미수신'
    elif error != '이상없음' or status in ['고장', '점검중']: return '🔴 점검중'
    elif '충전중' in status or '충전완료' in status: return '🔵 충전중'
    elif '대기' in status or '정상' in status: return '🟢 충전대기'
    else: return '⚪ 기타'

# 타임라인 표 색상 매핑 함수
def color_status(val):
    if pd.isna(val): return ''
    if '⚫ 미수신' in val: return 'background-color: #444444; color: white;'
    if '🔴 점검중' in val: return 'background-color: #EF553B; color: white;'
    if '🔵 충전중' in val: return 'background-color: #1F77B4; color: white;'
    if '🟢 충전대기' in val: return 'background-color: #00CC96; color: black;'
    return 'color: gray;'

# ---------------------------------------------------------
# UI 구성 시작
st.title("⚡ 에버온 충전기 시계열 상태 관제 대시보드")

# 1. 사이드바: 검색 및 타임라인 설정
st.sidebar.header("1️⃣ 충전소 검색 및 선택")
station_options = get_station_list()
selected_option = st.sidebar.selectbox("충전소명 또는 주소 입력", station_options)

st.sidebar.divider()
st.sidebar.header("2️⃣ 타임라인 옵션 설정")
# 중복되던 정렬 옵션은 삭제하고, 타임라인 간격 설정만 남김
time_view = st.sidebar.radio("타임라인 간격", ['시간별 (최근 24시간)', '일간 (최근 14일)', '주간 (최근 12주)'])

# ---------------------------------------------------------
# 🚨 선택 전 대기 화면 (버벅임 방지) 🚨
if selected_option == "🔍 충전소를 검색/선택하세요...":
    st.info("👈 좌측 사이드바에서 관제할 **충전소를 검색하고 선택**해 주세요. (표의 열 제목을 클릭하면 정렬이 가능합니다.)")
    st.stop() 

# ---------------------------------------------------------
# 충전소 선택 후 상세 조회
target_address = selected_option.split(" [")[-1].replace("]", "")
search_condition = f"AND m.address1 = '{target_address}'"

current_query = f"""
    SELECT m.charger_id, m.station_name, m.address1, m.address_detail, m.model_name,
           h.status, h.error_state, h.collected_at
    FROM charger_master m
    JOIN status_history h ON m.charger_id = h.charger_id
    WHERE h.id IN (SELECT MAX(id) FROM status_history GROUP BY charger_id)
    {search_condition}
"""
current_df = load_data(current_query)

st.subheader(f"📊 현재 최신 상태 요약 ({selected_option.split(' [')[0]})")

if not current_df.empty:
    current_df['상태분류'] = current_df.apply(categorize_status, axis=1)
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("해당 사이트 총 대수", f"{len(current_df):,} 대")
    col2.metric("🟢 충전대기", f"{len(current_df[current_df['상태분류'] == '🟢 충전대기']):,} 대")
    col3.metric("🔵 충전중", f"{len(current_df[current_df['상태분류'] == '🔵 충전중']):,} 대")
    col4.metric("🔴 점검중", f"{len(current_df[current_df['상태분류'] == '🔴 점검중']):,} 대")
    col5.metric("⚫ 미수신", f"{len(current_df[current_df['상태분류'] == '⚫ 미수신']):,} 대")
    col6.metric("⚪ 기타", f"{len(current_df[current_df['상태분류'] == '⚪ 기타']):,} 대")
    
    st.divider()

    # ---------------------------------------------------------
    # 상세 목록 표 (원문 상세 상태 표시)
    st.subheader("📋 상세 목록 현황")
    st.caption("💡 팁: 표의 컬럼명(충전기ID, 상세 상태 등)을 클릭하면 오름차순/내림차순 정렬이 됩니다.")
    
    # 노출할 컬럼 선택: 그룹화된 '상태분류' 대신 실제 'status' 값을 보여줌
    display_df = current_df[['station_name', 'address1', 'charger_id', 'status', 'error_state', 'collected_at']].copy()
    display_df.columns = ['충전소명', '주소', '충전기ID', '상세 상태', '이상상태_상세', '최종수집일시']
    display_df.index = range(1, len(display_df) + 1) 
    
    st.dataframe(display_df, use_container_width=True)

    st.divider()
    
    # ---------------------------------------------------------
    # 타임라인(Heatmap) 표
    st.subheader(f"🎛️ {time_view} 타임라인 이력")
    
    history_query = f"""
        SELECT h.charger_id, h.status, h.error_state, h.collected_at
        FROM status_history h
        JOIN charger_master m ON h.charger_id = m.charger_id
        WHERE m.address1 = '{target_address}'
    """
    hist_df = load_data(history_query)
    
    if not hist_df.empty:
        hist_df['상태그룹'] = hist_df.apply(categorize_status, axis=1)
        hist_df['날짜'] = pd.to_datetime(hist_df['collected_at'], errors='coerce')
        hist_df = hist_df.dropna(subset=['날짜']).sort_values('날짜')
        
        pivot_df = hist_df.pivot_table(index='charger_id', columns='날짜', values='상태그룹', aggfunc='last')
        
        if '시간별' in time_view: freq, fmt, limit = 'h', '%m-%d %H:00', 24
        elif '일간' in time_view: freq, fmt, limit = 'D', '%Y-%m-%d', 14
        else: freq, fmt, limit = 'W-MON', '%Y-%m-%d(주)', 12
            
        resampled_df = pivot_df.T.resample(freq).last().ffill().T
        resampled_df = resampled_df.fillna('⚪ 기타')
        resampled_df.columns = [col.strftime(fmt) for col in resampled_df.columns]
        resampled_df = resampled_df.iloc[:, -limit:]
        
        master_info = current_df[['charger_id', 'station_name']].set_index('charger_id')
        final_df = master_info.join(resampled_df, how='inner')
        final_df.index.name = '충전기 ID'
        final_df.rename(columns={'station_name': '충전소명'}, inplace=True)
        
        styled_df = final_df.style.map(color_status)
        st.dataframe(styled_df, use_container_width=True, height=500)
else:
    st.error("해당 충전소의 데이터에 문제가 있거나 로드할 수 없습니다.")