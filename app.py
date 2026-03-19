import streamlit as st
import pandas as pd
from supabase import create_client, Client

# 1. 페이지 설정
st.set_page_config(
    page_title="Project HEARTBEAT | 에버온",
    page_icon="💓",
    layout="wide"
)

# 2. 슈파베이스 접속 정보 설정
# (슈파베이스 대시보드 -> Settings -> API에서 확인한 주소와 키를 입력하세요)
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "sb_publishable_wFCWF2ARMVWV0gZ90vPYKQ_0vZh6sRR"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3. 데이터 로딩 함수 (슈파베이스 전용)
@st.cache_data(ttl=600)
def get_station_list():
    # charger_master 테이블에서 중복 없는 충전소 목록 가져오기
    response = supabase.table("charger_master").select("station_name, address1").execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return ["🔍 데이터가 없습니다..."]
    
    df['display_name'] = df['station_name'] + " [" + df['address1'].fillna('') + "]"
    return ["🔍 충전소를 검색/선택하세요..."] + sorted(df['display_name'].unique().tolist())

@st.cache_data(ttl=300)
def load_current_status(target_address):
    # 특정 주소의 충전기들의 가장 최신 상태 가져오기
    # 슈파베이스 쿼리: charger_master와 status_history를 조인하여 가져옴
    response = supabase.table("status_history") \
        .select("charger_id, status, error_state, collected_at, charger_master!inner(station_name, address1)") \
        .eq("charger_master.address1", target_address) \
        .order("collected_at", desc=True) \
        .execute()
    
    df = pd.DataFrame(response.data)
    if not df.empty:
        # 각 충전기별로 가장 최근 1건만 남김
        df = df.sort_values('collected_at', ascending=False).drop_duplicates('charger_id')
    return df

@st.cache_data(ttl=600)
def load_history_log(target_address):
    # 타임라인용 이력 데이터 가져오기
    response = supabase.table("status_history") \
        .select("charger_id, status, error_state, collected_at, charger_master!inner(address1)") \
        .eq("charger_master.address1", target_address) \
        .order("collected_at", desc=False) \
        .execute()
    return pd.DataFrame(response.data)

def categorize_status(row):
    status = str(row['status'])
    error = str(row['error_state'])
    if '미수신' in status or '통신' in status: return '⚫ 미수신'
    elif (error and error != '이상없음') or status in ['고장', '점검중']: return '🔴 점검중'
    elif '충전중' in status or '충전완료' in status: return '🔵 충전중'
    elif '대기' in status or '정상' in status: return '🟢 충전대기'
    else: return '⚪ 기타'

def color_status(val):
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
st.caption("에버온 충전 네트워크 관제 시스템 (Supabase Cloud 연결됨)")

# 사이드바
st.sidebar.header("📡 관제 타겟 설정")
all_stations = get_station_list()
selected_option = st.sidebar.selectbox("충전소 검색/선택", all_stations)

time_view = st.sidebar.radio("로그 간격", ['3시간별 (최근 72시간)', '일간 (최근 14일)', '주간 (최근 12주)'])

if selected_option == "🔍 충전소를 검색/선택하세요...":
    st.info("👈 왼쪽 메뉴에서 관제할 충전소를 선택해 주세요.")
else:
    target_address = selected_option.split(" [")[-1].replace("]", "")
    
    # 데이터 로드
    current_df = load_current_status(target_address)

    if not current_df.empty:
        current_df['상태분류'] = current_df.apply(categorize_status, axis=1)
        
        # 1. 요약 메트릭
        st.markdown(f"#### 📍 {selected_option.split(' [')[0]}")
        cols = st.columns(6)
        states = ['🟢 충전대기', '🔵 충전중', '🔴 점검중', '⚫ 미수신', '⚪ 기타']
        cols[0].metric("총 장비", f"{len(current_df):,}대")
        for i, s in enumerate(states):
            count = len(current_df[current_df['상태분류'] == s])
            cols[i+1].metric(s, f"{count:,}대")
        
        st.divider()

        # 2. 실시간 상세 상태
        st.subheader("📋 실시간 상세 상태 (Live)")
        display_df = current_df[['charger_id', 'status', 'error_state', 'collected_at']].copy()
        display_df.columns = ['충전기 ID', '현재 상태', '에러 코드', '최종 수신 일시']
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.divider()
        
        # 3. 타임라인 로그
        st.subheader(f"🎛️ {time_view} 상태 변화 이력")
        hist_df = load_history_log(target_address)
        
        if not hist_df.empty:
            hist_df['상태그룹'] = hist_df.apply(categorize_status, axis=1)
            hist_df['날짜'] = pd.to_datetime(hist_df['collected_at']).dt.tz_localize(None)
            
            pivot_df = hist_df.pivot_table(index='charger_id', columns='날짜', values='상태그룹', aggfunc='last')
            
            # 주기에 따른 샘플링
            if '3시간별' in time_view: freq, fmt, limit = '3h', '%m-%d %H시', 24
            elif '일간' in time_view: freq, fmt, limit = 'D', '%Y-%m-%d', 14
            else: freq, fmt, limit = 'W-MON', '%Y-%m-%d(주)', 12
                
            resampled_df = pivot_df.T.resample(freq).last().ffill().T
            resampled_df = resampled_df.fillna('⚪ 기타').iloc[:, -limit:]
            resampled_df.columns = [col.strftime(fmt) for col in resampled_df.columns]
            
            st.dataframe(resampled_df.style.map(color_status), use_container_width=True, height=400)
