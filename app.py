import streamlit as st
import pandas as pd
from supabase import create_client, Client

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. 팀장님의 프로젝트 ID를 적용한 정확한 주소입니다.
# 직접 입력 방식 (가장 빠른 확인용)
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"

# 슈파베이스 대시보드에서 복사한 'anon' 'public' 키를 여기에 정확히 붙여넣으세요.
SUPABASE_KEY = " 여기에_복사한_long_anon_key_입력 " 

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error(f"⚠️ 연결 설정 오류: {e}")
    st.stop()

# 3. 데이터 로딩 함수 (status_history 테이블만 사용)
@st.cache_data(ttl=600)
def get_station_list():
    # 이력 데이터에서 중복 없는 충전기 ID 목록을 가져옵니다.
    # 만약 status_history에 station_name 컬럼이 없다면 charger_id로 표시됩니다.
    response = supabase.table("status_history").select("charger_id").execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return ["🔍 데이터가 없습니다..."]
    
    return ["🔍 충전기를 검색/선택하세요..."] + sorted(df['charger_id'].unique().tolist())

@st.cache_data(ttl=300)
def load_target_data(target_id):
    # 선택한 충전기 ID의 모든 이력을 가져옵니다.
    response = supabase.table("status_history") \
        .eq("charger_id", target_id) \
        .order("collected_at", desc=False) \
        .execute()
    return pd.DataFrame(response.data)

def categorize_status(row):
    status = str(row.get('status', ''))
    error = str(row.get('error_state', ''))
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
st.title("💓 Project HEARTBEAT")
st.caption("충전기 실시간 이력 관제 (Single Table Mode)")

# 사이드바
st.sidebar.header("📡 관제 타겟")
all_chargers = get_station_list()
selected_id = st.sidebar.selectbox("충전기 ID 선택", all_chargers)

if selected_id == "🔍 충전기를 검색/선택하세요...":
    st.info("👈 왼쪽에서 **충전기 ID**를 선택하면 상세 이력이 표시됩니다.")
else:
    df = load_target_data(selected_id)

    if not df.empty:
        df['상태분류'] = df.apply(categorize_status, axis=1)
        latest = df.iloc[-1] # 가장 최근 데이터
        
        # 1. 현재 상태 요약
        st.subheader(f"📍 충전기 ID: {selected_id}")
        c1, c2, c3 = st.columns(3)
        c1.metric("현재 상태", latest['상태분류'])
        c2.metric("최종 수신", str(latest['collected_at'])[:19])
        c3.metric("에러 코드", latest.get('error_state', 'N/A'))
        
        st.divider()

        # 2. 타임라인 로그 (표 형식)
        st.subheader("🎛️ 시간대별 상태 변화")
        df['날짜'] = pd.to_datetime(df['collected_at']).dt.tz_localize(None)
        
        # 가로축을 시간으로 하는 타임라인 생성
        timeline = df.set_index('charger_id').pivot(columns='날짜', values='상태분류')
        # 시간 형식 가공
        timeline.columns = [c.strftime('%m-%d %H:%M') for c in timeline.columns]
        
        st.dataframe(timeline.style.map(color_status), use_container_width=True)
        
        st.divider()
        st.subheader("📋 전체 이력 데이터")
        st.write(df.sort_values('collected_at', ascending=False))
