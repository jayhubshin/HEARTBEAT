import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg"

# 3. Supabase 연결 및 초기 테스트
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    # 연결 테스트
    test_response = supabase.table("status_history").select("charger_id").limit(1).execute()
    connection_status = "✅ 연결 성공"
except APIError as e:
    st.error(f"⚠️ API 권한 오류: {e}")
    st.warning("""
    **RLS 정책 설정이 필요합니다:**
    
    Supabase 대시보드 → SQL Editor에서 다음을 실행하세요:
    
    ```sql
    ALTER TABLE public.status_history DISABLE ROW LEVEL SECURITY;
    ```
    
    또는 (보안이 중요한 경우):
    
    ```sql
    CREATE POLICY "읽기 허용"
    ON public.status_history
    FOR SELECT
    TO anon, authenticated
    USING (true);
    ```
    """)
    st.stop()
except Exception as e:
    st.error(f"⚠️ 연결 오류: {e}")
    st.stop()

# 4. 데이터 로딩 함수 (안정성 개선)
@st.cache_data(ttl=600)
def get_station_list():
    """충전기 ID 목록 조회"""
    try:
        response = supabase.table("status_history").select("charger_id").limit(1000).execute()
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return ["🔍 데이터가 없습니다..."]
        
        unique_chargers = sorted(df['charger_id'].unique().tolist())
        return ["🔍 충전기를 검색/선택하세요..."] + unique_chargers
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        st.info("💡 위의 RLS 정책 설정 가이드를 확인하세요.")
        return ["⚠️ 조회 실패"]
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return ["⚠️ 시스템 오류"]

@st.cache_data(ttl=300)
def load_target_data(target_id):
    """특정 충전기의 상세 이력 조회"""
    try:
        response = supabase.table("status_history") \
            .select("*") \
            .eq("charger_id", target_id) \
            .order("collected_at", desc=False) \
            .limit(500) \
            .execute()
        
        return pd.DataFrame(response.data)
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 데이터 로딩 오류: {e}")
        return pd.DataFrame()

def categorize_status(row):
    """상태 분류 함수"""
    status = str(row.get('status', ''))
    error = str(row.get('error_state', ''))
    
    if '미수신' in status or '통신' in status:
        return '⚫ 미수신'
    elif (error and error not in ['이상없음', 'None', '', 'null']) or status in ['고장', '점검중']:
        return '🔴 점검중'
    elif '충전중' in status or '충전완료' in status:
        return '🔵 충전중'
    elif '대기' in status or '정상' in status:
        return '🟢 충전대기'
    else:
        return '⚪ 기타'

def color_status(val):
    """상태별 색상 스타일"""
    colors = {
        '⚫ 미수신': 'background-color: #444444; color: white;',
        '🔴 점검중': 'background-color: #EF553B; color: white;',
        '🔵 충전중': 'background-color: #1F77B4; color: white;',
        '🟢 충전대기': 'background-color: #00CC96; color: black;',
        '⚪ 기타': 'background-color: #CCCCCC; color: black;'
    }
    return colors.get(val, 'color: gray;')

# ---------------------------------------------------------
# 메인 화면
# ---------------------------------------------------------

st.title("💓 Project HEARTBEAT")
st.caption("충전기 실시간 이력 관제 (Single Table Mode)")

# 사이드바
st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# 충전기 목록 로드
all_chargers = get_station_list()

# 오류 상태 확인
error_states = ["⚠️ 조회 실패", "⚠️ 시스템 오류"]
if any(state in all_chargers[0] for state in error_states):
    st.error("데이터를 불러올 수 없습니다. 위의 오류 메시지와 해결 가이드를 확인하세요.")
    st.stop()

# 충전기 선택
selected_id = st.sidebar.selectbox("충전기 ID 선택", all_chargers)

# 데이터가 없는 경우
if selected_id == "🔍 데이터가 없습니다...":
    st.info("데이터베이스에 충전기 데이터가 없습니다.")
    st.stop()

# 초기 화면
if selected_id == "🔍 충전기를 검색/선택하세요...":
    st.info("👈 왼쪽에서 **충전기 ID**를 선택하면 상세 이력이 표시됩니다.")
    
    # 전체 통계 표시
    with st.expander("📊 시스템 정보"):
        st.write("**전체 충전기 수:**", len(all_chargers) - 1)
        st.write("**연결 상태:**", connection_status)
    
else:
    # 선택한 충전기의 데이터 로드
    df = load_target_data(selected_id)

    if not df.empty:
        df['상태분류'] = df.apply(categorize_status, axis=1)
        latest = df.iloc[-1]
        
        # 1. 현재 상태 요약
        st.subheader(f"📍 충전기 ID: {selected_id}")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("현재 상태", latest['상태분류'])
        c2.metric("최종 수신", str(latest['collected_at'])[:19])
        c3.metric("에러 코드", latest.get('error_state', 'N/A'))
        c4.metric("전체 기록", f"{len(df)}건")
        
        st.divider()

        # 2. 타임라인 로그 (안정성 개선)
        st.subheader("🎛️ 시간대별 상태 변화")
        
        try:
            df['날짜'] = pd.to_datetime(df['collected_at'], errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=['날짜'])
            
            if len(df) > 0:
                # 최근 20개 데이터만 타임라인으로 표시 (성능 최적화)
                timeline_df = df.tail(20)
                timeline = timeline_df.set_index('charger_id').pivot(columns='날짜', values='상태분류')
                timeline.columns = [c.strftime('%m-%d %H:%M') for c in timeline.columns]
                
                st.dataframe(
                    timeline.style.map(color_status),
                    use_container_width=True,
                    height=150
                )
                
                if len(df) > 20:
                    st.caption(f"💡 최근 20건만 표시 중 (전체: {len(df)}건)")
            else:
                st.warning("유효한 시간 데이터가 없습니다.")
                
        except Exception as e:
            st.warning("타임라인 생성 중 오류가 발생했습니다.")
            st.caption(f"오류 상세: {str(e)}")
        
        st.divider()
        st.subheader("📋 전체 이력 데이터")
        st.dataframe(
            df.sort_values('collected_at', ascending=False),
            use_container_width=True
        )
        
    else:
        st.warning(f"선택한 충전기 '{selected_id}'의 데이터가 없습니다.")

# 푸터
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v1.1")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
