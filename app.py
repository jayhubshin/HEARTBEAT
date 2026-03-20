import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg"

# 3. 컬럼명 상수 정의 (유지보수성 향상)
COL_CHARGER_ID = "충전기ID"
COL_COLLECTED_AT = "수집날짜"
COL_STATUS = "충전기상태"
COL_ERROR_STATE = "충전이상상태"
COL_STATION_NAME = "충전소명"

# 4. Supabase 연결 및 초기 테스트
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    # 연결 테스트 (실제 컬럼명 사용)
    test_response = supabase.table("status_history").select(COL_CHARGER_ID).limit(1).execute()
    connection_status = "✅ 연결 성공"
except APIError as e:
    st.error(f"⚠️ API 권한 오류: {e}")
    st.warning("""
    **RLS 정책 설정이 필요합니다:**
    
    Supabase 대시보드 → SQL Editor에서 다음을 실행하세요:
    
    ```sql
    ALTER TABLE public.status_history DISABLE ROW LEVEL SECURITY;
    ```
    """)
    st.stop()
except Exception as e:
    st.error(f"⚠️ 연결 오류: {e}")
    st.stop()

# 5. 데이터 로딩 함수 (실제 컬럼명 사용)
@st.cache_data(ttl=600)
def get_station_list():
    """충전기 ID 목록 조회"""
    try:
        # 실제 컬럼명 사용: 충전기ID, 충전소명
        response = supabase.table("status_history").select(f"{COL_CHARGER_ID}, {COL_STATION_NAME}").limit(1000).execute()
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return ["🔍 데이터가 없습니다..."]
        
        # 충전기ID와 충전소명을 함께 표시
        charger_list = []
        for _, row in df.drop_duplicates(subset=[COL_CHARGER_ID]).iterrows():
            charger_id = str(row[COL_CHARGER_ID])
            station_name = str(row.get(COL_STATION_NAME, ''))
            
            if station_name and station_name != 'nan':
                charger_list.append(f"{charger_id} ({station_name})")
            else:
                charger_list.append(charger_id)
        
        return ["🔍 충전기를 검색/선택하세요..."] + sorted(charger_list)
        
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
        # 충전기ID 추출 (괄호 안의 충전소명 제거)
        if '(' in target_id:
            charger_id = target_id.split('(')[0].strip()
        else:
            charger_id = target_id
        
        # 실제 컬럼명 사용
        response = supabase.table("status_history") \
            .select("*") \
            .eq(COL_CHARGER_ID, charger_id) \
            .order(COL_COLLECTED_AT, desc=False) \
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
    """상태 분류 함수 (실제 컬럼명 사용)"""
    status = str(row.get(COL_STATUS, ''))
    error = str(row.get(COL_ERROR_STATE, ''))
    
    if '미수신' in status or '통신' in status:
        return '⚫ 미수신'
    elif (error and error not in ['이상없음', 'None', '', 'null', 'nan']) or status in ['고장', '점검중']:
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
        st.write("**사용 중인 컬럼:**")
        st.write(f"- 충전기ID: `{COL_CHARGER_ID}`")
        st.write(f"- 수집날짜: `{COL_COLLECTED_AT}`")
        st.write(f"- 충전기상태: `{COL_STATUS}`")
        st.write(f"- 충전이상상태: `{COL_ERROR_STATE}`")
    
else:
    # 선택한 충전기의 데이터 로드
    df = load_target_data(selected_id)

    if not df.empty:
        # 상태 분류 추가
        df['상태분류'] = df.apply(categorize_status, axis=1)
        latest = df.iloc[-1]
        
        # 충전기ID 추출
        if '(' in selected_id:
            display_id = selected_id.split('(')[0].strip()
        else:
            display_id = selected_id
        
        # 1. 현재 상태 요약
        st.subheader(f"📍 충전기 ID: {display_id}")
        if COL_STATION_NAME in latest and str(latest[COL_STATION_NAME]) != 'nan':
            st.caption(f"충전소: {latest[COL_STATION_NAME]}")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("현재 상태", latest['상태분류'])
        c2.metric("최종 수신", str(latest[COL_COLLECTED_AT])[:19])
        c3.metric("에러 코드", latest.get(COL_ERROR_STATE, 'N/A'))
        c4.metric("전체 기록", f"{len(df)}건")
        
        # 추가 정보
        with st.expander("🔧 충전기 상세 정보"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write("**기본 정보**")
                st.write(f"- 제조사: {latest.get('제조사', 'N/A')}")
                st.write(f"- 모델명: {latest.get('모델명', 'N/A')}")
                st.write(f"- 충전기용량: {latest.get('충전기용량', 'N/A')}")
                st.write(f"- 급속/완속: {latest.get('급속/완속', 'N/A')}")
            with col2:
                st.write("**설치 정보**")
                st.write(f"- 설치업체: {latest.get('설치업체', 'N/A')}")
                st.write(f"- 설치 년월: {latest.get('설치 년', 'N/A')}-{latest.get('설치 월', 'N/A')}")
                st.write(f"- 설치타입: {latest.get('설치타입', 'N/A')}")
                st.write(f"- 주소: {latest.get('주소1', 'N/A')}")
            with col3:
                st.write("**운영 정보**")
                st.write(f"- 충전소 상태: {latest.get('충전소 상태', 'N/A')}")
                st.write(f"- 사용여부: {latest.get('사용여부', 'N/A')}")
                st.write(f"- 신호세기: {latest.get('신호세기', 'N/A')}")
                st.write(f"- 누적사용량: {latest.get('누적사용량', 'N/A')} kWh")
        
        st.divider()

        # 2. 타임라인 로그 (안정성 개선)
        st.subheader("🎛️ 시간대별 상태 변화")
        
        try:
            # 날짜 변환 (실제 컬럼명 사용)
            df['날짜'] = pd.to_datetime(df[COL_COLLECTED_AT], errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=['날짜'])
            
            if len(df) > 0:
                # 최근 20개 데이터만 타임라인으로 표시 (성능 최적화)
                timeline_df = df.tail(20)
                
                # 충전기ID로 피벗 테이블 생성
                timeline = timeline_df.set_index(COL_CHARGER_ID).pivot(columns='날짜', values='상태분류')
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
        
        # 3. 상태별 통계
        st.subheader("📊 상태 분포")
        status_counts = df['상태분류'].value_counts()
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.bar_chart(status_counts)
        
        with col2:
            for status, count in status_counts.items():
                percentage = (count / len(df)) * 100
                st.metric(status, f"{count}건", f"{percentage:.1f}%")
        
        st.divider()
        
        # 4. 전체 이력 데이터 (필터링 기능 추가)
        st.subheader("📋 전체 이력 데이터")
        
        # 필터링 옵션
        col1, col2 = st.columns(2)
        with col1:
            status_filter = st.multiselect(
                "상태 필터",
                options=df['상태분류'].unique().tolist(),
                default=df['상태분류'].unique().tolist()
            )
        
        with col2:
            show_count = st.slider("표시 개수", 10, 500, 100, 10)
        
        # 필터 적용
        filtered_df = df[df['상태분류'].isin(status_filter)]
        
        # 표시할 주요 컬럼 선택
        display_columns = [COL_COLLECTED_AT, '상태분류', COL_STATUS, COL_ERROR_STATE, 
                          '신호세기', '누적사용량', '충전소 상태', COL_STATION_NAME]
        # 존재하는 컬럼만 선택
        display_columns = [col for col in display_columns if col in filtered_df.columns]
        
        display_df = filtered_df[display_columns].sort_values(COL_COLLECTED_AT, ascending=False).head(show_count)
        
        # 데이터프레임 표시
        st.dataframe(
            display_df.style.map(color_status, subset=['상태분류'] if '상태분류' in display_df.columns else []),
            use_container_width=True,
            height=400
        )
        
        # CSV 다운로드 기능
        csv = filtered_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 CSV 다운로드",
            data=csv,
            file_name=f"heartbeat_{display_id}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
    else:
        st.warning(f"선택한 충전기 '{selected_id}'의 데이터가 없습니다.")

# 푸터
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v1.2")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
