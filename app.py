import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg"

# 3. 컬럼명 상수 정의
COL_CHARGER_ID = "충전기ID"
COL_COLLECTED_AT = "수집날짜"
COL_STATUS = "충전기상태"
COL_ERROR_STATE = "충전이상상태"
COL_STATION_NAME = "충전소명"

# 4. Supabase 연결
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    connection_status = "✅ 연결 성공"
except Exception as e:
    st.error(f"⚠️ 연결 오류: {e}")
    st.stop()

# 5. 데이터 로딩 함수 (대폭 최적화)
@st.cache_data(ttl=600)
def get_station_list():
    """충전기 ID 목록 조회 (성능 최적화)"""
    try:
        # ⚡ 핵심 최적화:
        # 1. 최신 데이터만 제한적으로 조회 (전체 스캔 방지)
        # 2. 필요한 컬럼만 선택 (네트워크 부하 감소)
        # 3. 인덱스를 활용한 정렬
        response = supabase.table("status_history") \
            .select(f"{COL_CHARGER_ID}, {COL_STATION_NAME}") \
            .order(COL_COLLECTED_AT, desc=True) \
            .limit(2000) \
            .execute()
        
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return ["🔍 데이터가 없습니다..."]
        
        # Python에서 중복 제거 (DB보다 빠름)
        unique_df = df.drop_duplicates(subset=[COL_CHARGER_ID])
        
        # 사용자 친화적 표시 형식 생성
        charger_list = []
        for _, row in unique_df.iterrows():
            charger_id = str(row[COL_CHARGER_ID])
            station_name = str(row.get(COL_STATION_NAME, ''))
            
            if station_name and station_name not in ['nan', 'None', '']:
                charger_list.append(f"{charger_id} ({station_name})")
            else:
                charger_list.append(charger_id)
        
        return ["🔍 충전기를 검색/선택하세요..."] + sorted(charger_list)
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        
        # 타임아웃 오류 자동 감지 및 해결 가이드 제공
        if 'timeout' in str(e).lower() or '57014' in str(e):
            st.warning("""
            **⏱️ 쿼리 타임아웃 발생!**
            
            **해결 방법:** Supabase 대시보드 → SQL Editor에서 실행
            
            ```sql
            CREATE INDEX idx_charger_id ON public.status_history ("충전기ID");
            CREATE INDEX idx_collected_at ON public.status_history ("수집날짜" DESC);
            ```
            """)
        
        return ["⚠️ 조회 실패"]
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return ["⚠️ 시스템 오류"]

@st.cache_data(ttl=300)
def load_target_data(target_id):
    """특정 충전기의 상세 이력 조회 (최적화)"""
    try:
        # 충전기ID 추출
        if '(' in target_id:
            charger_id = target_id.split('(')[0].strip()
        else:
            charger_id = target_id
        
        # ⚡ 성능 최적화 포인트:
        # 1. 인덱스 활용을 위한 WHERE + ORDER BY 조합
        # 2. 최근 데이터만 제한 (200건)
        # 3. 필요한 컬럼만 선택
        
        select_columns = f"{COL_CHARGER_ID}, {COL_COLLECTED_AT}, {COL_STATUS}, {COL_ERROR_STATE}, {COL_STATION_NAME}, 제조사, 모델명, 충전기용량, 급속/완속, 신호세기, 누적사용량, 충전소 상태"
        
        response = supabase.table("status_history") \
            .select(select_columns) \
            .eq(COL_CHARGER_ID, charger_id) \
            .order(COL_COLLECTED_AT, desc=True) \
            .limit(200) \
            .execute()
        
        df = pd.DataFrame(response.data)
        
        # 시간 순서대로 재정렬 (최신 데이터가 마지막)
        if not df.empty:
            df = df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)
        
        return df
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        
        # 타임아웃 오류 처리
        if 'timeout' in str(e).lower() or '57014' in str(e):
            st.warning("⏱️ 쿼리 타임아웃! 인덱스를 생성하거나 조회 기간을 줄여주세요.")
        
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 데이터 로딩 오류: {e}")
        return pd.DataFrame()

def categorize_status(row):
    """상태 분류 함수"""
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
st.caption("충전기 실시간 이력 관제 (Performance Optimized)")

# 사이드바
st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# 성능 정보 표시
with st.sidebar.expander("⚡ 성능 최적화 상태"):
    st.write("**현재 설정:**")
    st.write("✅ 충전기 목록: 최근 2000건")
    st.write("✅ 상세 이력: 최근 200건")
    st.write("✅ 캐싱: 10분/5분")
    st.write("")
    st.write("**권장 사항:**")
    st.write("• 데이터베이스 인덱스 생성")
    st.write("• 정기적 데이터 정리")

# 충전기 목록 로드
with st.spinner("충전기 목록 로딩 중..."):
    all_chargers = get_station_list()

# 오류 상태 확인
error_states = ["⚠️ 조회 실패", "⚠️ 시스템 오류"]
if any(state in all_chargers[0] for state in error_states):
    st.error("데이터를 불러올 수 없습니다.")
    
    with st.expander("🔧 타임아웃 문제 해결 가이드"):
        st.markdown("""
        ### **1단계: 인덱스 생성 (필수)**
        
        Supabase 대시보드 → SQL Editor에서 실행:
        
        ```sql
        CREATE INDEX idx_charger_id ON public.status_history ("충전기ID");
        CREATE INDEX idx_collected_at ON public.status_history ("수집날짜" DESC);
        CREATE INDEX idx_charger_collected ON public.status_history ("충전기ID", "수집날짜" DESC);
        ```
        
        ### **2단계: 데이터 정리 (선택사항)**
        
        오래된 데이터 삭제로 테이블 크기 축소:
        
        ```sql
        -- 현재 데이터 양 확인
        SELECT COUNT(*) as total_rows,
               pg_size_pretty(pg_total_relation_size('status_history')) as table_size
        FROM status_history;
        
        -- 6개월 이상 된 데이터 삭제 (예시)
        DELETE FROM public.status_history 
        WHERE "수집날짜" < NOW() - INTERVAL '6 months';
        ```
        
        ### **3단계: 정기적 유지보수**
        
        - 월 1회 오래된 데이터 정리
        - 쿼리 성능 모니터링
        - 필요시 추가 인덱스 생성
        """)
    st.stop()

# 충전기 선택
selected_id = st.sidebar.selectbox("충전기 ID 선택", all_chargers, key="charger_select")

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
        st.write("**데이터 조회 제한:**", "충전기 목록 2000건, 상세 이력 200건")
    
else:
    # 선택한 충전기의 데이터 로드
    with st.spinner(f"'{selected_id}' 데이터 로딩 중..."):
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
        if COL_STATION_NAME in latest and str(latest[COL_STATION_NAME]) not in ['nan', 'None', '']:
            st.caption(f"충전소: {latest[COL_STATION_NAME]}")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("현재 상태", latest['상태분류'])
        c2.metric("최종 수신", str(latest[COL_COLLECTED_AT])[:19])
        c3.metric("에러 코드", latest.get(COL_ERROR_STATE, 'N/A'))
        c4.metric("조회 기록", f"{len(df)}건")
        
        # 추가 정보
        with st.expander("🔧 충전기 상세 정보"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**기본 정보**")
                st.write(f"- 제조사: {latest.get('제조사', 'N/A')}")
                st.write(f"- 모델명: {latest.get('모델명', 'N/A')}")
                st.write(f"- 충전기용량: {latest.get('충전기용량', 'N/A')}")
                st.write(f"- 급속/완속: {latest.get('급속/완속', 'N/A')}")
            with col2:
                st.write("**운영 정보**")
                st.write(f"- 충전소 상태: {latest.get('충전소 상태', 'N/A')}")
                st.write(f"- 신호세기: {latest.get('신호세기', 'N/A')}")
                st.write(f"- 누적사용량: {latest.get('누적사용량', 'N/A')} kWh")
        
        st.divider()

        # 2. 타임라인 로그
        st.subheader("🎛️ 시간대별 상태 변화")
        
        try:
            df['날짜'] = pd.to_datetime(df[COL_COLLECTED_AT], errors='coerce').dt.tz_localize(None)
            df = df.dropna(subset=['날짜'])
            
            if len(df) > 0:
                # 최근 15개 데이터만 타임라인으로 표시
                timeline_df = df.tail(15)
                timeline = timeline_df.set_index(COL_CHARGER_ID).pivot(columns='날짜', values='상태분류')
                timeline.columns = [c.strftime('%m-%d %H:%M') for c in timeline.columns]
                
                st.dataframe(
                    timeline.style.map(color_status),
                    use_container_width=True,
                    height=150
                )
                
                if len(df) > 15:
                    st.caption(f"💡 최근 15건만 표시 중 (전체: {len(df)}건)")
            else:
                st.warning("유효한 시간 데이터가 없습니다.")
                
        except Exception as e:
            st.warning("타임라인 생성 중 오류가 발생했습니다.")
            st.caption(f"오류: {str(e)}")
        
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
        
        # 4. 전체 이력 데이터
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
            show_count = st.slider("표시 개수", 10, min(200, len(df)), min(50, len(df)), 10)
        
        # 필터 적용
        filtered_df = df[df['상태분류'].isin(status_filter)]
        
        # 표시할 주요 컬럼 선택
        display_columns = [COL_COLLECTED_AT, '상태분류', COL_STATUS, COL_ERROR_STATE, '신호세기', '누적사용량']
        display_columns = [col for col in display_columns if col in filtered_df.columns]
        
        display_df = filtered_df[display_columns].sort_values(COL_COLLECTED_AT, ascending=False).head(show_count)
        
        # 데이터프레임 표시
        st.dataframe(
            display_df.style.map(color_status, subset=['상태분류'] if '상태분류' in display_df.columns else []),
            use_container_width=True,
            height=400
        )
        
        # CSV 다운로드
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
st.sidebar.caption("💓 Project HEARTBEAT v1.3 (Performance Optimized)")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
