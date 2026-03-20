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
COL_SITE_ID = "사이트ID"
COL_STATION_ID = "충전소ID"
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

# 5. 데이터 로딩 함수 (슬래시 문제 완전 해결)
@st.cache_data(ttl=600)
def get_station_list():
    """충전기 목록 조회 (다중 검색 지원)"""
    try:
        # ⚡ 핵심 해결책: select("*")로 모든 컬럼 조회
        response = supabase.table("status_history") \
            .select("*") \
            .order(COL_COLLECTED_AT, desc=True) \
            .limit(2000) \
            .execute()
        
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return ["🔍 데이터가 없습니다..."]
        
        # Python에서 중복 제거 및 검색 목록 생성
        unique_df = df.drop_duplicates(subset=[COL_CHARGER_ID])
        
        search_list = []
        for _, row in unique_df.iterrows():
            charger_id = str(row[COL_CHARGER_ID])
            station_name = str(row.get(COL_STATION_NAME, ''))
            station_id = str(row.get(COL_STATION_ID, ''))
            site_id = str(row.get(COL_SITE_ID, ''))
            
            # 검색 가능한 표시 형식 생성
            display_text = f"{charger_id}"
            
            # 충전소명 추가
            if station_name and station_name not in ['nan', 'None', '']:
                display_text += f" ({station_name})"
            
            # ID 정보 추가
            id_info = []
            if station_id and station_id not in ['nan', 'None', '']:
                id_info.append(f"충전소:{station_id}")
            if site_id and site_id not in ['nan', 'None', '']:
                id_info.append(f"사이트:{site_id}")
            
            if id_info:
                display_text += f" [{', '.join(id_info)}]"
            
            search_list.append(display_text)
        
        return ["🔍 충전기/충전소/사이트 ID로 검색하세요..."] + sorted(search_list)
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        
        if 'timeout' in str(e).lower() or '57014' in str(e):
            st.warning("""
            **⏱️ 쿼리 타임아웃 발생!**
            
            Supabase 대시보드 → SQL Editor에서 인덱스 생성:
            
            ```sql
            CREATE INDEX idx_charger_id ON public.status_history ("충전기ID");
            CREATE INDEX idx_collected_at ON public.status_history ("수집날짜" DESC);
            CREATE INDEX idx_station_id ON public.status_history ("충전소ID");
            CREATE INDEX idx_site_id ON public.status_history ("사이트ID");
            ```
            """)
        
        return ["⚠️ 조회 실패"]
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return ["⚠️ 시스템 오류"]

@st.cache_data(ttl=300)
def load_target_data(target_text):
    """선택된 항목의 상세 이력 조회"""
    try:
        # 충전기ID 추출 (괄호 앞부분)
        if '(' in target_text:
            charger_id = target_text.split('(')[0].strip()
        else:
            charger_id = target_text.strip()
        
        # ⚡ 슬래시 문제 해결: select("*") 사용
        response = supabase.table("status_history") \
            .select("*") \
            .eq(COL_CHARGER_ID, charger_id) \
            .order(COL_COLLECTED_AT, desc=True) \
            .limit(200) \
            .execute()
        
        df = pd.DataFrame(response.data)
        
        # 시간 순서대로 재정렬
        if not df.empty:
            df = df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)
        
        return df, charger_id
        
    except APIError as e:
        st.error(f"❌ 데이터 조회 실패: {e}")
        return pd.DataFrame(), ""
    except Exception as e:
        st.error(f"❌ 데이터 로딩 오류: {e}")
        return pd.DataFrame(), ""

@st.cache_data(ttl=300)
def search_by_id(search_type, search_value):
    """충전소ID 또는 사이트ID로 직접 검색"""
    try:
        search_column = COL_STATION_ID if search_type == "충전소ID" else COL_SITE_ID
        
        response = supabase.table("status_history") \
            .select("*") \
            .eq(search_column, search_value) \
            .order(COL_COLLECTED_AT, desc=True) \
            .limit(500) \
            .execute()
        
        df = pd.DataFrame(response.data)
        
        if not df.empty:
            df = df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)
        
        return df
        
    except APIError as e:
        st.error(f"❌ 검색 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 검색 오류: {e}")
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
st.caption("충전기 실시간 이력 관제 (Multi-Search Support)")

# 사이드바
st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# 검색 모드 선택
search_mode = st.sidebar.radio(
    "검색 방법",
    ["목록에서 선택", "충전소ID 직접 입력", "사이트ID 직접 입력"],
    index=0
)

df = pd.DataFrame()
display_id = ""

if search_mode == "목록에서 선택":
    # 기존 방식: 드롭다운에서 선택
    with st.spinner("목록 로딩 중..."):
        all_chargers = get_station_list()
    
    # 오류 상태 확인
    if "⚠️" in all_chargers[0]:
        st.error("데이터를 불러올 수 없습니다. 위의 해결 가이드를 확인하세요.")
        st.stop()
    
    # 검색 가능한 선택박스
    selected_item = st.sidebar.selectbox(
        "충전기/충전소/사이트 검색",
        all_chargers,
        key="item_select"
    )
    
    if selected_item.startswith("🔍"):
        st.info("👈 왼쪽에서 **충전기ID**, **충전소ID**, 또는 **사이트ID**를 검색하세요.")
        
        with st.expander("💡 검색 도움말"):
            st.markdown("""
            **검색 방법:**
            - 선택박스에 ID나 충전소명의 일부를 입력하면 자동 필터링됩니다
            - 형식: `충전기ID (충전소명) [충전소:XXX, 사이트:YYY]`
            
            **예시:**
            - `1111057000004-02` 입력 → 해당 충전기 찾기
            - `서울종로` 입력 → 충전소명으로 찾기
            - `충전소:12345` 입력 → 충전소ID로 찾기
            """)
        st.stop()
    else:
        df, display_id = load_target_data(selected_item)

elif search_mode == "충전소ID 직접 입력":
    station_id = st.sidebar.text_input("충전소ID", placeholder="예: 12345")
    
    if st.sidebar.button("🔍 검색", key="search_station"):
        if station_id:
            with st.spinner(f"충전소ID '{station_id}' 검색 중..."):
                df = search_by_id("충전소ID", station_id)
            display_id = f"충전소ID: {station_id}"
        else:
            st.warning("충전소ID를 입력하세요.")
            st.stop()
    else:
        st.info("👈 **충전소ID**를 입력하고 검색 버튼을 클릭하세요.")
        st.stop()

else:  # 사이트ID 직접 입력
    site_id = st.sidebar.text_input("사이트ID", placeholder="예: 67890")
    
    if st.sidebar.button("🔍 검색", key="search_site"):
        if site_id:
            with st.spinner(f"사이트ID '{site_id}' 검색 중..."):
                df = search_by_id("사이트ID", site_id)
            display_id = f"사이트ID: {site_id}"
        else:
            st.warning("사이트ID를 입력하세요.")
            st.stop()
    else:
        st.info("👈 **사이트ID**를 입력하고 검색 버튼을 클릭하세요.")
        st.stop()

# ---------------------------------------------------------
# 데이터 표시
# ---------------------------------------------------------

if not df.empty:
    # 상태 분류 추가
    df['상태분류'] = df.apply(categorize_status, axis=1)
    latest = df.iloc[-1]
    
    # 1. 헤더 정보
    st.subheader(f"📍 {display_id}")
    
    # 기본 정보 표시
    info_cols = st.columns(4)
    with info_cols[0]:
        if COL_CHARGER_ID in latest:
            st.info(f"**충전기ID:** {latest[COL_CHARGER_ID]}")
    with info_cols[1]:
        if COL_STATION_NAME in latest and str(latest[COL_STATION_NAME]) not in ['nan', 'None', '']:
            st.info(f"**충전소:** {latest[COL_STATION_NAME]}")
    with info_cols[2]:
        if COL_STATION_ID in latest and str(latest[COL_STATION_ID]) not in ['nan', 'None', '']:
            st.info(f"**충전소ID:** {latest[COL_STATION_ID]}")
    with info_cols[3]:
        if COL_SITE_ID in latest and str(latest[COL_SITE_ID]) not in ['nan', 'None', '']:
            st.info(f"**사이트ID:** {latest[COL_SITE_ID]}")
    
    # 2. 상태 메트릭
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재 상태", latest['상태분류'])
    c2.metric("최종 수신", str(latest[COL_COLLECTED_AT])[:19])
    c3.metric("에러 코드", latest.get(COL_ERROR_STATE, 'N/A'))
    c4.metric("조회 기록", f"{len(df)}건")
    
    # 3. 상세 정보 (슬래시 문제 해결됨!)
    with st.expander("🔧 충전기 상세 정보"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("**기본 스펙**")
            st.write(f"- 제조사: {latest.get('제조사', 'N/A')}")
            st.write(f"- 모델명: {latest.get('모델명', 'N/A')}")
            st.write(f"- 충전기용량: {latest.get('충전기용량', 'N/A')}")
            # ✅ 이제 슬래시 포함 컬럼도 정상 접근 가능!
            st.write(f"- 급속/완속: {latest.get('급속/완속', 'N/A')}")
        with col2:
            st.write("**운영 정보**")
            st.write(f"- 충전소 상태: {latest.get('충전소 상태', 'N/A')}")
            st.write(f"- 신호세기: {latest.get('신호세기', 'N/A')}")
            st.write(f"- 누적사용량: {latest.get('누적사용량', 'N/A')} kWh")
            st.write(f"- 사용여부: {latest.get('사용여부', 'N/A')}")
        with col3:
            st.write("**위치 정보**")
            st.write(f"- 주소: {latest.get('주소1', 'N/A')}")
            st.write(f"- 상세주소: {latest.get('상세주소', 'N/A')}")
            st.write(f"- 설치업체: {latest.get('설치업체', 'N/A')}")
    
    st.divider()

    # 4. 다중 충전기 요약 (충전소/사이트 검색 시)
    if search_mode in ["충전소ID 직접 입력", "사이트ID 직접 입력"]:
        unique_chargers = df[COL_CHARGER_ID].nunique()
        if unique_chargers > 1:
            st.subheader(f"📋 충전기 목록 ({unique_chargers}대)")
            
            # 충전기별 최신 상태 요약
            charger_summary = df.groupby(COL_CHARGER_ID).agg({
                COL_COLLECTED_AT: 'max',
                COL_STATUS: 'last',
                COL_ERROR_STATE: 'last',
                COL_STATION_NAME: 'first'
            }).reset_index()
            
            charger_summary['상태분류'] = charger_summary.apply(categorize_status, axis=1)
            
            st.dataframe(
                charger_summary.style.map(color_status, subset=['상태분류']),
                use_container_width=True,
                height=300
            )
            st.divider()

    # 5. 타임라인 로그
    st.subheader("🎛️ 시간대별 상태 변화")
    
    try:
        df['날짜'] = pd.to_datetime(df[COL_COLLECTED_AT], errors='coerce').dt.tz_localize(None)
        df_clean = df.dropna(subset=['날짜'])
        
        if len(df_clean) > 0:
            # 최근 20개 데이터만 타임라인으로 표시
            timeline_df = df_clean.tail(20)
            
            # 충전기ID별로 피벗 (다중 충전기 지원)
            timeline = timeline_df.pivot_table(
                index=COL_CHARGER_ID,
                columns='날짜',
                values='상태분류',
                aggfunc='first'
            )
            timeline.columns = [c.strftime('%m-%d %H:%M') for c in timeline.columns]
            
            st.dataframe(
                timeline.style.map(color_status),
                use_container_width=True,
                height=min(200, len(timeline) * 35 + 50)
            )
            
            if len(df_clean) > 20:
                st.caption(f"💡 최근 20건만 표시 중 (전체: {len(df_clean)}건)")
        else:
            st.warning("유효한 시간 데이터가 없습니다.")
            
    except Exception as e:
        st.warning("타임라인 생성 중 오류가 발생했습니다.")
        st.caption(f"오류: {str(e)}")
    
    st.divider()
    
    # 6. 상태별 통계
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
    
    # 7. 전체 이력 데이터
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
    display_columns = [
        COL_CHARGER_ID, COL_COLLECTED_AT, '상태분류', COL_STATUS, 
        COL_ERROR_STATE, '신호세기', '누적사용량', '급속/완속'
    ]
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
        file_name=f"heartbeat_{display_id.replace(':', '_')}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
else:
    st.warning(f"'{display_id}'에 해당하는 데이터가 없습니다.")

# 푸터
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v1.5 (Special Character Fixed)")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
