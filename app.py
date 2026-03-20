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

# ============================================================
# ⚡ 핵심 수정: 슬래시가 포함된 "급속/완속" 컬럼 때문에
#    select에 컬럼을 나열하면 PostgREST가 파싱 에러를 낸다.
#    → Supabase RPC(서버 함수)를 쓰거나,
#      select("*")를 쓰되, PostgREST 버전에 따라 동작이 다를 수 있다.
#    → 가장 안전한 방법: 컬럼명에 따옴표를 붙여 이스케이프하거나
#      select 파라미터를 아예 비워두지 않고 "*"만 사용.
#
#    실제 원인: supabase-py의 .select()에 컬럼 리스트를 넣으면
#    내부적으로 쉼표로 이어붙이는데 "급속/완속"의 /가 JSON 연산자로 해석됨.
#    → select("*")만 사용하면 문제 없음.
#    → 이전 코드에서도 select("*")를 쓰고 있었지만,
#      어딘가에서 컬럼 리스트가 들어간 것으로 보임.
#    → 모든 쿼리를 select("*")로 통일.
# ============================================================

# 4. Supabase 연결
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    connection_status = "✅ 연결 성공"
except Exception as e:
    st.error(f"⚠️ 연결 오류: {e}")
    st.stop()


# ============================================================
# 5. 데이터 로딩 함수
# ============================================================

@st.cache_data(ttl=600)
def load_search_index():
    """
    충전기 목록을 로드하여 검색 인덱스(DataFrame)를 만든다.
    충전기ID, 충전소명, 충전소ID, 사이트ID, 주소 등을 포함.
    """
    try:
        response = (
            supabase.table("status_history")
            .select("*")
            .order(COL_COLLECTED_AT, desc=True)
            .limit(3000)
            .execute()
        )

        df = pd.DataFrame(response.data)

        if df.empty:
            return pd.DataFrame()

        # 충전기ID 기준 중복 제거 (최신 데이터 기준)
        index_df = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="first").copy()

        # 검색용 통합 텍스트 컬럼 생성
        search_parts = []
        for col in [COL_CHARGER_ID, COL_STATION_NAME, COL_STATION_ID, COL_SITE_ID,
                     "주소1", "상세주소", "제조사", "모델명"]:
            if col in index_df.columns:
                search_parts.append(index_df[col].astype(str).fillna(""))

        index_df["_search_text"] = ""
        for part in search_parts:
            index_df["_search_text"] = index_df["_search_text"] + " " + part

        index_df["_search_text"] = index_df["_search_text"].str.lower()

        return index_df

    except APIError as e:
        st.error(f"❌ 검색 인덱스 로딩 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame()


def search_chargers(index_df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    """키워드로 검색 인덱스를 필터링한다."""
    if index_df.empty or not keyword.strip():
        return pd.DataFrame()

    kw = keyword.strip().lower()
    # 공백으로 분리된 다중 키워드를 모두 포함하는 행만 반환 (AND 검색)
    tokens = kw.split()
    mask = pd.Series([True] * len(index_df), index=index_df.index)
    for token in tokens:
        mask = mask & index_df["_search_text"].str.contains(token, na=False)

    results = index_df[mask].copy()
    return results


@st.cache_data(ttl=300)
def load_charger_history(charger_id: str):
    """단일 충전기의 이력 조회"""
    try:
        response = (
            supabase.table("status_history")
            .select("*")
            .eq(COL_CHARGER_ID, charger_id)
            .order(COL_COLLECTED_AT, desc=True)
            .limit(200)
            .execute()
        )

        df = pd.DataFrame(response.data)

        if not df.empty:
            df = df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)

        return df

    except APIError as e:
        st.error(f"❌ 이력 조회 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 데이터 로딩 오류: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_group_history(search_type: str, search_value: str):
    """충전소ID 또는 사이트ID로 그룹 이력 조회"""
    try:
        col = COL_STATION_ID if search_type == "충전소ID" else COL_SITE_ID

        response = (
            supabase.table("status_history")
            .select("*")
            .eq(col, search_value)
            .order(COL_COLLECTED_AT, desc=True)
            .limit(500)
            .execute()
        )

        df = pd.DataFrame(response.data)

        if not df.empty:
            df = df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)

        return df

    except APIError as e:
        st.error(f"❌ 그룹 조회 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame()


# ============================================================
# 유틸리티 함수
# ============================================================

def categorize_status(row):
    """상태 분류 함수"""
    status = str(row.get(COL_STATUS, ""))
    error = str(row.get(COL_ERROR_STATE, ""))

    if "미수신" in status or "통신" in status:
        return "⚫ 미수신"
    elif (error and error not in ["이상없음", "None", "", "null", "nan"]) or status in ["고장", "점검중"]:
        return "🔴 점검중"
    elif "충전중" in status or "충전완료" in status:
        return "🔵 충전중"
    elif "대기" in status or "정상" in status:
        return "🟢 충전대기"
    else:
        return "⚪ 기타"


def color_status(val):
    """상태별 색상 스타일"""
    colors = {
        "⚫ 미수신": "background-color: #444444; color: white;",
        "🔴 점검중": "background-color: #EF553B; color: white;",
        "🔵 충전중": "background-color: #1F77B4; color: white;",
        "🟢 충전대기": "background-color: #00CC96; color: black;",
        "⚪ 기타": "background-color: #CCCCCC; color: black;",
    }
    return colors.get(val, "color: gray;")


def render_data(df: pd.DataFrame, display_id: str):
    """데이터 시각화 공통 함수"""

    if df.empty:
        st.warning(f"'{display_id}'에 해당하는 데이터가 없습니다.")
        return

    # 상태 분류 추가
    df["상태분류"] = df.apply(categorize_status, axis=1)
    latest = df.iloc[-1]

    # 1. 헤더 정보
    st.subheader(f"📍 {display_id}")

    info_cols = st.columns(4)
    with info_cols[0]:
        if COL_CHARGER_ID in latest:
            st.info(f"**충전기ID:** {latest[COL_CHARGER_ID]}")
    with info_cols[1]:
        val = str(latest.get(COL_STATION_NAME, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**충전소:** {val}")
    with info_cols[2]:
        val = str(latest.get(COL_STATION_ID, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**충전소ID:** {val}")
    with info_cols[3]:
        val = str(latest.get(COL_SITE_ID, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**사이트ID:** {val}")

    # 2. 상태 메트릭
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재 상태", latest["상태분류"])
    c2.metric("최종 수신", str(latest[COL_COLLECTED_AT])[:19])
    c3.metric("에러 코드", latest.get(COL_ERROR_STATE, "N/A"))
    c4.metric("조회 기록", f"{len(df)}건")

    # 3. 상세 정보
    with st.expander("🔧 충전기 상세 정보"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("**기본 스펙**")
            st.write(f"- 제조사: {latest.get('제조사', 'N/A')}")
            st.write(f"- 모델명: {latest.get('모델명', 'N/A')}")
            st.write(f"- 충전기용량: {latest.get('충전기용량', 'N/A')}")
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

    # 4. 다중 충전기 요약
    unique_chargers = df[COL_CHARGER_ID].nunique()
    if unique_chargers > 1:
        st.subheader(f"📋 충전기 목록 ({unique_chargers}대)")

        charger_summary = (
            df.groupby(COL_CHARGER_ID)
            .agg({
                COL_COLLECTED_AT: "max",
                COL_STATUS: "last",
                COL_ERROR_STATE: "last",
                COL_STATION_NAME: "first",
            })
            .reset_index()
        )
        charger_summary["상태분류"] = charger_summary.apply(categorize_status, axis=1)

        st.dataframe(
            charger_summary.style.map(color_status, subset=["상태분류"]),
            use_container_width=True,
            height=300,
        )
        st.divider()

    # 5. 타임라인 로그
    st.subheader("🎛️ 시간대별 상태 변화")

    try:
        df["날짜"] = pd.to_datetime(df[COL_COLLECTED_AT], errors="coerce").dt.tz_localize(None)
        df_clean = df.dropna(subset=["날짜"])

        if len(df_clean) > 0:
            timeline_df = df_clean.tail(20)

            timeline = timeline_df.pivot_table(
                index=COL_CHARGER_ID,
                columns="날짜",
                values="상태분류",
                aggfunc="first",
            )
            timeline.columns = [c.strftime("%m-%d %H:%M") for c in timeline.columns]

            st.dataframe(
                timeline.style.map(color_status),
                use_container_width=True,
                height=min(200, len(timeline) * 35 + 50),
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
    status_counts = df["상태분류"].value_counts()

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

    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.multiselect(
            "상태 필터",
            options=df["상태분류"].unique().tolist(),
            default=df["상태분류"].unique().tolist(),
        )

    with col2:
        show_count = st.slider("표시 개수", 10, min(200, len(df)), min(50, len(df)), 10)

    filtered_df = df[df["상태분류"].isin(status_filter)]

    # 표시할 컬럼 (슬래시 포함 컬럼도 DataFrame 내부에서는 문제 없음)
    display_columns = [
        COL_CHARGER_ID, COL_COLLECTED_AT, "상태분류", COL_STATUS,
        COL_ERROR_STATE, "신호세기", "누적사용량", "급속/완속",
    ]
    display_columns = [col for col in display_columns if col in filtered_df.columns]

    display_df = (
        filtered_df[display_columns]
        .sort_values(COL_COLLECTED_AT, ascending=False)
        .head(show_count)
    )

    st.dataframe(
        display_df.style.map(
            color_status, subset=["상태분류"] if "상태분류" in display_df.columns else []
        ),
        use_container_width=True,
        height=400,
    )

    csv = filtered_df.to_csv(index=False).encode("utf-8-sig")
    safe_name = display_id.replace(":", "_").replace("/", "_").replace(" ", "_")
    st.download_button(
        label="📥 CSV 다운로드",
        data=csv,
        file_name=f"heartbeat_{safe_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


# ============================================================
# 메인 화면
# ============================================================

st.title("💓 Project HEARTBEAT")
st.caption("충전기 실시간 이력 관제 — 키워드 자유 검색 지원")

# 사이드바
st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# -------------------------------------------------------
# 검색 모드 선택
# -------------------------------------------------------
search_mode = st.sidebar.radio(
    "검색 방법",
    ["키워드 검색", "충전소ID 직접 입력", "사이트ID 직접 입력"],
    index=0,
)

df = pd.DataFrame()
display_id = ""

# -------------------------------------------------------
# 키워드 검색 (충전소명, 주소, 충전기ID, 제조사 등 자유 검색)
# -------------------------------------------------------
if search_mode == "키워드 검색":
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "충전소명, 주소, 충전기ID, 제조사 등\n"
        "**아무 단어**를 입력하세요."
    )

    keyword = st.sidebar.text_input(
        "검색어",
        placeholder="예: 인왕산, 종로, 서울, 급속 ...",
        key="keyword_input",
    )

    # 검색 인덱스 로드
    with st.spinner("검색 인덱스 로딩 중..."):
        index_df = load_search_index()

    if index_df.empty:
        st.error("검색 인덱스를 불러올 수 없습니다.")
        st.stop()

    if not keyword.strip():
        st.info("👈 왼쪽 검색창에 **충전소명**, **주소**, **충전기ID** 등 키워드를 입력하세요.")

        with st.expander("💡 검색 예시"):
            st.markdown(
                "- `인왕산` → 충전소명에 '인왕산'이 포함된 충전기\n"
                "- `종로` → 주소나 충전소명에 '종로'가 포함된 충전기\n"
                "- `1111057` → 충전기ID가 '1111057'로 시작하는 충전기\n"
                "- `서울 급속` → '서울' AND '급속' 모두 포함된 충전기\n"
                "- `LG` → 제조사가 LG인 충전기"
            )
        st.stop()

    # 검색 실행
    results = search_chargers(index_df, keyword)

    if results.empty:
        st.warning(f"'{keyword}'에 해당하는 충전기가 없습니다.")
        st.stop()

    st.sidebar.success(f"🔎 {len(results)}건 발견")

    # 검색 결과를 선택 가능한 목록으로 표시
    # 표시 텍스트 생성
    result_options = []
    for _, row in results.iterrows():
        cid = str(row[COL_CHARGER_ID])
        name = str(row.get(COL_STATION_NAME, ""))
        addr = str(row.get("주소1", ""))

        label = cid
        extras = []
        if name and name not in ["nan", "None", ""]:
            extras.append(name)
        if addr and addr not in ["nan", "None", ""]:
            extras.append(addr)
        if extras:
            label += f"  —  {' / '.join(extras)}"

        result_options.append(label)

    # selectbox 대신 radio 또는 selectbox
    selected_label = st.sidebar.selectbox(
        "검색 결과에서 선택",
        result_options,
        key="result_select",
    )

    # 선택된 항목에서 충전기ID 추출 (첫 공백 전 또는 ' — ' 전까지)
    selected_charger_id = selected_label.split("  —  ")[0].strip()

    with st.spinner(f"'{selected_charger_id}' 이력 조회 중..."):
        df = load_charger_history(selected_charger_id)
    display_id = selected_charger_id

# -------------------------------------------------------
# 충전소ID 직접 입력
# -------------------------------------------------------
elif search_mode == "충전소ID 직접 입력":
    station_id = st.sidebar.text_input("충전소ID", placeholder="예: 12345")

    if st.sidebar.button("🔍 검색", key="search_station"):
        if station_id:
            with st.spinner(f"충전소ID '{station_id}' 검색 중..."):
                df = load_group_history("충전소ID", station_id)
            display_id = f"충전소ID: {station_id}"
        else:
            st.warning("충전소ID를 입력하세요.")
            st.stop()
    else:
        st.info("👈 **충전소ID**를 입력하고 검색 버튼을 클릭하세요.")
        st.stop()

# -------------------------------------------------------
# 사이트ID 직접 입력
# -------------------------------------------------------
else:
    site_id = st.sidebar.text_input("사이트ID", placeholder="예: 67890")

    if st.sidebar.button("🔍 검색", key="search_site"):
        if site_id:
            with st.spinner(f"사이트ID '{site_id}' 검색 중..."):
                df = load_group_history("사이트ID", site_id)
            display_id = f"사이트ID: {site_id}"
        else:
            st.warning("사이트ID를 입력하세요.")
            st.stop()
    else:
        st.info("👈 **사이트ID**를 입력하고 검색 버튼을 클릭하세요.")
        st.stop()

# -------------------------------------------------------
# 데이터 렌더링
# -------------------------------------------------------
render_data(df, display_id)

# -------------------------------------------------------
# 푸터
# -------------------------------------------------------
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v2.0 (Keyword Search)")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
