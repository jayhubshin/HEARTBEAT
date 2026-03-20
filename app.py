import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg"

# 3. 컬럼명 상수
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


# ============================================================
# 데이터 로딩 함수
# ============================================================

@st.cache_data(ttl=600)
def load_search_index():
    """검색 인덱스 구축 — 충전소(사이트) 단위로 그룹핑"""
    try:
        response = (
            supabase.table("status_history")
            .select("*")
            .order(COL_COLLECTED_AT, desc=True)
            .limit(5000)
            .execute()
        )
        df = pd.DataFrame(response.data)
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # --- 충전기 단위 인덱스 (중복 제거) ---
        charger_df = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="first").copy()

        # --- 충전소(사이트) 단위 인덱스 ---
        # 사이트ID가 같으면 같은 장소로 묶는다
        # 사이트ID가 없으면 충전소ID 기준으로 묶는다
        charger_df["_group_key"] = charger_df[COL_SITE_ID].astype(str).replace(
            ["nan", "None", ""], pd.NA
        )
        charger_df["_group_key"] = charger_df["_group_key"].fillna(
            charger_df[COL_STATION_ID].astype(str)
        )

        # 충전소(사이트) 단위 요약
        site_groups = []
        for group_key, group in charger_df.groupby("_group_key"):
            first = group.iloc[0]
            station_name = str(first.get(COL_STATION_NAME, ""))
            station_id = str(first.get(COL_STATION_ID, ""))
            site_id = str(first.get(COL_SITE_ID, ""))
            addr = str(first.get("주소1", ""))
            addr_detail = str(first.get("상세주소", ""))
            maker = str(first.get("제조사", ""))
            charger_count = len(group)
            charger_ids = ", ".join(group[COL_CHARGER_ID].astype(str).tolist())

            # 검색용 통합 텍스트
            parts = [station_name, station_id, site_id, addr, addr_detail, maker, charger_ids]
            search_text = " ".join(
                p for p in parts if p and p not in ["nan", "None", ""]
            ).lower()

            # 표시 텍스트
            display_parts = []
            if station_name and station_name not in ["nan", "None", ""]:
                display_parts.append(station_name)
            if addr and addr not in ["nan", "None", ""]:
                display_parts.append(addr)
            display_label = " / ".join(display_parts) if display_parts else group_key

            site_groups.append({
                "_group_key": group_key,
                "display_label": display_label,
                COL_STATION_NAME: station_name if station_name not in ["nan", "None", ""] else "",
                COL_STATION_ID: station_id if station_id not in ["nan", "None", ""] else "",
                COL_SITE_ID: site_id if site_id not in ["nan", "None", ""] else "",
                "주소1": addr if addr not in ["nan", "None", ""] else "",
                "charger_count": charger_count,
                "charger_ids": charger_ids,
                "_search_text": search_text,
            })

        site_df = pd.DataFrame(site_groups)
        return site_df, charger_df

    except APIError as e:
        st.error(f"❌ 검색 인덱스 로딩 실패: {e}")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame(), pd.DataFrame()


def keyword_search(site_df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    """키워드 AND 검색"""
    if site_df.empty or not keyword.strip():
        return pd.DataFrame()

    tokens = keyword.strip().lower().split()
    mask = pd.Series([True] * len(site_df), index=site_df.index)
    for token in tokens:
        mask = mask & site_df["_search_text"].str.contains(token, na=False)

    return site_df[mask].copy()


@st.cache_data(ttl=300)
def load_site_history(site_id: str, station_id: str):
    """
    사이트 전체 이력 조회.
    사이트ID가 있으면 사이트ID로, 없으면 충전소ID로 조회.
    """
    try:
        if site_id and site_id not in ["nan", "None", ""]:
            response = (
                supabase.table("status_history")
                .select("*")
                .eq(COL_SITE_ID, site_id)
                .order(COL_COLLECTED_AT, desc=True)
                .limit(1000)
                .execute()
            )
        else:
            response = (
                supabase.table("status_history")
                .select("*")
                .eq(COL_STATION_ID, station_id)
                .order(COL_COLLECTED_AT, desc=True)
                .limit(500)
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
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_charger_history(charger_id: str):
    """단일 충전기 이력 조회"""
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
    except Exception as e:
        st.error(f"❌ 이력 조회 실패: {e}")
        return pd.DataFrame()


# ============================================================
# 유틸리티
# ============================================================

def categorize_status(row):
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
    colors = {
        "⚫ 미수신": "background-color: #444444; color: white;",
        "🔴 점검중": "background-color: #EF553B; color: white;",
        "🔵 충전중": "background-color: #1F77B4; color: white;",
        "🟢 충전대기": "background-color: #00CC96; color: black;",
        "⚪ 기타": "background-color: #CCCCCC; color: black;",
    }
    return colors.get(val, "color: gray;")


def render_site_dashboard(df: pd.DataFrame, site_label: str):
    """사이트 전체 대시보드 렌더링"""

    if df.empty:
        st.warning(f"'{site_label}'에 해당하는 데이터가 없습니다.")
        return

    df["상태분류"] = df.apply(categorize_status, axis=1)

    # -------------------------------------------------------
    # 사이트 헤더
    # -------------------------------------------------------
    st.subheader(f"📍 {site_label}")

    latest = df.iloc[-1]
    info_cols = st.columns(4)
    with info_cols[0]:
        val = str(latest.get(COL_STATION_NAME, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**충전소명:** {val}")
    with info_cols[1]:
        val = str(latest.get("주소1", ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**주소:** {val}")
    with info_cols[2]:
        val = str(latest.get(COL_STATION_ID, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**충전소ID:** {val}")
    with info_cols[3]:
        val = str(latest.get(COL_SITE_ID, ""))
        if val and val not in ["nan", "None", ""]:
            st.info(f"**사이트ID:** {val}")

    st.divider()

    # -------------------------------------------------------
    # 충전기별 현재 상태 요약 (사이트 내 전체)
    # -------------------------------------------------------
    unique_chargers = df[COL_CHARGER_ID].nunique()
    st.subheader(f"⚡ 사이트 내 충전기 현황 ({unique_chargers}대)")

    # 충전기별 최신 상태
    latest_per_charger = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="last").copy()

    # 상태 요약 카드
    status_summary = latest_per_charger["상태분류"].value_counts()
    summary_cols = st.columns(len(status_summary) + 1)
    summary_cols[0].metric("전체", f"{unique_chargers}대")
    for i, (status, count) in enumerate(status_summary.items(), 1):
        summary_cols[i].metric(status, f"{count}대")

    # 충전기 목록 테이블
    display_cols_summary = [COL_CHARGER_ID, COL_STATION_NAME, COL_COLLECTED_AT,
                            "상태분류", COL_STATUS, COL_ERROR_STATE]
    display_cols_summary = [c for c in display_cols_summary if c in latest_per_charger.columns]

    charger_table = latest_per_charger[display_cols_summary].sort_values(
        COL_CHARGER_ID
    ).reset_index(drop=True)

    st.dataframe(
        charger_table.style.map(color_status, subset=["상태분류"]),
        use_container_width=True,
        height=min(400, len(charger_table) * 35 + 50),
    )

    st.divider()

    # -------------------------------------------------------
    # 개별 충전기 상세 보기 (탭)
    # -------------------------------------------------------
    st.subheader("🔎 개별 충전기 상세 이력")

    charger_list = sorted(df[COL_CHARGER_ID].unique().tolist())

    selected_charger = st.selectbox(
        "충전기 선택",
        charger_list,
        format_func=lambda cid: f"{cid} — {latest_per_charger[latest_per_charger[COL_CHARGER_ID] == cid].iloc[0].get('상태분류', '')}",
        key="charger_detail_select",
    )

    charger_df = df[df[COL_CHARGER_ID] == selected_charger].copy()
    charger_latest = charger_df.iloc[-1]

    # 상태 메트릭
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재 상태", charger_latest["상태분류"])
    c2.metric("최종 수신", str(charger_latest[COL_COLLECTED_AT])[:19])
    c3.metric("에러 코드", charger_latest.get(COL_ERROR_STATE, "N/A"))
    c4.metric("기록 수", f"{len(charger_df)}건")

    # 상세 스펙
    with st.expander("🔧 충전기 상세 정보"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("**기본 스펙**")
            st.write(f"- 충전기ID: {charger_latest.get(COL_CHARGER_ID, 'N/A')}")
            st.write(f"- 제조사: {charger_latest.get('제조사', 'N/A')}")
            st.write(f"- 모델명: {charger_latest.get('모델명', 'N/A')}")
            st.write(f"- 충전기용량: {charger_latest.get('충전기용량', 'N/A')}")
            st.write(f"- 급속/완속: {charger_latest.get('급속/완속', 'N/A')}")
        with col2:
            st.write("**운영 정보**")
            st.write(f"- 충전소 상태: {charger_latest.get('충전소 상태', 'N/A')}")
            st.write(f"- 신호세기: {charger_latest.get('신호세기', 'N/A')}")
            st.write(f"- 누적사용량: {charger_latest.get('누적사용량', 'N/A')} kWh")
            st.write(f"- 사용여부: {charger_latest.get('사용여부', 'N/A')}")
        with col3:
            st.write("**위치 정보**")
            st.write(f"- 주소: {charger_latest.get('주소1', 'N/A')}")
            st.write(f"- 상세주소: {charger_latest.get('상세주소', 'N/A')}")
            st.write(f"- 설치업체: {charger_latest.get('설치업체', 'N/A')}")

    # 타임라인
    st.markdown("#### 🎛️ 시간대별 상태 변화")
    try:
        charger_df["날짜"] = pd.to_datetime(
            charger_df[COL_COLLECTED_AT], errors="coerce"
        ).dt.tz_localize(None)
        cdf_clean = charger_df.dropna(subset=["날짜"])

        if len(cdf_clean) > 0:
            tail = cdf_clean.tail(30)
            timeline = tail.set_index("날짜")[["상태분류"]].T
            timeline.columns = [c.strftime("%m-%d %H:%M") for c in timeline.columns]
            timeline.index = [selected_charger]

            st.dataframe(
                timeline.style.map(color_status),
                use_container_width=True,
            )
            if len(cdf_clean) > 30:
                st.caption(f"💡 최근 30건만 표시 (전체: {len(cdf_clean)}건)")
    except Exception as e:
        st.caption(f"타임라인 오류: {e}")

    st.divider()

    # -------------------------------------------------------
    # 사이트 전체 타임라인 (모든 충전기 한눈에)
    # -------------------------------------------------------
    st.subheader("🗺️ 사이트 전체 타임라인")

    try:
        df["날짜"] = pd.to_datetime(df[COL_COLLECTED_AT], errors="coerce").dt.tz_localize(None)
        df_clean = df.dropna(subset=["날짜"])

        if len(df_clean) > 0:
            # 최근 시간 기준 상위 20개 시점
            recent_times = sorted(df_clean["날짜"].unique())[-20:]
            recent_df = df_clean[df_clean["날짜"].isin(recent_times)]

            site_timeline = recent_df.pivot_table(
                index=COL_CHARGER_ID,
                columns="날짜",
                values="상태분류",
                aggfunc="first",
            )
            site_timeline.columns = [c.strftime("%m-%d %H:%M") for c in site_timeline.columns]

            st.dataframe(
                site_timeline.style.map(color_status),
                use_container_width=True,
                height=min(400, len(site_timeline) * 35 + 50),
            )
        else:
            st.warning("유효한 시간 데이터가 없습니다.")
    except Exception as e:
        st.caption(f"사이트 타임라인 오류: {e}")

    st.divider()

    # -------------------------------------------------------
    # 상태 분포
    # -------------------------------------------------------
    st.subheader("📊 상태 분포")
    status_counts = df["상태분류"].value_counts()

    col1, col2 = st.columns([2, 1])
    with col1:
        st.bar_chart(status_counts)
    with col2:
        for status, count in status_counts.items():
            pct = (count / len(df)) * 100
            st.metric(status, f"{count}건", f"{pct:.1f}%")

    st.divider()

    # -------------------------------------------------------
    # 전체 이력 테이블
    # -------------------------------------------------------
    st.subheader("📋 전체 이력 데이터")

    col1, col2, col3 = st.columns(3)
    with col1:
        filter_charger = st.multiselect(
            "충전기 필터",
            options=charger_list,
            default=charger_list,
            key="filter_charger",
        )
    with col2:
        filter_status = st.multiselect(
            "상태 필터",
            options=df["상태분류"].unique().tolist(),
            default=df["상태분류"].unique().tolist(),
            key="filter_status",
        )
    with col3:
        show_count = st.slider("표시 개수", 10, min(500, len(df)), min(100, len(df)), 10)

    filtered = df[
        (df[COL_CHARGER_ID].isin(filter_charger)) &
        (df["상태분류"].isin(filter_status))
    ]

    display_columns = [
        COL_CHARGER_ID, COL_COLLECTED_AT, "상태분류", COL_STATUS,
        COL_ERROR_STATE, "신호세기", "누적사용량", "급속/완속",
    ]
    display_columns = [c for c in display_columns if c in filtered.columns]

    display_df = (
        filtered[display_columns]
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

    # CSV 다운로드
    csv = filtered.to_csv(index=False).encode("utf-8-sig")
    safe_name = site_label.replace(":", "_").replace("/", "_").replace(" ", "_")[:50]
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
st.caption("충전기 실시간 이력 관제 — 키워드 검색 → 사이트 전체 조회")

# 사이드바
st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# -------------------------------------------------------
# 검색 인덱스 로드 (한 번만)
# -------------------------------------------------------
with st.spinner("검색 인덱스 로딩 중..."):
    site_df, charger_idx_df = load_search_index()

if site_df.empty:
    st.error("검색 인덱스를 불러올 수 없습니다. Supabase 연결을 확인하세요.")
    st.stop()

st.sidebar.success(f"충전소(사이트) {len(site_df)}곳 로드됨")

# -------------------------------------------------------
# 키워드 검색
# -------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.markdown("**충전소명, 주소, 충전기ID** 등\n아무 단어를 입력하세요.")

keyword = st.sidebar.text_input(
    "🔍 검색어",
    placeholder="예: 노원, 서울 종로, 인왕산, 급속 ...",
    key="main_keyword",
)

if not keyword.strip():
    st.info("👈 왼쪽 검색창에 **충전소명**, **주소**, **충전기ID** 등 키워드를 입력하세요.")

    with st.expander("💡 검색 예시", expanded=True):
        st.markdown(
            "| 입력 | 의미 |\n"
            "|------|------|\n"
            "| `노원` | 충전소명이나 주소에 '노원'이 포함된 사이트 |\n"
            "| `서울 종로` | '서울' AND '종로' 모두 포함 |\n"
            "| `인왕산 아이파크` | 충전소명 검색 |\n"
            "| `1111057` | 충전기ID 일부로 검색 |\n"
            "| `LG 급속` | 제조사 + 타입 조합 검색 |"
        )

    st.stop()

# -------------------------------------------------------
# 검색 실행
# -------------------------------------------------------
results = keyword_search(site_df, keyword)

if results.empty:
    st.warning(f"'{keyword}'에 해당하는 충전소/사이트가 없습니다.")
    st.stop()

st.sidebar.markdown(f"**검색 결과: {len(results)}곳**")

# -------------------------------------------------------
# 검색 결과 목록 (사이드바)
# -------------------------------------------------------
result_options = []
for _, row in results.iterrows():
    label = row["display_label"]
    count = row["charger_count"]
    sid = row.get(COL_SITE_ID, "")
    stid = row.get(COL_STATION_ID, "")

    tag = f"[사이트:{sid}]" if sid else f"[충전소:{stid}]"
    result_options.append(f"{label}  ({count}대) {tag}")

selected_result_label = st.sidebar.selectbox(
    "충전소(사이트) 선택",
    result_options,
    key="site_select",
)

# 선택된 인덱스로부터 사이트ID/충전소ID 추출
selected_idx = result_options.index(selected_result_label)
selected_row = results.iloc[selected_idx]

sel_site_id = selected_row.get(COL_SITE_ID, "")
sel_station_id = selected_row.get(COL_STATION_ID, "")
sel_display = selected_row["display_label"]

# -------------------------------------------------------
# 데이터 로드 (사이트 전체)
# -------------------------------------------------------
with st.spinner(f"'{sel_display}' 사이트 이력 조회 중..."):
    df = load_site_history(sel_site_id, sel_station_id)

# -------------------------------------------------------
# 대시보드 렌더링
# -------------------------------------------------------
render_site_dashboard(df, sel_display)

# -------------------------------------------------------
# 푸터
# -------------------------------------------------------
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v2.1 (Site-Wide View)")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
