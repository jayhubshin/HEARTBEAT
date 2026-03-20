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
# ⚡ 핵심 전략 변경:
#    전체 데이터를 먼저 로딩하지 않음 (타임아웃 원인)
#    → 사용자가 키워드 입력 → Supabase에서 ilike로 서버 필터링
#    → 소량의 결과만 가져옴 → 타임아웃 없음
# ============================================================

# 슬래시 없는 안전한 컬럼만 select에 사용
SAFE_COLUMNS = ",".join([
    COL_SITE_ID, COL_STATION_ID, COL_CHARGER_ID,
    COL_COLLECTED_AT, COL_STATUS, COL_ERROR_STATE,
    COL_STATION_NAME, "주소1", "상세주소", "제조사", "모델명"
])


@st.cache_data(ttl=300)
def search_by_keyword(keyword: str):
    """
    키워드로 서버에서 직접 검색.
    충전소명, 주소1, 충전기ID, 충전소ID, 사이트ID 각각에 대해
    ilike 검색 후 합침.
    """
    try:
        keyword = keyword.strip()
        if not keyword:
            return pd.DataFrame()

        pattern = f"%{keyword}%"
        all_data = []

        # 여러 컬럼에서 순차적으로 검색 (각각은 빠름)
        search_targets = [
            COL_STATION_NAME,  # 충전소명
            "주소1",            # 주소
            COL_CHARGER_ID,    # 충전기ID
            COL_STATION_ID,    # 충전소ID
            COL_SITE_ID,       # 사이트ID
            "상세주소",         # 상세주소
        ]

        for col in search_targets:
            try:
                response = (
                    supabase.table("status_history")
                    .select(SAFE_COLUMNS)
                    .ilike(col, pattern)
                    .order(COL_COLLECTED_AT, desc=True)
                    .limit(500)
                    .execute()
                )
                if response.data:
                    all_data.extend(response.data)
            except Exception:
                # 해당 컬럼 검색 실패 시 다음으로
                continue

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)

        # 중복 제거 (여러 컬럼에서 동일 행이 중복될 수 있음)
        if COL_CHARGER_ID in df.columns and COL_COLLECTED_AT in df.columns:
            df = df.drop_duplicates(
                subset=[COL_CHARGER_ID, COL_COLLECTED_AT], keep="first"
            )

        return df

    except APIError as e:
        st.error(f"❌ 검색 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame()


def build_site_list(df: pd.DataFrame) -> pd.DataFrame:
    """검색 결과에서 사이트 단위로 그룹핑"""
    if df.empty:
        return pd.DataFrame()

    charger_df = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="first").copy()

    # 그룹키: 사이트ID 우선, 없으면 충전소ID
    charger_df["_group_key"] = charger_df[COL_SITE_ID].astype(str)
    charger_df.loc[
        charger_df["_group_key"].isin(["nan", "None", ""]), "_group_key"
    ] = charger_df.loc[
        charger_df["_group_key"].isin(["nan", "None", ""]), COL_STATION_ID
    ].astype(str)

    clean = lambda s: "" if str(s) in ["nan", "None", ""] else str(s)

    site_groups = []
    for gk, grp in charger_df.groupby("_group_key"):
        first = grp.iloc[0]
        station_name = clean(first.get(COL_STATION_NAME, ""))
        station_id = clean(first.get(COL_STATION_ID, ""))
        site_id = clean(first.get(COL_SITE_ID, ""))
        addr = clean(first.get("주소1", ""))
        charger_count = len(grp)

        parts = []
        if station_name:
            parts.append(station_name)
        if addr:
            parts.append(addr)
        display_label = " / ".join(parts) if parts else gk

        site_groups.append({
            "_group_key": gk,
            "display_label": display_label,
            COL_STATION_NAME: station_name,
            COL_STATION_ID: station_id,
            COL_SITE_ID: site_id,
            "주소1": addr,
            "charger_count": charger_count,
        })

    return pd.DataFrame(site_groups)


@st.cache_data(ttl=300)
def load_site_history(site_id: str, station_id: str):
    """사이트 전체 이력 조회 — select(*) 사용 (eq 필터로 소량)"""
    try:
        if site_id:
            response = (
                supabase.table("status_history")
                .select("*")
                .eq(COL_SITE_ID, site_id)
                .order(COL_COLLECTED_AT, desc=True)
                .limit(1000)
                .execute()
            )
        elif station_id:
            response = (
                supabase.table("status_history")
                .select("*")
                .eq(COL_STATION_ID, station_id)
                .order(COL_COLLECTED_AT, desc=True)
                .limit(500)
                .execute()
            )
        else:
            return pd.DataFrame()

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
    """사이트 전체 대시보드"""

    if df.empty:
        st.warning(f"'{site_label}'에 해당하는 이력 데이터가 없습니다.")
        return

    df["상태분류"] = df.apply(categorize_status, axis=1)

    # ── 사이트 헤더 ──
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

    # ── 충전기별 현재 상태 ──
    unique_chargers = df[COL_CHARGER_ID].nunique()
    st.subheader(f"⚡ 사이트 내 충전기 현황 ({unique_chargers}대)")

    latest_per_charger = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="last").copy()

    status_summary = latest_per_charger["상태분류"].value_counts()
    num_cols = min(len(status_summary) + 1, 6)
    summary_cols = st.columns(num_cols)
    summary_cols[0].metric("전체", f"{unique_chargers}대")
    for i, (status, count) in enumerate(status_summary.items()):
        if i + 1 < num_cols:
            summary_cols[i + 1].metric(status, f"{count}대")

    display_cols_summary = [
        COL_CHARGER_ID, COL_STATION_NAME, COL_COLLECTED_AT,
        "상태분류", COL_STATUS, COL_ERROR_STATE
    ]
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

    # ── 개별 충전기 상세 ──
    st.subheader("🔎 개별 충전기 상세 이력")

    charger_list = sorted(df[COL_CHARGER_ID].unique().tolist())

    def charger_label(cid):
        match = latest_per_charger[latest_per_charger[COL_CHARGER_ID] == cid]
        if not match.empty:
            return f"{cid} — {match.iloc[0].get('상태분류', '')}"
        return cid

    selected_charger = st.selectbox(
        "충전기 선택", charger_list,
        format_func=charger_label, key="charger_detail_select",
    )

    charger_df = df[df[COL_CHARGER_ID] == selected_charger].copy()
    charger_latest = charger_df.iloc[-1]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("현재 상태", charger_latest["상태분류"])
    c2.metric("최종 수신", str(charger_latest[COL_COLLECTED_AT])[:19])
    c3.metric("에러 코드", charger_latest.get(COL_ERROR_STATE, "N/A"))
    c4.metric("기록 수", f"{len(charger_df)}건")

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

    # 개별 충전기 타임라인
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
            st.dataframe(timeline.style.map(color_status), use_container_width=True)
            if len(cdf_clean) > 30:
                st.caption(f"💡 최근 30건만 표시 (전체: {len(cdf_clean)}건)")
    except Exception as e:
        st.caption(f"타임라인 오류: {e}")

    st.divider()

    # ── 사이트 전체 타임라인 ──
    st.subheader("🗺️ 사이트 전체 타임라인")
    try:
        df["날짜"] = pd.to_datetime(df[COL_COLLECTED_AT], errors="coerce").dt.tz_localize(None)
        df_clean = df.dropna(subset=["날짜"])

        if len(df_clean) > 0:
            recent_times = sorted(df_clean["날짜"].unique())[-20:]
            recent_df = df_clean[df_clean["날짜"].isin(recent_times)]
            site_timeline = recent_df.pivot_table(
                index=COL_CHARGER_ID, columns="날짜",
                values="상태분류", aggfunc="first",
            )
            site_timeline.columns = [c.strftime("%m-%d %H:%M") for c in site_timeline.columns]
            st.dataframe(
                site_timeline.style.map(color_status),
                use_container_width=True,
                height=min(400, len(site_timeline) * 35 + 50),
            )
    except Exception as e:
        st.caption(f"사이트 타임라인 오류: {e}")

    st.divider()

    # ── 상태 분포 ──
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

    # ── 전체 이력 테이블 ──
    st.subheader("📋 전체 이력 데이터")

    col1, col2, col3 = st.columns(3)
    with col1:
        filter_charger = st.multiselect(
            "충전기 필터", options=charger_list,
            default=charger_list, key="filter_charger",
        )
    with col2:
        filter_status = st.multiselect(
            "상태 필터", options=df["상태분류"].unique().tolist(),
            default=df["상태분류"].unique().tolist(), key="filter_status",
        )
    with col3:
        show_count = st.slider(
            "표시 개수", 10, min(500, len(df)), min(100, len(df)), 10
        )

    filtered = df[
        (df[COL_CHARGER_ID].isin(filter_charger)) &
        (df["상태분류"].isin(filter_status))
    ]

    display_columns = [
        COL_CHARGER_ID, COL_COLLECTED_AT, "상태분류", COL_STATUS,
        COL_ERROR_STATE, "신호세기", "누적사용량",
    ]
    display_columns = [c for c in display_columns if c in filtered.columns]

    display_df = (
        filtered[display_columns]
        .sort_values(COL_COLLECTED_AT, ascending=False)
        .head(show_count)
    )

    st.dataframe(
        display_df.style.map(
            color_status,
            subset=["상태분류"] if "상태분류" in display_df.columns else []
        ),
        use_container_width=True, height=400,
    )

    csv = filtered.to_csv(index=False).encode("utf-8-sig")
    safe_name = site_label.replace(":", "_").replace("/", "_").replace(" ", "_")[:50]
    st.download_button(
        label="📥 CSV 다운로드", data=csv,
        file_name=f"heartbeat_{safe_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


# ============================================================
# 메인 화면
# ============================================================

st.title("💓 Project HEARTBEAT")
st.caption("충전기 실시간 이력 관제 — 키워드 검색 → 사이트 전체 조회")

st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)

# ── 검색 UI ──
st.sidebar.markdown("---")
st.sidebar.markdown(
    "**충전소명, 주소, 충전기ID** 등\n"
    "키워드를 입력하고 검색 버튼을 누르세요."
)

keyword = st.sidebar.text_input(
    "🔍 검색어",
    placeholder="예: 노원, 서울종로, 인왕산 ...",
    key="main_keyword",
)

search_clicked = st.sidebar.button("🔍 검색", key="search_btn", use_container_width=True)

# ── 세션 상태 관리 ──
if "search_results" not in st.session_state:
    st.session_state.search_results = None
    st.session_state.site_list = None
    st.session_state.last_keyword = ""

# 검색 실행
if search_clicked and keyword.strip():
    with st.spinner(f"'{keyword}' 검색 중..."):
        # 공백으로 분리된 키워드는 첫 번째 키워드로 서버 검색
        tokens = keyword.strip().split()
        primary_keyword = tokens[0]

        raw_results = search_by_keyword(primary_keyword)

        # 나머지 키워드로 Python에서 추가 필터링 (AND 검색)
        if not raw_results.empty and len(tokens) > 1:
            for token in tokens[1:]:
                # 모든 문자열 컬럼에서 검색
                mask = pd.Series([False] * len(raw_results), index=raw_results.index)
                for col in raw_results.columns:
                    if raw_results[col].dtype == object:
                        mask = mask | raw_results[col].astype(str).str.contains(
                            token, case=False, na=False
                        )
                raw_results = raw_results[mask]

        st.session_state.search_results = raw_results
        st.session_state.last_keyword = keyword

        if not raw_results.empty:
            st.session_state.site_list = build_site_list(raw_results)
        else:
            st.session_state.site_list = pd.DataFrame()

# 초기 안내 화면
if st.session_state.search_results is None:
    st.info("👈 왼쪽에서 **충전소명**, **주소**, **충전기ID** 등을 검색하세요.")

    with st.expander("💡 검색 방법", expanded=True):
        st.markdown(
            "| 입력 | 의미 |\n"
            "|------|------|\n"
            "| `노원` | 충전소명이나 주소에 '노원' 포함 |\n"
            "| `서울 종로` | '서울' AND '종로' 모두 포함 |\n"
            "| `인왕산` | 충전소명에 '인왕산' 포함 |\n"
            "| `1111057` | 충전기ID 일부 |\n"
        )

    st.markdown("---")
    st.markdown(
        "**작동 방식:** 검색어 입력 → 서버에서 직접 필터링 → "
        "충전소(사이트) 목록 표시 → 선택 시 같은 사이트 전체 충전기 이력을 조회합니다."
    )
    st.stop()

# 검색 결과 없음
if st.session_state.search_results is not None and st.session_state.search_results.empty:
    st.warning(f"'{st.session_state.last_keyword}'에 해당하는 결과가 없습니다.")
    st.info("다른 키워드로 검색해 보세요. 충전소명, 주소, 충전기ID 등 다양한 검색이 가능합니다.")
    st.stop()

# 검색 결과가 있을 때
site_list = st.session_state.site_list

if site_list is not None and not site_list.empty:
    st.sidebar.markdown(f"**검색 결과: {len(site_list)}곳**")

    result_options = []
    for _, row in site_list.iterrows():
        label = row["display_label"]
        count = row["charger_count"]
        sid = row.get(COL_SITE_ID, "")
        stid = row.get(COL_STATION_ID, "")
        tag = f"사이트:{sid}" if sid else f"충전소:{stid}"
        result_options.append(f"{label}  ({count}대) [{tag}]")

    selected_result_label = st.sidebar.selectbox(
        "충전소(사이트) 선택",
        result_options,
        key="site_select",
    )

    selected_idx = result_options.index(selected_result_label)
    selected_row = site_list.iloc[selected_idx]

    sel_site_id = selected_row.get(COL_SITE_ID, "")
    sel_station_id = selected_row.get(COL_STATION_ID, "")
    sel_display = selected_row["display_label"]

    # 사이트 이력 로드
    with st.spinner(f"'{sel_display}' 사이트 이력 조회 중..."):
        df = load_site_history(sel_site_id, sel_station_id)

    # 대시보드 렌더링
    render_site_dashboard(df, sel_display)

else:
    st.warning("검색 결과를 사이트별로 그룹핑할 수 없습니다.")

# ── 푸터 ──
st.sidebar.divider()
st.sidebar.caption("💓 Project HEARTBEAT v3.0 (Server-Side Search)")
st.sidebar.caption(f"마지막 업데이트: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
