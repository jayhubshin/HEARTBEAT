import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정 (본인의 URL/KEY 그대로 사용)
SUPABASE_URL = "https://gkwtucqymzkvpurcpihk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdrd3R1Y3F5bXprdnB1cmNwaWhrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM5MDIxNDcsImV4cCI6MjA4OTQ3ODE0N30.V0FnaZ-BaTEYUOKfzxvQ-T4Qk4E83LNIi4GflQsURUg"

# 3. 새로운 DB 구조에 맞춘 컬럼명 상수
COL_CHARGER_ID = "charger_id"
COL_SITE_ID = "site_id"
COL_STATION_ID = "station_id"
COL_STATION_NAME = "station_name"
COL_ADDR = "address1"
COL_ADDR_DTL = "address_detail"
COL_MODEL = "model_name"

COL_STATUS = "status"
COL_ERROR_STATE = "error_state"
COL_COLLECTED_AT = "collected_at"
COL_USAGE = "usage"

# 4. Supabase 연결
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    connection_status = "✅ 연결 성공"
except Exception as e:
    st.error(f"⚠️ 연결 오류: {e}")
    st.stop()


# ============================================================
# 데이터 로딩 및 병합 로직 (테이블 2개 분리 대응)
# ============================================================

@st.cache_data(ttl=300)
def search_by_keyword(keyword: str):
    """키워드로 charger_master 테이블에서 충전소/주소 검색"""
    try:
        keyword = keyword.strip()
        if not keyword:
            return pd.DataFrame()

        pattern = f"%{keyword}%"
        all_data = []

        # charger_master에서 검색할 타겟 컬럼
        search_targets = [
            COL_STATION_NAME, COL_ADDR, COL_CHARGER_ID,
            COL_STATION_ID, COL_SITE_ID, COL_ADDR_DTL
        ]

        for col in search_targets:
            try:
                response = (
                    supabase.table("charger_master")
                    .select("*")
                    .ilike(col, pattern)
                    .limit(500)
                    .execute()
                )
                if response.data:
                    all_data.extend(response.data)
            except Exception:
                continue

        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        # 중복 제거 (charger_id 기준)
        if COL_CHARGER_ID in df.columns:
            df = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="first")

        return df

    except APIError as e:
        st.error(f"❌ 검색 실패: {e}")
        return pd.DataFrame()


def build_site_list(df: pd.DataFrame) -> pd.DataFrame:
    """검색된 charger_master 데이터를 사이트 단위로 그룹핑"""
    if df.empty:
        return pd.DataFrame()

    df["_group_key"] = df[COL_SITE_ID].astype(str)
    # site_id가 없으면 station_id로 그룹핑
    df.loc[df["_group_key"].isin(["nan", "None", ""]), "_group_key"] = df.loc[
        df["_group_key"].isin(["nan", "None", ""]), COL_STATION_ID
    ].astype(str)

    clean = lambda s: "" if str(s) in ["nan", "None", ""] else str(s)

    site_groups = []
    for gk, grp in df.groupby("_group_key"):
        first = grp.iloc[0]
        station_name = clean(first.get(COL_STATION_NAME, ""))
        station_id = clean(first.get(COL_STATION_ID, ""))
        site_id = clean(first.get(COL_SITE_ID, ""))
        addr = clean(first.get(COL_ADDR, ""))
        charger_count = len(grp)

        parts = []
        if station_name: parts.append(station_name)
        if addr: parts.append(addr)
        display_label = " / ".join(parts) if parts else gk

        site_groups.append({
            "_group_key": gk,
            "display_label": display_label,
            COL_STATION_NAME: station_name,
            COL_STATION_ID: station_id,
            COL_SITE_ID: site_id,
            COL_ADDR: addr,
            "charger_count": charger_count,
        })

    return pd.DataFrame(site_groups)


@st.cache_data(ttl=300)
def load_site_history(site_id: str, station_id: str):
    """
    1. charger_master에서 해당 사이트의 전체 충전기 정보 가져오기
    2. status_history에서 해당 충전기들의 상태 이력 가져오기
    3. 두 데이터 병합(Merge)하여 반환
    """
    try:
        # 1. 마스터 정보 가져오기
        if site_id:
            master_res = supabase.table("charger_master").select("*").eq(COL_SITE_ID, site_id).execute()
        elif station_id:
            master_res = supabase.table("charger_master").select("*").eq(COL_STATION_ID, station_id).execute()
        else:
            return pd.DataFrame()

        df_master = pd.DataFrame(master_res.data)
        if df_master.empty:
            return pd.DataFrame()

        # 충전기 ID 목록 추출
        charger_ids = df_master[COL_CHARGER_ID].dropna().unique().tolist()
        if not charger_ids:
            return pd.DataFrame()

        # 2. 상태 이력 가져오기 (.in_ 필터 사용)
        history_res = (
            supabase.table("status_history")
            .select("*")
            .in_(COL_CHARGER_ID, charger_ids)
            .order(COL_COLLECTED_AT, desc=True)
            .limit(2000) # 필요시 조정
            .execute()
        )
        
        df_history = pd.DataFrame(history_res.data)

        # 3. 데이터 병합 (Merge)
        if df_history.empty:
            # 이력이 없어도 마스터 정보라도 띄우기 위해 병합
            merged_df = df_master.copy()
        else:
            merged_df = pd.merge(df_history, df_master, on=COL_CHARGER_ID, how="left")
            merged_df = merged_df.sort_values(COL_COLLECTED_AT, ascending=True).reset_index(drop=True)

        return merged_df

    except Exception as e:
        st.error(f"❌ 이력 조회/병합 실패: {e}")
        return pd.DataFrame()


# ============================================================
# 유틸리티 (상태 진단 및 색상 맵핑)
# ============================================================

def color_status(val):
    colors = {
        "🚨 임의OFF/방치(>7일)": "background-color: #8B0000; color: white; font-weight: bold;",
        "⚠️ 현장조치요망(2~7일)": "background-color: #FF8C00; color: white; font-weight: bold;",
        "⚠️ 연속대기(이상의심)": "background-color: #FFD700; color: black; font-weight: bold;",
        "⚫ 단기미수신": "background-color: #444444; color: white;",
        "🔴 점검중": "background-color: #EF553B; color: white;",
        "🔵 충전중": "background-color: #1F77B4; color: white;",
        "🟢 충전대기": "background-color: #00CC96; color: black;",
        "⚪ 기타": "background-color: #CCCCCC; color: black;",
    }
    return colors.get(val, "color: gray;")

def render_site_dashboard(df: pd.DataFrame, site_label: str):
    """사이트 전체 대시보드 렌더링"""
    if df.empty or COL_COLLECTED_AT not in df.columns:
        st.warning(f"'{site_label}'에 해당하는 상태 이력 데이터가 없습니다.")
        return

    # 날짜 변환 및 시간 기반 필터링
    df["날짜"] = pd.to_datetime(df[COL_COLLECTED_AT], errors="coerce").dt.tz_localize(None)
    now = pd.Timestamp.now().tz_localize(None)

    def diagnose_status(row):
        status = str(row.get(COL_STATUS, ""))
        error = str(row.get(COL_ERROR_STATE, ""))
        dt = row["날짜"]
        
        if pd.notna(dt):
            time_diff = now - dt
            if time_diff > pd.Timedelta(days=7):
                return "🚨 임의OFF/방치(>7일)"
            elif time_diff >= pd.Timedelta(days=2):
                return "⚠️ 현장조치요망(2~7일)"

        # 에러 상태나 고장 여부 확인 (0이 아니거나 None이 아니면 에러로 간주)
        if (error and error not in ["이상없음", "None", "", "null", "nan", "0"]) or "고장" in status or "점검" in status:
            return "🔴 점검중"
        elif "미수신" in status or "통신" in status:
            return "⚫ 단기미수신"
        elif "충전중" in status or "충전완료" in status:
            return "🔵 충전중"
        elif "대기" in status or "정상" in status:
            return "🟢 충전대기"
        else:
            return "⚪ 기타"

    df["상태분류"] = df.apply(diagnose_status, axis=1)

    # ── 사이트 헤더 ──
    st.subheader(f"📍 {site_label}")
    latest = df.iloc[-1]
    info_cols = st.columns(4)
    info_cols[0].info(f"**충전소명:** {latest.get(COL_STATION_NAME, '')}")
    info_cols[1].info(f"**주소:** {latest.get(COL_ADDR, '')}")
    info_cols[2].info(f"**충전소ID:** {latest.get(COL_STATION_ID, '')}")
    info_cols[3].info(f"**사이트ID:** {latest.get(COL_SITE_ID, '')}")
    st.divider()

    # ── 충전기별 현재 상태 (연속대기 로직 포함) ──
    unique_chargers = df[COL_CHARGER_ID].dropna().nunique()
    st.subheader(f"⚡ 사이트 내 충전기 현황 ({unique_chargers}대)")

    latest_per_charger = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="last").copy()

    # 최근 이력을 분석하여 연속 5회 '충전대기'인 경우 이상신호로 변경
    for cid in latest_per_charger[COL_CHARGER_ID]:
        charger_history = df[df[COL_CHARGER_ID] == cid].tail(5)
        if len(charger_history) >= 5 and all(charger_history["상태분류"] == "🟢 충전대기"):
            latest_per_charger.loc[latest_per_charger[COL_CHARGER_ID] == cid, "상태분류"] = "⚠️ 연속대기(이상의심)"

    status_summary = latest_per_charger["상태분류"].value_counts()
    num_cols = min(len(status_summary) + 1, 6)
    summary_cols = st.columns(num_cols)
    summary_cols[0].metric("전체", f"{unique_chargers}대")
    for i, (status, count) in enumerate(status_summary.items()):
        if i + 1 < num_cols:
            summary_cols[i + 1].metric(status, f"{count}대")

    display_cols_summary = [COL_CHARGER_ID, COL_STATION_NAME, COL_COLLECTED_AT, "상태분류", COL_STATUS, COL_ERROR_STATE]
    display_cols_summary = [c for c in display_cols_summary if c in latest_per_charger.columns]

    charger_table = latest_per_charger[display_cols_summary].sort_values(COL_CHARGER_ID).reset_index(drop=True)
    st.dataframe(charger_table.style.map(color_status, subset=["상태분류"]), use_container_width=True)
    st.divider()

    # ── 개별 충전기 상세 ──
    st.subheader("🔎 개별 충전기 상세 이력")
    charger_list = sorted(df[COL_CHARGER_ID].dropna().unique().tolist())
    
    def charger_label(cid):
        match = latest_per_charger[latest_per_charger[COL_CHARGER_ID] == cid]
        return f"{cid} — {match.iloc[0].get('상태분류', '')}" if not match.empty else cid

    if charger_list:
        selected_charger = st.selectbox("충전기 선택", charger_list, format_func=charger_label, key="charger_detail_select")
        charger_df = df[df[COL_CHARGER_ID] == selected_charger].copy()
        charger_latest = charger_df.iloc[-1]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("현재 상태", latest_per_charger[latest_per_charger[COL_CHARGER_ID] == selected_charger]["상태분류"].values[0])
        c2.metric("최종 수신", str(charger_latest.get(COL_COLLECTED_AT, ""))[:19])
        c3.metric("에러 코드", str(charger_latest.get(COL_ERROR_STATE, "N/A")))
        c4.metric("누적 사용량(usage)", str(charger_latest.get(COL_USAGE, "N/A")))

        with st.expander("🔧 마스터 정보 (상세)"):
            c_m1, c_m2 = st.columns(2)
            c_m1.write(f"**모델명:** {charger_latest.get(COL_MODEL, 'N/A')}")
            c_m1.write(f"**상세주소:** {charger_latest.get(COL_ADDR_DTL, 'N/A')}")
            c_m2.write(f"**메모:** {charger_latest.get('memo', 'N/A')}")

        # 개별 충전기 타임라인
        st.markdown("#### 🎛️ 시간대별 상태 변화")
        try:
            cdf_clean = charger_df.dropna(subset=["날짜"])
            if len(cdf_clean) > 0:
                tail = cdf_clean.tail(30)
                timeline = tail.set_index("날짜")[["상태분류"]].T
                timeline.columns = [c.strftime("%m-%d %H:%M") for c in timeline.columns]
                timeline.index = [selected_charger]
                st.dataframe(timeline.style.map(color_status), use_container_width=True)
        except Exception as e:
            st.caption(f"타임라인 오류: {e}")
    st.divider()

    # ── 사이트 전체 타임라인 ──
    st.subheader("🗺️ 사이트 전체 타임라인")
    try:
        df_clean = df.dropna(subset=["날짜"])
        if len(df_clean) > 0:
            recent_times = sorted(df_clean["날짜"].unique())[-20:]
            recent_df = df_clean[df_clean["날짜"].isin(recent_times)]
            site_timeline = recent_df.pivot_table(index=COL_CHARGER_ID, columns="날짜", values="상태분류", aggfunc="first")
            site_timeline.columns = [c.strftime("%m-%d %H:%M") for c in site_timeline.columns]
            st.dataframe(site_timeline.style.map(color_status), use_container_width=True)
    except Exception as e:
        st.caption(f"사이트 타임라인 오류: {e}")


# ============================================================
# 메인 화면
# ============================================================
st.title("💓 Project HEARTBEAT")
st.caption("충전기 실시간 이력 관제 — 키워드 검색 → DB Join (Master + History) → 렌더링")

st.sidebar.header("📡 관제 타겟")
st.sidebar.caption(connection_status)
st.sidebar.markdown("---")
st.sidebar.markdown("**충전소명, 주소, 충전기ID** 등\n키워드를 입력하고 검색 버튼을 누르세요.")

keyword = st.sidebar.text_input("🔍 검색어", placeholder="예: 노원, 서울종로, 인왕산 ...", key="main_keyword")
search_clicked = st.sidebar.button("🔍 검색", key="search_btn", use_container_width=True)

if "search_results" not in st.session_state:
    st.session_state.search_results = None
    st.session_state.site_list = None
    st.session_state.last_keyword = ""

if search_clicked and keyword.strip():
    with st.spinner(f"'{keyword}' 마스터 데이터 검색 중..."):
        tokens = keyword.strip().split()
        primary_keyword = tokens[0]
        # 1. 마스터 테이블에서 기본 검색
        raw_results = search_by_keyword(primary_keyword)

        # 2. 다중 키워드(띄어쓰기) Python 필터링
        if not raw_results.empty and len(tokens) > 1:
            for token in tokens[1:]:
                mask = pd.Series([False] * len(raw_results), index=raw_results.index)
                for col in raw_results.columns:
                    if raw_results[col].dtype == object:
                        mask = mask | raw_results[col].astype(str).str.contains(token, case=False, na=False)
                raw_results = raw_results[mask]

        st.session_state.search_results = raw_results
        st.session_state.last_keyword = keyword

        if not raw_results.empty:
            st.session_state.site_list = build_site_list(raw_results)
        else:
            st.session_state.site_list = pd.DataFrame()

if st.session_state.search_results is None:
    st.info("👈 왼쪽에서 **충전소명**, **주소**, **충전기ID** 등을 검색하세요.")
    st.stop()

if st.session_state.search_results is not None and st.session_state.search_results.empty:
    st.warning(f"'{st.session_state.last_keyword}'에 해당하는 마스터 정보가 없습니다.")
    st.stop()

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

    selected_result_label = st.sidebar.selectbox("충전소(사이트) 선택", result_options, key="site_select")
    selected_idx = result_options.index(selected_result_label)
    selected_row = site_list.iloc[selected_idx]

    sel_site_id = selected_row.get(COL_SITE_ID, "")
    sel_station_id = selected_row.get(COL_STATION_ID, "")
    sel_display = selected_row["display_label"]

    with st.spinner(f"'{sel_display}' 상태 이력 연동 중..."):
        # 여기서 charger_master 와 status_history 가 병합된 최종 df가 생성됨
        df = load_site_history(sel_site_id, sel_station_id)

    render_site_dashboard(df, sel_display)
else:
    st.warning("검색 결과를 사이트별로 그룹핑할 수 없습니다.")
