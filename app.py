import streamlit as st
import pandas as pd
from supabase import create_client, Client
from postgrest.exceptions import APIError
from datetime import datetime, timedelta

# 1. 페이지 설정
st.set_page_config(page_title="Project HEARTBEAT | Live", page_icon="💓", layout="wide")

# 2. Supabase 설정
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
# 데이터 검색 및 기준 시간 조회 로직
# ============================================================

@st.cache_data(ttl=60)
def get_db_last_update_time():
    """DB(status_history) 전체에서 가장 최신 수집날짜를 조회하여 기준 시간으로 설정"""
    try:
        response = (
            supabase.table("status_history")
            .select(COL_COLLECTED_AT)
            .order(COL_COLLECTED_AT, desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return pd.to_datetime(response.data[0][COL_COLLECTED_AT]).tz_localize(None)
    except Exception:
        pass
    # DB 조회가 실패하거나 데이터가 아예 없을 경우에만 서버 현재 시간 사용
    return pd.Timestamp.now().tz_localize(None)


@st.cache_data(ttl=300)
def search_by_keyword(keyword: str):
    """키워드로 charger_master 테이블에서 유연한 단어 검색"""
    try:
        tokens = [t.strip() for t in keyword.split() if t.strip()]
        if not tokens:
            return pd.DataFrame()

        search_cols = [COL_STATION_NAME, COL_ADDR, COL_ADDR_DTL, COL_SITE_ID, COL_STATION_ID]
        primary_keyword = max(tokens, key=len)
        pattern = f"%{primary_keyword}%"

        or_query = (
            f"{COL_STATION_NAME}.ilike.{pattern},"
            f"{COL_ADDR}.ilike.{pattern},"
            f"{COL_ADDR_DTL}.ilike.{pattern},"
            f"{COL_SITE_ID}.ilike.{pattern},"
            f"{COL_STATION_ID}.ilike.{pattern}"
        )

        response = (
            supabase.table("charger_master")
            .select("*")
            .or_(or_query)
            .limit(3000)
            .execute()
        )

        df = pd.DataFrame(response.data)
        if df.empty:
            return df

        for col in search_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna("")

        other_tokens = [t for t in tokens if t != primary_keyword]
        for token in other_tokens:
            mask = pd.Series(False, index=df.index)
            for col in search_cols:
                if col in df.columns:
                    mask |= df[col].str.contains(token, case=False, na=False)
            df = df[mask]

        if COL_CHARGER_ID in df.columns:
            df = df.drop_duplicates(subset=[COL_CHARGER_ID], keep="first")

        return df

    except APIError as e:
        st.error(f"❌ 검색 실패: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ 시스템 오류: {e}")
        return pd.DataFrame()


def build_site_list(df: pd.DataFrame) -> pd.DataFrame:
    """검색된 데이터를 사이트 단위로 그룹핑"""
    if df.empty:
        return pd.DataFrame()

    df["_group_key"] = df[COL_SITE_ID].astype(str)
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
    """마스터 정보와 상태 이력을 병합"""
    try:
        if site_id:
            master_res = supabase.table("charger_master").select("*").eq(COL_SITE_ID, site_id).execute()
        elif station_id:
            master_res = supabase.table("charger_master").select("*").eq(COL_STATION_ID, station_id).execute()
        else:
            return pd.DataFrame()

        df_master = pd.DataFrame(master_res.data)
        if df_master.empty:
            return pd.DataFrame()

        charger_ids = df_master[COL_CHARGER_ID].dropna().unique().tolist()
        if not charger_ids:
            return pd.DataFrame()

        history_res = (
            supabase.table("status_history")
            .select("*")
            .in_(COL_CHARGER_ID, charger_ids)
            .order(COL_COLLECTED_AT, desc=True)
            .limit(2000)
            .execute()
        )
        
        df_history = pd.DataFrame(history_res.data)

        if df_history.empty:
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

def color_raw_status(val):
    val = str(val)
    if "미수신" in val or "통신" in val:
        return "background-color: #444444; color: white;"
    elif "고장" in val or "점검" in val or "에러" in val:
        return "background-color: #EF553B; color: white;"
    elif "충전중" in val or "충전완료" in val:
        return "background-color: #1F77B4; color: white;"
    elif "대기" in val or "정상" in val:
        return "background-color: #00CC96; color: black;"
    elif val in ["nan", "None", ""]:
        return "background-color: transparent;"
    else:
        return "background-color: #CCCCCC; color: black;"


def render_site_dashboard(df: pd.DataFrame, site_label: str):
    """선택된 사이트 대시보드 렌더링"""
    if df.empty or COL_COLLECTED_AT not in df.columns:
        st.warning(f"'{site_label}'에 해당하는 상태 이력 데이터가 없습니다.")
        return

    df["날짜"] = pd.to_datetime(df[COL_COLLECTED_AT], errors="coerce").dt.tz_localize(None)
    
    # [핵심 로직 변경] DB의 가장 최신 시간 불러오기
    db_last_time = get_db_last_update_time()

    def diagnose_status(row):
        status = str(row.get(COL_STATUS, ""))
        error = str(row.get(COL_ERROR_STATE, ""))
        dt = row["날짜"]
        
        if pd.notna(dt):
            # 현재 시간이 아닌 DB 마지막 업데이트 기준 시간과 차이 계산
            time_diff = db_last_time - dt 
            if time_diff > pd.Timedelta(days=7):
                return "🚨 임의OFF/방치(>7일)"
            elif time_diff >= pd.Timedelta(days=2):
                return "⚠️ 현장조치요망(2~7일)"

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

    # ── 사이트 전체 타임라인 (클릭 이벤트 적용) ──
    st.subheader("🗺️ 사이트 전체 타임라인 (3시간 기준)")
    st.markdown("👇 전체 충전기의 흐름입니다. **상세 이력을 확인할 충전기 행(Row)을 클릭**하세요.")
    
    selected_charger = None
    
    try:
        df_clean = df.dropna(subset=["날짜"]).copy()
        if len(df_clean) > 0:
            df_clean["시간대"] = df_clean["날짜"].dt.round("3H")
            df_3h = df_clean.drop_duplicates(subset=[COL_CHARGER_ID, "시간대"], keep="last")
            
            recent_times = sorted(df_3h["시간대"].unique())[-20:]
            recent_df = df_3h[df_3h["시간대"].isin(recent_times)]
            
            site_timeline = recent_df.pivot_table(index=COL_CHARGER_ID, columns="시간대", values=COL_STATUS, aggfunc="last")
            site_timeline.columns = [c.strftime("%m-%d %H:%M") for c in site_timeline.columns]
            site_timeline = site_timeline.fillna("")
            
            # [핵심] 타임라인 데이터프레임 클릭 활성화
            timeline_event = st.dataframe(
                site_timeline.style.map(color_raw_status), 
                use_container_width=True,
                on_select="rerun",           # 선택 시 화면 새로고침
                selection_mode="single-row"  # 단일 행 선택
            )
            
            # 클릭된 행의 인덱스를 통해 충전기 ID 추출
            if timeline_event.selection.rows:
                selected_idx = timeline_event.selection.rows[0]
                selected_charger = site_timeline.index[selected_idx]
            
    except Exception as e:
        st.caption(f"사이트 타임라인 오류: {e}")
    st.divider()

    # ── 개별 충전기 상세 ──
    st.subheader("🔎 개별 충전기 상세 이력")
    
    if selected_charger:
        st.success(f"✅ 선택된 충전기: **{selected_charger}**")
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
        st.markdown("#### 🎛️ 선택된 충전기 상태 변화 (3시간 기준)")
        try:
            cdf_clean = charger_df.dropna(subset=["날짜"]).copy()
            if len(cdf_clean) > 0:
                cdf_clean["시간대"] = cdf_clean["날짜"].dt.round("3H")
                cdf_3h = cdf_clean.drop_duplicates(subset=["시간대"], keep="last")
                
                tail = cdf_3h.tail(20) # 최근 20개
                
                timeline = tail.set_index("시간대")[[COL_STATUS]].T
                timeline.columns = [c.strftime("%m-%d %H:%M") for c in timeline.columns]
                timeline.index = [selected_charger]
                st.dataframe(timeline.style.map(color_raw_status), use_container_width=True)
        except Exception as e:
            st.caption(f"타임라인 오류: {e}")
    else:
        st.info("👆 위 타임라인 표에서 상세조회할 **충전기를 마우스로 클릭**해 주세요.")


# ============================================================
# 메인 화면 (오른쪽 프레임) 및 사이드바 (왼쪽 프레임) 레이아웃
# ============================================================
st.sidebar.title("💓 HEARTBEAT")
st.sidebar.header("📡 관제 타겟 검색")
st.sidebar.caption(connection_status)

# DB 마지막 업데이트 시간 표시
global_db_last_time = get_db_last_update_time()
st.sidebar.caption(f"🕒 DB 최종 수신: {global_db_last_time.strftime('%Y-%m-%d %H:%M')}")
st.sidebar.markdown("---")
st.sidebar.markdown("**충전소명, 주소, 사이트명** 등\n단어를 띄어쓰기로 조합하여 유연하게 검색하세요.")

# 검색창은 사이드바 유지
keyword = st.sidebar.text_input("🔍 검색어", placeholder="예: 서울 아파트, 노원 에버온 ...", key="main_keyword")
search_clicked = st.sidebar.button("🔍 검색", key="search_btn", use_container_width=True)

if "search_results" not in st.session_state:
    st.session_state.search_results = None
    st.session_state.site_list = None
    st.session_state.last_keyword = ""

if search_clicked and keyword.strip():
    with st.spinner(f"'{keyword}' 검색 중..."):
        raw_results = search_by_keyword(keyword)
        
        st.session_state.search_results = raw_results
        st.session_state.last_keyword = keyword

        if not raw_results.empty:
            st.session_state.site_list = build_site_list(raw_results)
        else:
            st.session_state.site_list = pd.DataFrame()

# ── 메인 화면 (오른쪽 프레임) ──
st.title("📊 사이트 통합 관제 대시보드")

if st.session_state.search_results is None:
    st.info("👈 왼쪽 사이드바에서 **충전소명**, **주소**, **사이트명** 등을 띄어쓰기로 조합하여 검색해 보세요.")
    st.stop()

if st.session_state.search_results is not None and st.session_state.search_results.empty:
    st.warning(f"'{st.session_state.last_keyword}'에 해당하는 마스터 정보가 없습니다.")
    st.stop()

site_list = st.session_state.site_list

# 검색 결과 표(Table)를 메인 화면 상단에 출력하고 클릭 이벤트 감지
if site_list is not None and not site_list.empty:
    st.success(f"✅ 총 **{len(site_list)}**곳의 사이트가 검색되었습니다.")
    
    # 표(Dataframe) 구성을 위해 컬럼명 예쁘게 변경
    display_site_df = site_list.rename(columns={
        COL_STATION_NAME: "충전소명",
        COL_ADDR: "주소",
        "charger_count": "충전기 대수",
        COL_SITE_ID: "사이트ID",
        COL_STATION_ID: "충전소ID"
    })[["충전소명", "주소", "충전기 대수", "사이트ID", "충전소ID"]]

    st.markdown("#### 📋 검색된 사이트 목록 (👇 원하는 행을 마우스로 클릭하세요!)")
    
    # 표 클릭(Selection) 이벤트 활성화
    selection_event = st.dataframe(
        display_site_df, 
        use_container_width=True, 
        hide_index=True,
        on_select="rerun",           # 클릭 시 화면 즉시 갱신
        selection_mode="single-row"  # 단일 행 선택 모드
    )

    # 사용자가 표에서 특정 행을 클릭했는지 확인
    selected_rows = selection_event.selection.rows

    if selected_rows:
        selected_idx = selected_rows[0]
        selected_row = site_list.iloc[selected_idx]

        sel_site_id = selected_row.get(COL_SITE_ID, "")
        sel_station_id = selected_row.get(COL_STATION_ID, "")
        sel_display = selected_row["display_label"]

        with st.spinner(f"'{sel_display}' 상태 이력 연동 중..."):
            df = load_site_history(sel_site_id, sel_station_id)

        # 표 아래쪽에 대시보드 렌더링
        st.markdown("---")
        render_site_dashboard(df, sel_display)
    else:
        st.info("👆 위 표에서 상세 이력을 확인할 충전소(사이트)를 마우스로 클릭해 주세요.")

else:
    st.warning("검색 결과를 사이트별로 그룹핑할 수 없습니다.")
