# app.py — CO5173 TikTok Hashtag Intelligence (Databricks Lakehouse)
# UI nâng cấp V5: Thêm Tab 9 - AI Phân tích Kênh & Tab 10 - Promote
# Sử dụng Python Requests để gọi Generative AI

import streamlit as st
import pandas as pd
from typing import List, Optional, Dict
import requests  # Thêm thư viện để gọi API
import json      # Thêm thư viện để xử lý JSON

# ---- Plotly import guard ----
try:
    import plotly.express as px
except Exception:
    px = None

from util.db import run_sql
from util.filters import sidebar_filters

# ---- Cấu hình trang (Page Config) ----
st.set_page_config(
    page_title="TikTok Creator Studio - Quyết định Nội dung",
    page_icon="💡",  # Icon mới
    layout="wide"
)
st.title("💡 TikTok Creator Studio")
st.caption("Dashboard 10 Chức năng hỗ trợ Ra Quyết định Sáng tạo & Quảng bá")

# ---------------- Helpers ----------------
def _sql_quote(val: str) -> str:
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def _in_list_sql(values: List[str]) -> str:
    return ",".join(_sql_quote(v) for v in values)

def _date_expr(col: str) -> str:
    return f"DATE({col})"

@st.cache_data(ttl=600)  # Giữ cache 10 phút
def run_sql_safe(sql: str) -> pd.DataFrame:
    try:
        return run_sql(sql)
    except Exception as e:
        st.warning(f"SQL error: {e}")
        return pd.DataFrame()

def dedup_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    return df.loc[:, ~pd.Index(df.columns).duplicated()]

def uniquify_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    seen: Dict[str, int] = {}
    new_cols: List[str] = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}__{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols
    return df

# Helper để hiển thị bảng trong expander (Ưu tiên biểu đồ)
def show_data_expander(df: pd.DataFrame, title: str = "Xem dữ liệu chi tiết (bảng)"):
    if not df.empty:
        with st.expander(title):
            st.dataframe(df, use_container_width=True)

def plot_stretch(fig):
    st.plotly_chart(fig, use_container_width=True)  # Giữ nguyên để tránh lỗi version

def csv_download(df: pd.DataFrame, filename: str):
    if not df.empty:
        st.download_button("⬇️ Tải CSV", df.to_csv(index=False).encode("utf-8"), filename, "text/csv")

def table_columns(table: str) -> List[str]:
    df = run_sql_safe(f"SHOW COLUMNS IN {table}")
    cols: List[str] = []
    if not df.empty:
        if 'col_name' in df.columns:
            cols = [str(x).strip() for x in df['col_name'].tolist() if x and str(x).strip() != '']
        else:
            cols = [c for c in df.columns]
    else:
        ddf = run_sql_safe(f"DESCRIBE TABLE {table}")
        if not ddf.empty and 'col_name' in ddf.columns:
            cols = [str(x).strip() for x in ddf['col_name'].tolist()
                    if x and not str(x).startswith('#') and str(x).lower() != 'partition']
    return [c for c in cols if c and c.lower() != 'partition']

# ------------- Load meta -------------
meta = run_sql_safe("SELECT MIN(dt) AS min_d, MAX(dt) AS max_d FROM silver.silver_trend")
min_d = meta.iloc[0]["min_d"] if not meta.empty else None
max_d = meta.iloc[0]["max_d"] if not meta.empty else None

countries_df = run_sql_safe("""
    SELECT DISTINCT country_code
    FROM silver.silver_trend
    WHERE country_code IS NOT NULL
    ORDER BY country_code
""")
countries = [str(x) for x in countries_df["country_code"].tolist()] if not countries_df.empty else []

industries_df = run_sql_safe("""
    SELECT DISTINCT industry
    FROM silver.silver_trend
    WHERE industry IS NOT NULL
    ORDER BY industry
""")
industries = [str(x) for x in industries_df["industry"].tolist()] if not industries_df.empty else []

# Sidebar filters
START_DATE, END_DATE, COUNTRIES, INDUSTRIES, KEYWORD, TOPN = sidebar_filters(
    min_d if pd.notna(min_d) else None,
    max_d if pd.notna(max_d) else None,
    countries,
    industries,
)

# ------------- WHERE-builder -------------
def build_where(
    dt_col: Optional[str] = "dt",
    country_col: Optional[str] = "country_code",
    industry_col: Optional[str] = "industry",
    hashtag_expr: Optional[str] = "COALESCE(hashtag_raw, hashtag)",
) -> str:
    """
    Xây WHERE clause theo filter global. 
    Lưu ý:
      - Nếu hashtag_expr=None thì sẽ bỏ qua filter KEYWORD (dùng cho bảng không có hashtag).
    """
    clauses: List[str] = []
    if dt_col and START_DATE and END_DATE:
        clauses.append(f"{_date_expr(dt_col)} BETWEEN DATE('{START_DATE}') AND DATE('{END_DATE}')")
    if country_col and COUNTRIES and COUNTRIES != ["ALL"]:
        items = [c for c in COUNTRIES if c != "ALL"]
        if items:
            clauses.append(f"{country_col} IN ({_in_list_sql(items)})")
    if industry_col and INDUSTRIES and INDUSTRIES != ["ALL"]:
        items = [i for i in INDUSTRIES if i != "ALL"]
        if items:
            clauses.append(f"{industry_col} IN ({_in_list_sql(items)})")
    if KEYWORD and hashtag_expr:
        kw = str(KEYWORD).lower().replace("'", "''")
        clauses.append(f"LOWER({hashtag_expr}) LIKE '%{kw}%'")
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""

# ------------- Action buttons (Simplified) -------------
if st.sidebar.button("🔄 Refresh Cache (10m)"):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared.")

# --------- KPI header ---------
st.divider()
colA, colB, colC, colD = st.columns(4)
sql_kpi = f"""
SELECT
  COUNT(DISTINCT hashtag) AS uniq_hashtags,
  COUNT(DISTINCT CASE WHEN {_date_expr('dt')} = (SELECT MAX(DATE(dt)) FROM silver.silver_trend) THEN hashtag END) AS today_tags,
  COUNT(DISTINCT country_code) AS uniq_countries,
  COUNT(DISTINCT industry) AS uniq_industries
FROM silver.silver_trend
{build_where(dt_col='dt', hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
"""
kpi = run_sql_safe(sql_kpi)
a = int(kpi.iloc[0]["uniq_hashtags"] or 0) if not kpi.empty else 0
b = int(kpi.iloc[0]["today_tags"] or 0)    if not kpi.empty else 0
c = int(kpi.iloc[0]["uniq_countries"] or 0)if not kpi.empty else 0
d = int(kpi.iloc[0]["uniq_industries"] or 0)if not kpi.empty else 0
colA.metric("Hashtags trong phạm vi", f"{a:,}")
colB.metric("Hashtags hôm mới nhất", f"{b:,}")
colC.metric("Quốc gia", f"{c:,}")
colD.metric("Ngành", f"{d:,}")
st.caption("Các KPI phản ánh bộ lọc hiện tại trong sidebar.")
st.divider()

# ------------- Tải Dữ liệu 1 lần (Tối ưu) -------------

# Lấy dữ liệu Momentum (dùng cho Tab 2)
sql_m = f"""
  WITH b AS (
    SELECT DISTINCT DATE(dt) AS dt, hashtag, country_code, industry, hashtag_raw
    FROM silver.silver_trend
  ),
  m AS (
    SELECT dt, hashtag, rank, prev_rank, rank_velocity, view_delta, video_delta
    FROM gold.trend_momentum
  ),
  j AS (
    SELECT m.*, b.country_code, b.industry, b.hashtag_raw
    FROM m LEFT JOIN b ON DATE(m.dt)=b.dt AND m.hashtag=b.hashtag
  )
  SELECT * FROM j
  {build_where(dt_col='j.dt', country_col='j.country_code', industry_col='j.industry',
              hashtag_expr='COALESCE(j.hashtag_raw, j.hashtag)')}
"""
mom = run_sql_safe(sql_m)
if mom.empty:
    sql_fb = f"""
      WITH s AS (
        SELECT DATE(dt) dt, hashtag, rank, view_count, video_count, country_code, industry, hashtag_raw
        FROM silver.silver_trend
      ),
      best AS (
        SELECT * FROM (
          SELECT s.*, ROW_NUMBER() OVER (PARTITION BY dt, hashtag ORDER BY COALESCE(rank,999), view_count DESC) rn
          FROM s
        ) x WHERE rn=1
      ),
      x AS (
        SELECT
          dt, hashtag, rank,
          LAG(rank) OVER (PARTITION BY hashtag ORDER BY dt) AS prev_rank,
          (LAG(rank) OVER (PARTITION BY hashtag ORDER BY dt) - rank) AS rank_velocity,
          (view_count - LAG(view_count) OVER (PARTITION BY hashtag ORDER BY dt)) AS view_delta,
          (video_count - LAG(video_count) OVER (PARTITION BY hashtag ORDER BY dt)) AS video_delta,
          country_code, industry, hashtag_raw
        FROM best
      )
      SELECT * FROM x
      {build_where(dt_col='dt', country_col='country_code', industry_col='industry',
                   hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
    """
    mom = run_sql_safe(sql_fb)

mom = uniquify_columns(dedup_cols(mom))
mom["view_delta"] = pd.to_numeric(mom.get("view_delta"), errors="coerce").fillna(0)
mom["rank_velocity"] = pd.to_numeric(mom.get("rank_velocity"), errors="coerce").fillna(0)
latest_mom_dt = mom['dt'].max() if not mom.empty else "N/A"
mom_latest = mom[mom['dt'] == latest_mom_dt] if not mom.empty else pd.DataFrame()

# Lấy dữ liệu Retention (dùng cho Tab 3, 4)
sql_ret = f"""
  WITH base AS (
    SELECT DISTINCT DATE(dt) AS dt, hashtag, url, country_code, industry, hashtag_raw
    FROM silver.silver_trend
  ),
  j AS (
    SELECT r.hashtag, r.start_dt, r.end_dt, r.streak_days,
           b.url, b.country_code, b.industry, b.hashtag_raw
    FROM gold.trend_retention r
    LEFT JOIN base b ON r.hashtag=b.hashtag AND r.end_dt=b.dt
  )
  SELECT * FROM j
  {build_where(dt_col='j.end_dt', country_col='j.country_code', industry_col='j.industry',
               hashtag_expr='COALESCE(j.hashtag_raw, j.hashtag)')}
"""
df_ret = run_sql_safe(sql_ret)
if df_ret.empty:
    sql_ret_fb = f"""
      WITH s AS (
        SELECT DISTINCT DATE(dt) dt, hashtag FROM silver.silver_trend
      ),
      g AS (
        SELECT hashtag, dt,
          DATEDIFF(dt, DATE'1970-01-01') - ROW_NUMBER() OVER (PARTITION BY hashtag ORDER BY dt) grp
        FROM s
      ),
      streaks AS (
        SELECT hashtag, MIN(dt) start_dt, MAX(dt) end_dt, COUNT(*) streak_days
        FROM g GROUP BY hashtag, grp
      ),
      dim AS (
        SELECT DISTINCT DATE(dt) dt, hashtag, url, country_code, industry, hashtag_raw
        FROM silver.silver_trend
      )
      SELECT r.hashtag, r.start_dt, r.end_dt, r.streak_days,
             d.url, d.country_code, d.industry, d.hashtag_raw
      FROM streaks r
      LEFT JOIN dim d ON r.hashtag=d.hashtag AND r.end_dt=d.dt
      {build_where(dt_col='r.end_dt', country_col='d.country_code', industry_col='d.industry',
                   hashtag_expr='COALESCE(d.hashtag_raw, r.hashtag)')}
    """
    df_ret = run_sql_safe(sql_ret_fb)
df_ret = uniquify_columns(dedup_cols(df_ret))

# Lấy dữ liệu New Entries (dùng cho Tab 3)
sql_new = f"""
WITH base AS (SELECT DISTINCT DATE(dt) dt, hashtag FROM silver.silver_trend),
     firsts AS (SELECT hashtag, MIN(dt) dt FROM base GROUP BY hashtag)
SELECT dt, COUNT(*) AS new_count
FROM firsts
{build_where(dt_col='dt', country_col=None, industry_col=None, hashtag_expr='hashtag')}
GROUP BY dt
ORDER BY dt
"""
df_new = run_sql_safe(sql_new)

# ------------- Tabs (Cấu trúc 10 Chức năng Sáng tạo) -------------
tabs = st.tabs([
    "🎯 1. Tìm Ngách (Niche Finder)",
    "🔥 2. Động lượng Trend (Momentum)",
    "⚡ 3. Chiến lược Trend Nhanh",
    "🌳 4. Chiến lược Bền vững",
    "📊 5. Phân tích Bão hòa Ngành",
    "🌍 6. Phân tích Thị trường QG",
    "🏆 7. Top 100 Đã Kiểm chứng",
    "📅 8. Lập kế hoạch Tuần",
    "🤖 9. AI Phân tích Kênh",   # <-- TAB 9
    "📣 10. Phân tích Promote"   # <-- TAB 10 mới
])

# ===== 🎯 1. Tìm Ngách (Niche Finder) =====
with tabs[0]:
    st.subheader("🎯 1. Phát hiện Cơ hội (Niche Finder)")
    st.markdown("Chức năng: Tìm hashtag có **lượt xem (Demand) cao** nhưng **số video (Competition) thấp**."
                " Hãy tìm các điểm ở **góc trên bên trái**.")

    sql_opp = f"""
        WITH mx AS (SELECT MAX(dt) AS mx FROM silver.silver_trend)
        SELECT
          t.hashtag, t.view_count, t.video_count,
          t.industry, t.country_code, t.rank
        FROM gold.trend_latest_top100 t
        JOIN mx ON t.dt = mx.mx
        {build_where(dt_col='t.dt', country_col='t.country_code', industry_col='t.industry',
                     hashtag_expr='COALESCE(t.hashtag_raw, t.hashtag)')}
    """
    df_opp = run_sql_safe(sql_opp)
    
    # Fallback nếu gold rỗng
    if df_opp.empty:
        sql_opp_fb = f"""
            WITH mx AS (SELECT MAX(DATE(dt)) AS mx FROM silver.silver_trend),
            s AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY hashtag ORDER BY view_count DESC) rn
                FROM silver.silver_trend
                WHERE DATE(dt) = (SELECT mx FROM mx)
            )
            SELECT hashtag, view_count, video_count, industry, country_code, rank
            FROM s WHERE rn = 1
            {build_where(dt_col=None, country_col='country_code', industry_col='industry',
                         hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
        """
        df_opp = run_sql_safe(sql_opp_fb)

    if not df_opp.empty and px is not None and "view_count" in df_opp.columns and "video_count" in df_opp.columns:
        df_opp_plot = df_opp.dropna(subset=["view_count", "video_count"])
        df_opp_plot = df_opp_plot[df_opp_plot['view_count'] > 0]
        df_opp_plot = df_opp_plot[df_opp_plot['video_count'] > 0]
        
        if not df_opp_plot.empty:
            fig_opp = px.scatter(
                df_opp_plot,
                x="video_count",
                y="view_count",
                color="industry",
                size="view_count",
                hover_data=["hashtag", "country_code", "rank"],
                title="Biểu đồ Cơ hội: Lượt xem (Y) vs. Cạnh tranh (X)",
                labels={"video_count": "Số lượng video (Cạnh tranh ⬆️)", "view_count": "Số lượt xem (Nhu cầu ⬆️)"}
            )
            fig_opp.update_xaxes(type="log") 
            fig_opp.update_yaxes(type="log") 
            plot_stretch(fig_opp)
        else:
            st.info("Không có dữ liệu (sau khi lọc) cho biểu đồ cơ hội.")
            
    elif px is None:
        st.warning("Cài đặt Plotly để xem biểu đồ.")
    else:
        st.info("Không có dữ liệu cho biểu đồ cơ hội.")
    
    show_data_expander(df_opp, "Xem dữ liệu cơ hội")
    csv_download(df_opp, "opportunity_latest.csv")

# ===== 🔥 2. Động lượng Trend (Momentum) =====
with tabs[1]:
    st.subheader("🔥 2. Phân tích Động lượng Trend (Momentum)")
    st.markdown("Chức năng: Xem nhanh các hashtag 'Nóng', 'Ngôi sao' và 'Nguội' trong ngày."
                f" (Dữ liệu ngày: **{latest_mom_dt}**)")

    if not mom_latest.empty and px is not None:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### 🔥 Trend Nóng (Views)")
            df_rising = mom_latest.sort_values("view_delta", ascending=False).head(15)
            if not df_rising.empty:
                fig_rising = px.bar(
                    df_rising, x="view_delta", y="hashtag", orientation="h",
                    title="Top 15 Tăng View",
                    labels={"view_delta": "Thay đổi Lượt xem (DoD)", "hashtag": "Hashtag"},
                    hover_data=["industry", "rank", "rank_velocity"]
                )
                fig_rising.update_layout(yaxis={'categoryorder':'total ascending'})
                plot_stretch(fig_rising)
            else:
                st.info("Không có dữ liệu tăng trưởng.")

        with col2:
            st.markdown("#### ✨ Ngôi sao mới (Rank)")
            df_rank_rising = mom_latest.sort_values("rank_velocity", ascending=False).head(15)
            if not df_rank_rising.empty:
                fig_rank_rising = px.bar(
                    df_rank_rising, x="rank_velocity", y="hashtag", orientation="h",
                    title="Top 15 Tăng Hạng nhanh nhất",
                    labels={"rank_velocity": "Thay đổi Hạng (DoD)", "hashtag": "Hashtag"},
                    hover_data=["industry", "rank", "view_delta"]
                )
                fig_rank_rising.update_layout(yaxis={'categoryorder':'total ascending'})
                plot_stretch(fig_rank_rising)
            else:
                st.info("Không có dữ liệu tăng trưởng hạng.")
        
        with col3:
            st.markdown("#### ❄️ Trend Nguội (Fading)")
            df_fading = mom_latest.sort_values("view_delta", ascending=True).head(15)
            if not df_fading.empty:
                fig_fading = px.bar(
                    df_fading, x="view_delta", y="hashtag", orientation="h",
                    title="Top 15 Giảm View",
                    labels={"view_delta": "Thay đổi Lượt xem (DoD)", "hashtag": "Hashtag"},
                    hover_data=["industry", "rank", "rank_velocity"]
                )
                fig_fading.update_layout(yaxis={'categoryorder':'total descending'})
                plot_stretch(fig_fading)
            else:
                st.info("Không có dữ liệu suy giảm.")
    else:
        st.info("Không có dữ liệu Momentum cho ngày mới nhất.")
        
    show_data_expander(mom, "Xem toàn bộ dữ liệu Momentum")

# ===== ⚡ 3. Chiến lược Trend Nhanh =====
with tabs[2]:
    st.subheader("⚡ 3. Chiến lược Trend Nhanh (Short-term)")
    st.markdown("Chức năng: Hiểu tốc độ của trend. Hầu hết trend 'sống' bao lâu và mỗi ngày có bao nhiêu trend mới?")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Vòng đời Xu hướng (Trend Lifespan)")
        if not df_ret.empty and px is not None:
            df_life = df_ret.copy()
            fig_life = px.histogram(
                df_life, x="streak_days",
                title="Phân bổ Vòng đời Xu hướng",
                labels={"streak_days": "Số ngày liên tục (Streak)", "count": "Số lượng chuỗi (Count)"},
                nbins=max(20, int(df_life["streak_days"].max())) 
            )
            plot_stretch(fig_life)
        elif px is None:
            st.warning("Cài đặt Plotly.")
        else:
            st.info("Không có dữ liệu Vòng đời xu hướng.")
            
    with col2:
        st.markdown("#### Lượng Hashtag Mới Hàng Ngày")
        if not df_new.empty and px is not None:
            fig_new = px.bar(df_new, x="dt", y="new_count", title="Số hashtag mới theo ngày")
            plot_stretch(fig_new)
        else:
            st.info("Không có dữ liệu Hashtag mới.")

    show_data_expander(df_new.merge(df_ret, how='cross'), "Xem dữ liệu (Kết hợp)")

# ===== 🌳 4. Chiến lược Bền vững =====
with tabs[3]:
    st.subheader("🌳 4. Chiến lược Bền vững (Long-term)")
    st.markdown("Chức năng: Tìm các chủ đề/hashtag 'evergreen' để xây dựng nội dung kênh dài hạn.")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### Top 20 Hashtag 'sống dai' nhất")
        if not df_ret.empty and px is not None and "streak_days" in df_ret.columns:
            top_streak = df_ret.sort_values("streak_days", ascending=False).head(20)
            fig_streak = px.bar(top_streak, x="streak_days", y="hashtag", orientation="h", title="Top 20 chuỗi dài nhất")
            fig_streak.update_yaxes(autorange="reversed")
            plot_stretch(fig_streak)
        else:
            st.info("Không có dữ liệu Retention.")
    
    with col2:
        st.markdown("#### Top 10 Ngành Bền vững nhất")
        if not df_ret.empty and px is not None:
            df_ret_agg = df_ret.groupby('industry')['streak_days'].mean().reset_index().sort_values('streak_days', ascending=False).head(10)
            if not df_ret_agg.empty:
                fig_ret_agg = px.bar(
                    df_ret_agg, x="streak_days", y="industry", orientation="h",
                    title="Top 10 Ngành Bền vững (TB số ngày trend)",
                    labels={"streak_days": "Số ngày trend trung bình", "industry": "Ngành"}
                )
                fig_ret_agg.update_yaxes(autorange="reversed")
                plot_stretch(fig_ret_agg)
            else:
                st.info("Không thể tính TB ngành.")
        else:
            st.info("Không có dữ liệu Retention.")
            
    show_data_expander(df_ret, "Xem dữ liệu Retention chi tiết")
    csv_download(df_ret, "retention.csv")

# ===== 📊 5. Phân tích Bão hòa Ngành =====
with tabs[4]:
    st.subheader("📊 5. Phân tích Bão hòa & Hiệu quả Ngành")
    st.markdown("Chức năng: Ngành nào đang có nhiều 'Thị phần' (Views) và ngành nào 'Hiệu quả' (dễ có view) nhất?")
    
    mx = run_sql_safe("SELECT MAX(DATE(dt)) AS mx FROM silver.silver_trend")
    latest = mx.iloc[0]["mx"] if not mx.empty else None
    
    if latest:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"#### Cơ cấu Thị phần (Views) - Ngày {latest}")
            extra = ""
            if COUNTRIES and COUNTRIES != ["ALL"]:
                extra += f" AND country_code IN ({_in_list_sql([c for c in COUNTRIES if c!='ALL'])})"
            if KEYWORD:
                kw = str(KEYWORD).lower().replace("'", "''")
                extra += f" AND LOWER(COALESCE(hashtag_raw, hashtag)) LIKE '%{kw}%'"
            sql_ind = f"""
            SELECT industry, SUM(view_count) AS total_views
            FROM silver.silver_trend
            WHERE DATE(dt)=DATE('{latest}') AND industry IS NOT NULL
            {extra}
            GROUP BY industry ORDER BY total_views DESC LIMIT 12
            """
            df_ind = run_sql_safe(sql_ind)
            if not df_ind.empty and px is not None:
                fig3 = px.pie(df_ind, names="industry", values="total_views", title=f"Cơ cấu view theo ngành")
                plot_stretch(fig3)
            else:
                st.info("Không có dữ liệu cơ cấu ngành.")

        with col2:
            st.markdown(f"#### Hiệu quả Ngành (Views / Video) - Ngày {latest}")
            sql_eff = f"""
                SELECT 
                    industry, 
                    SUM(view_count) AS total_views, 
                    SUM(video_count) AS total_videos,
                    SUM(view_count) / NULLIF(SUM(video_count), 0) AS view_per_video
                FROM silver.silver_trend
                WHERE DATE(dt)=DATE('{latest}') 
                  AND industry IS NOT NULL 
                  AND video_count > 0
                {extra}  
                GROUP BY industry 
                ORDER BY view_per_video DESC
                LIMIT 15
            """
            df_eff = run_sql_safe(sql_eff)
            if not df_eff.empty and px is not None:
                fig_eff = px.bar(
                    df_eff, x="view_per_video", y="industry", orientation="h",
                    title="Top 15 Ngành Hiệu quả nhất",
                    labels={"view_per_video": "Lượt View / 1 Video", "industry": "Ngành"},
                    hover_data=["total_views", "total_videos"]
                )
                fig_eff.update_yaxes(autorange="reversed")
                plot_stretch(fig_eff)
            else:
                st.info("Không có dữ liệu hiệu quả ngành.")
        
        merged = pd.DataFrame()
        if 'df_ind' in locals() and 'df_eff' in locals():
            try:
                merged = df_ind.merge(df_eff, on='industry', how='outer')
            except Exception:
                merged = pd.DataFrame()
        show_data_expander(merged, "Xem dữ liệu Ngành chi tiết")

    else:
        st.info("Không có dữ liệu cho ngày mới nhất.")

# ===== 🌍 6. Phân tích Thị trường QG =====
with tabs[5]:
    st.subheader("🌍 6. Phân tích Thị trường Quốc gia")
    st.markdown("Chức năng: Xem tổng quan thị trường theo quốc gia. Thị trường nào đang phát triển nhanh nhất?")

    gold_country_cols = table_columns("gold.trend_country_summary")
    df_ct = pd.DataFrame()
    if any(c in gold_country_cols for c in ["total_views", "views", "view_sum"]):
        col_name = "total_views" if "total_views" in gold_country_cols else ("views" if "views" in gold_country_cols else "view_sum")
        sql_country = f"""
            SELECT dt, country_code, {col_name} AS total_views
            FROM gold.trend_country_summary
            {build_where(dt_col='dt', country_col='country_code', industry_col=None, hashtag_expr=None)}
            ORDER BY dt, country_code
        """
        df_ct = run_sql_safe(sql_country)

    if df_ct.empty:
        sql_country_fb = f"""
        SELECT DATE(dt) dt, country_code, SUM(view_count) AS total_views
        FROM silver.silver_trend
        {build_where(dt_col='dt', country_col='country_code', industry_col=None,
                     hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
        GROUP BY 1,2 ORDER BY 1,2
        """
        df_ct = run_sql_safe(sql_country_fb)

    if not df_ct.empty and px is not None:
        fig_ct = px.area(df_ct, x="dt", y="total_views", color="country_code", title="Tổng view theo quốc gia (stacked)")
        plot_stretch(fig_ct)
    show_data_expander(df_ct, "Xem dữ liệu View theo Quốc gia")
    csv_download(df_ct, "views_by_country.csv")

# ===== 🏆 7. Top 100 Đã Kiểm chứng =====
with tabs[6]:
    st.subheader("🏆 7. Top 100 Đã Kiểm chứng (Proven Winners)")
    st.markdown("Chức năng: Danh sách 100 hashtag hàng đầu đã được chứng minh hiệu quả."
                " Dùng cho các chiến dịch cần sự an toàn, đã kiểm chứng (proven winners).")
    
    df_top100 = run_sql_safe(f"""
        WITH mx AS (SELECT MAX(dt) AS mx FROM silver.silver_trend)
        SELECT
          t.dt, t.hashtag, t.rank, t.view_count, t.video_count,
          t.country_code, t.industry, t.category,
          t.hashtag_raw, t.url
        FROM gold.trend_latest_top100 t
        JOIN mx ON t.dt = mx.mx
        {build_where(dt_col='t.dt', country_col='t.country_code', industry_col='t.industry',
                     hashtag_expr='COALESCE(t.hashtag_raw, t.hashtag)')}
        ORDER BY COALESCE(t.rank, 999) ASC
        LIMIT 100
    """)
    df_top100 = uniquify_columns(dedup_cols(df_top100))
    
    if not df_top100.empty and px is not None:
        topn_val = min(20, len(df_top100))
        fig_top100_bar = px.bar(
            df_top100.head(topn_val),
            x="view_count", y="hashtag",
            color="industry",
            orientation="h",
            title=f"Top {topn_val} theo view (latest day)",
            hover_data=["rank", "video_count", "country_code"]
        )
        fig_top100_bar.update_yaxes(autorange="reversed")
        plot_stretch(fig_top100_bar)
    
    show_data_expander(df_top100, "Xem dữ liệu Top 100 Mới nhất")
    csv_download(df_top100, "latest_top100.csv")

# ===== 📅 8. Lập kế hoạch Tuần =====
with tabs[7]:
    st.subheader("📅 8. Lập kế hoạch theo Tuần (Weekly Planner)")
    st.markdown("Chức năng: Xem xu hướng thứ hạng trung bình của hashtag theo tuần. "
                "Dùng để lập kế hoạch nội dung hàng tuần.")
    
    # SQL Builder riêng cho Weekly (vì dt_col là 'week')
    where_parts: List[str] = []
    if START_DATE and END_DATE:
        where_parts.append(
            "DATE(w.week) BETWEEN DATE(DATE_TRUNC('week', DATE('{s}'))) AND DATE(DATE_TRUNC('week', DATE('{e}')))"
            .format(s=START_DATE, e=END_DATE)
        )
    if COUNTRIES and COUNTRIES != ["ALL"]:
        where_parts.append(f"b.country_code IN ({_in_list_sql([c for c in COUNTRIES if c!='ALL'])})")
    if INDUSTRIES and INDUSTRIES != ["ALL"]:
        where_parts.append(f"b.industry IN ({_in_list_sql([i for i in INDUSTRIES if i!='ALL'])})")
    if KEYWORD:
        kw = str(KEYWORD).lower().replace("'", "''")
        where_parts.append("LOWER(COALESCE(b.hashtag_raw, w.hashtag)) LIKE '%" + kw + "%'")
    weekly_where = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sql_week = f"""
      WITH b AS (
        SELECT DISTINCT DATE(dt) dt, hashtag, country_code, industry, hashtag_raw
        FROM silver.silver_trend
      )
      SELECT w.week, w.hashtag, w.best_rank, w.avg_rank, w.new_days_count, w.max_views,
             b.country_code, b.industry, b.hashtag_raw
      FROM gold.trend_weekly_summary w
      LEFT JOIN b ON w.hashtag = b.hashtag
      {weekly_where}
      ORDER BY w.week DESC, COALESCE(w.best_rank, 999) ASC
    """
    dfw = run_sql_safe(sql_week)
    if dfw.empty:
        sql_fb = f"""
          WITH base AS (
            SELECT DATE(dt) dt, hashtag, COALESCE(rank,999) rank, view_count
            FROM silver.silver_trend
          ),
          best AS (
            SELECT * FROM (
              SELECT base.*, ROW_NUMBER() OVER (PARTITION BY dt, hashtag ORDER BY rank, view_count DESC) rn
              FROM base
            ) x WHERE rn=1
          ),
          w AS (
            SELECT DATE_TRUNC('week', dt) AS week, hashtag,
                   MIN(rank) AS best_rank, AVG(rank) AS avg_rank,
                   COUNT(*) AS new_days_count, MAX(view_count) AS max_views
            FROM best GROUP BY 1,2
          ),
          b AS (
            SELECT DISTINCT DATE(dt) dt, hashtag, country_code, industry, hashtag_raw
            FROM silver.silver_trend
          )
          SELECT w.week, w.hashtag, w.best_rank, w.avg_rank, w.new_days_count, w.max_views,
                 b.country_code, b.industry, b.hashtag_raw
          FROM w LEFT JOIN b ON w.hashtag=b.hashtag
          {weekly_where}
          ORDER BY w.week DESC, COALESCE(w.best_rank,999) ASC
        """
        dfw = run_sql_safe(sql_fb)

    dfw = uniquify_columns(dedup_cols(dfw))
    
    if not dfw.empty and px is not None and "hashtag" in dfw.columns and "avg_rank" in dfw.columns:
        top_tags = dfw.sort_values(["best_rank"]).dropna(subset=["best_rank"]).head(5)["hashtag"].unique().tolist()
        sel = st.multiselect(
            "Chọn hashtag để xem xu hướng thứ hạng (tối đa 10)",
            options=sorted(dfw["hashtag"].unique()),
            default=top_tags[:5],
            max_selections=10
        )
        if sel:
            plot_df = dfw[dfw["hashtag"].isin(sel)].copy()
            fig = px.line(plot_df, x="week", y="avg_rank", color="hashtag", title="Xu hướng thứ hạng TB theo tuần")
            fig.update_yaxes(autorange="reversed") # Rank thấp (1) tốt hơn
            plot_stretch(fig)
            
    show_data_expander(dfw, "Xem dữ liệu Weekly Summary chi tiết")
    csv_download(dfw, "weekly_summary.csv")

# ===== 🤖 9. AI Phân tích Kênh =====
with tabs[8]:  # Index 8 cho tab thứ 9
    import datetime as _dt

    st.subheader("🤖 9. AI Phân tích Kênh & Gợi ý Kịch bản (No URL, No Mock)")
    st.markdown(
        "Điền **Prompt Builder** (mục tiêu/đối tượng/KPI/giọng), hệ thống sẽ **tự động chọn hashtag** "
        "từ dữ liệu (Hot/Evergreen/Opportunity/Proven/Weekly) để tạo **ý tưởng** + **kịch bản**."
    )

    # ---------------- Prompt helper: hướng dẫn cấu trúc ----------------
    with st.expander("🧩 Hướng dẫn cấu trúc Prompt gợi ý (copy/sửa trực tiếp)"):
        st.markdown("""
**[Mục tiêu]**: Tăng view/engagement/CTR/đơn hàng cho ..., trong ... ngày  
**[Đối tượng]**: Nam/Nữ, 18–24, ở ..., quan tâm ...  
**[Sản phẩm/Dịch vụ]**: Tên, lợi ích chính, USP  
**[Tông giọng]**: Vui vẻ/đanh đá/chuyên gia/ấm áp  
**[Định dạng]**: TikTok 30–45s, Hook ≤3s, 3 cảnh chính, cuối có CTA  
**[KPI]**: View ≥..., ER ≥..., CTR ≥...  
**[Ràng buộc]**: Không dùng nhạc bản quyền, không nói giá, tránh đề cập ...  
**[Đầu ra mong muốn]**:  
- 3–5 ý tưởng (ghi rõ Hook, Visual, Voiceover, CTA)  
- 1 kịch bản chi tiết 30–45s (shot-by-shot)  
- Hashtag đề xuất (kết hợp niche + trend)  
- Lịch đăng 1 tuần (giờ vàng gợi ý)
        """)

    # ---------------- 1) Output options ----------------
    col_lang, col_num = st.columns([1, 1])
    with col_lang:
        out_lang = st.selectbox("Ngôn ngữ đầu ra", ["Tiếng Việt", "English"], index=0)
    with col_num:
        idea_count = st.number_input("Số ý tưởng", min_value=1, max_value=10, value=3, step=1)

    # ---------------- 2) Prompt Builder ----------------
    st.markdown("### 🛠️ Prompt Builder")
    col_a, col_b = st.columns(2)
    with col_a:
        pb_goal = st.text_area("Mục tiêu chiến dịch", height=80,
                               placeholder="VD: Tăng view 30% trong 14 ngày cho niche 'Food & Travel'…")
        pb_audience = st.text_area("Đối tượng mục tiêu", height=80,
                                   placeholder="VD: Nữ 18–24, HN/HCM, thích ăn uống & du lịch budget…")
        pb_product = st.text_area("Sản phẩm/Dịch vụ (USP/Lợi ích)", height=80,
                                  placeholder="VD: Tour ẩm thực đêm Sài Gòn, giá tốt, lịch trình linh hoạt…")
    with col_b:
        pb_tone = st.text_input("Tông giọng", value="Vui vẻ, giàu năng lượng, tự nhiên")
        pb_kpi = st.text_input("KPI kỳ vọng", value="View ≥ 50k/video, ER ≥ 6%")
        pb_constraints = st.text_area("Ràng buộc", height=80,
                                      placeholder="Không dùng nhạc bản quyền; tránh nói giá; phù hợp brand safe…")

    freeform_prompt = st.text_area(
        "Prompt bổ sung (tự do, sẽ gộp cùng Builder ở trên)",
        height=100,
        placeholder="Thêm hướng dẫn riêng của bạn…"
    )

    # ---------------- Helpers ----------------
    def _dedup_ci_keep_order(seq):
        out, seen = [], set()
        for s in (seq or []):
            if s is None:
                continue
            k = str(s).strip()
            if not k:
                continue
            key = k.lower()
            if key not in seen:
                seen.add(key)
                out.append(k)
        return out

    def _extract_keywords(*texts):
        import re
        txt = " ".join([t for t in texts if t])[:5000].lower()
        tags = re.findall(r"#([a-z0-9_]+)", txt)               # hashtag
        words = re.findall(r"[a-z0-9_]{3,}", txt)              # keyword ASCII
        return _dedup_ci_keep_order(tags + words)

    # ---------------- 3) Gom nhóm hashtag từ data đã load ----------------
    # Hot (Momentum)
    hot_hashtags = []
    if 'mom_latest' in locals() and mom_latest is not None and not mom_latest.empty:
        hot_hashtags = (
            mom_latest.sort_values("view_delta", ascending=False)
            .dropna(subset=["hashtag"]).head(50)["hashtag"].astype(str).tolist()
        )

    # Evergreen (Retention)
    evergreen_hashtags = []
    if 'df_ret' in locals() and df_ret is not None and not df_ret.empty:
        evergreen_hashtags = (
            df_ret.sort_values("streak_days", ascending=False)
            .dropna(subset=["hashtag"]).head(50)["hashtag"].astype(str).tolist()
        )

    # Opportunity (view/video cao)
    opportunity_hashtags = []
    if 'df_opp' in locals() and df_opp is not None and not df_opp.empty:
        tmp = df_opp.copy()
        if all(c in tmp.columns for c in ["view_count", "video_count"]):
            tmp["vv"] = pd.to_numeric(tmp["view_count"], errors="coerce") / pd.to_numeric(tmp["video_count"], errors="coerce").replace(0, pd.NA)
            tmp = tmp.dropna(subset=["vv"]).sort_values("vv", ascending=False)
            opportunity_hashtags = tmp.head(50)["hashtag"].astype(str).tolist()

    # Proven winners (Top100)
    proven_hashtags = []
    if 'df_top100' in locals() and df_top100 is not None and not df_top100.empty:
        proven_hashtags = df_top100["hashtag"].dropna().astype(str).head(100).tolist()

    # Weekly top (best_rank tốt)
    weekly_hashtags = []
    if 'dfw' in locals() and dfw is not None and not dfw.empty:
        weekly_hashtags = (
            dfw.sort_values(["week", "best_rank"])
            .dropna(subset=["hashtag", "best_rank"]).head(100)["hashtag"].astype(str).tolist()
        )

    # Merge và khử trùng lặp (case-insensitive)
    all_suggested = _dedup_ci_keep_order(
        hot_hashtags + evergreen_hashtags + opportunity_hashtags + proven_hashtags + weekly_hashtags
    )

    # ---------------- 4) Auto-pick hashtag theo Prompt + Data ----------------
    prompt_keywords = _extract_keywords(pb_goal, pb_audience, pb_product, pb_tone, freeform_prompt)

    def _matches_prompt(tag: str) -> bool:
        t = tag.lower().lstrip("#")
        return any(kw in t for kw in prompt_keywords)

    # Ưu tiên: match prompt trước, rồi Hot/Evergreen/Opportunity/Proven/Weekly
    prefer_auto = _dedup_ci_keep_order(
        [t for t in all_suggested if _matches_prompt(t)] +
        hot_hashtags[:10] + evergreen_hashtags[:10] + opportunity_hashtags[:10] +
        proven_hashtags[:10] + weekly_hashtags[:10]
    )[:20]

    # Tránh: các tag đang giảm (fading) + không liên quan prompt
    avoid_auto = []
    if 'mom_latest' in locals() and mom_latest is not None and not mom_latest.empty:
        fading = (
            mom_latest.sort_values("view_delta", ascending=True)
            .dropna(subset=["hashtag"]).head(30)["hashtag"].astype(str).tolist()
        )
        avoid_auto = _dedup_ci_keep_order([t for t in fading if not _matches_prompt(t)])[:20]

    st.markdown("### 🏷️ Hashtag hệ thống tự chọn")
    st.caption("**Ưu tiên** (AI sẽ cố gắng kết hợp):")
    st.write(", ".join([f"#{h.lstrip('#')}" for h in prefer_auto]) if prefer_auto else "_(không có)_")
    st.caption("**Tránh** (AI sẽ hạn chế dùng):")
    st.write(", ".join([f"#{h.lstrip('#')}" for h in avoid_auto]) if avoid_auto else "_(không có)_")

    # ---------------- 5) Tạo context tổng hợp (ép ngày -> chuỗi) ----------------
    trend_hot_df = pd.DataFrame()
    if 'mom_latest' in locals() and mom_latest is not None and not mom_latest.empty:
        trend_hot_df = mom_latest.sort_values("view_delta", ascending=False).head(10)[
            ["hashtag", "industry", "view_delta"]
        ].copy()

    # Ngành & Quốc gia
    industry_share = pd.DataFrame()
    industry_eff = pd.DataFrame()
    country_views = pd.DataFrame()

    mx2 = run_sql_safe("SELECT MAX(DATE(dt)) AS mx FROM silver.silver_trend")
    latest2 = mx2.iloc[0]["mx"] if not mx2.empty else None
    if latest2:
        extra2 = ""
        if COUNTRIES and COUNTRIES != ["ALL"]:
            extra2 += f" AND country_code IN ({_in_list_sql([c for c in COUNTRIES if c!='ALL'])})"
        if KEYWORD:
            kw2 = str(KEYWORD).lower().replace("'", "''")
            extra2 += f" AND LOWER(COALESCE(hashtag_raw, hashtag)) LIKE '%{kw2}%'"

        industry_share = run_sql_safe(f"""
            SELECT industry, SUM(view_count) AS total_views
            FROM silver.silver_trend
            WHERE DATE(dt)=DATE('{latest2}') AND industry IS NOT NULL
            {extra2}
            GROUP BY industry
            ORDER BY total_views DESC
            LIMIT 12
        """)
        industry_eff = run_sql_safe(f"""
            SELECT 
                industry, 
                SUM(view_count) AS total_views, 
                SUM(video_count) AS total_videos,
                SUM(view_count) / NULLIF(SUM(video_count), 0) AS view_per_video
            FROM silver.silver_trend
            WHERE DATE(dt)=DATE('{latest2}') AND industry IS NOT NULL AND video_count > 0
            {extra2}
            GROUP BY industry
            ORDER BY view_per_video DESC
            LIMIT 15
        """)
        country_views = run_sql_safe(f"""
            SELECT DATE(dt) dt, country_code, SUM(view_count) AS total_views
            FROM silver.silver_trend
            {build_where(dt_col='dt', country_col='country_code', industry_col=None,
                         hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
            GROUP BY 1,2
            ORDER BY 1,2
        """)
        if not country_views.empty and "dt" in country_views.columns:
            country_views["dt"] = country_views["dt"].astype(str)  # tránh lỗi JSON date

    context_payload = {
        "filters_active": {
            "start_date": str(START_DATE) if START_DATE else None,
            "end_date": str(END_DATE) if END_DATE else None,
            "countries": COUNTRIES,
            "industries": INDUSTRIES,
            "keyword": KEYWORD,
            "topn": TOPN,
        },
        "prompt_builder": {
            "goal": pb_goal or "",
            "audience": pb_audience or "",
            "product_service": pb_product or "",
            "tone": pb_tone or "",
            "kpi": pb_kpi or "",
            "constraints": pb_constraints or "",
            "freeform": freeform_prompt or "",
            "idea_count": idea_count,
            "output_language": out_lang
        },
        "hashtags": {
            "prefer": prefer_auto,
            "avoid": avoid_auto,
            "hot_top10": trend_hot_df.to_dict(orient="records") if not trend_hot_df.empty else [],
            "evergreen_top20": (
                df_ret.sort_values("streak_days", ascending=False)
                .head(20)[["hashtag", "streak_days"]].to_dict("records")
                if ('df_ret' in locals() and df_ret is not None and not df_ret.empty) else []
            ),
            "opportunity_top20": (
                pd.DataFrame({"hashtag": opportunity_hashtags[:20]}).to_dict("records")
            ),
            "proven_top100": (
                df_top100.head(100)[["hashtag", "rank", "view_count", "video_count"]].to_dict("records")
                if ('df_top100' in locals() and df_top100 is not None and not df_top100.empty) else []
            ),
            "weekly_top": weekly_hashtags[:30]
        },
        "market": {
            "industry_share_latest": industry_share.to_dict("records") if not industry_share.empty else [],
            "industry_efficiency_latest": industry_eff.to_dict("records") if not industry_eff.empty else [],
            "country_views_timeseries_head": country_views.head(50).to_dict("records") if not country_views.empty else []
        }
    }

    def _json_default(o):
        # Chuyển mọi kiểu không serializable sang chuỗi an toàn
        if isinstance(o, (_dt.date, _dt.datetime)):
            return o.isoformat()
        try:
            import numpy as _np
            if isinstance(o, (_np.integer,)):
                return int(o)
            if isinstance(o, (_np.floating,)):
                return float(o)
            if isinstance(o, (_np.ndarray,)):
                return o.tolist()
        except Exception:
            pass
        return str(o)

    system_prompt = (
        "Bạn là chiến lược gia TikTok. Hãy đọc kỹ JSON context (dữ liệu trend & cấu hình Prompt Builder) "
        "để đề xuất ý tưởng & kịch bản có tính hành động cao. "
        "Luôn gợi ý hashtag kết hợp giữa niche + trend, và nêu rõ vì sao lựa chọn đó phù hợp."
    )

    builder_prompt = f"""
[Builder]
- Mục tiêu: {pb_goal or "(chưa cung cấp)"}
- Đối tượng: {pb_audience or "(chưa cung cấp)"}
- Sản phẩm/DV: {pb_product or "(chưa cung cấp)"}
- Tông giọng: {pb_tone or "(chưa cung cấp)"}
- KPI: {pb_kpi or "(chưa cung cấp)"}
- Ràng buộc: {pb_constraints or "(chưa cung cấp)"}
- Số ý tưởng cần tạo: {idea_count}
- Ngôn ngữ đầu ra: {out_lang}
"""

    context_json_str = json.dumps(context_payload, ensure_ascii=False, default=_json_default)

    final_user_prompt = f"""
[Context JSON]
{context_json_str}

[User Prompt]
{builder_prompt}

[Yêu cầu đầu ra]
1) Phân tích kênh (từ góc nhìn thị trường & Builder): nêu niche/điểm mạnh/điểm yếu-cơ hội.
2) Đề xuất {idea_count} ý tưởng video. Mỗi ý tưởng bắt buộc có:
   - Hook ≤ 3s (rất ngắn và mạnh)
   - Visual (shot-by-shot)
   - Voiceover (ngắn, gọn)
   - CTA (cụ thể)
   - Hashtag đề xuất (kết hợp giữa hashtag ưu tiên và trend hot/evergreen phù hợp)
3) Chọn 1 ý tưởng tốt nhất và viết kịch bản chi tiết 30–45s.
4) Lịch đăng gợi ý 1 tuần (giờ vàng) + lưu ý A/B test.
Trình bày ngắn gọn, rõ ràng, có bullet.
"""

    with st.expander("👀 Xem prompt đã tổng hợp (debug)"):
        st.code(final_user_prompt, language="markdown")

    st.download_button(
        "⬇️ Tải Prompt (.txt)",
        data=final_user_prompt.encode("utf-8"),
        file_name="tiktok_prompt_compiled.txt",
        mime="text/plain"
    )
    st.download_button(
        "⬇️ Tải Context (.json)",
        data=json.dumps(context_payload, ensure_ascii=False, indent=2, default=_json_default).encode("utf-8"),
        file_name="tiktok_context.json",
        mime="application/json"
    )

    # ---------------- 6) Gọi Gemini API qua requests ----------------
    run_ai = st.button("🚀 Phân tích & Gợi ý bằng AI")
    if run_ai:
        try:
            apiKey = None
            if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
                apiKey = st.secrets["gemini"]["api_key"]

            if not apiKey or apiKey == "YOUR_API_KEY_HERE" or len(apiKey) < 30:
                st.error("Lỗi cấu hình: Không tìm thấy API Key Gemini hoặc key không hợp lệ.")
                st.info("Kiểm tra `.streamlit/secrets.toml`:\n\n[gemini]\napi_key = \"AIza...\"")
            else:
                MODEL_ID = "gemini-2.5-flash-preview-09-2025"
                apiUrl = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_ID}:generateContent?key={apiKey}"

                payload = {
                    "contents": [{"parts": [{"text": final_user_prompt}]}],
                    "systemInstruction": {"parts": [{"text": system_prompt}]},
                    "generationConfig": {"temperature": 0.7, "topP": 0.95}
                }
                headers = {'Content-Type': 'application/json'}

                with st.spinner("Đang gọi AI…"):
                    response = requests.post(apiUrl, headers=headers, json=payload, timeout=90)

                if response.status_code == 200:
                    result = response.json()
                    text_response = "Không thể lấy được phản hồi từ AI."
                    candidates = result.get("candidates")
                    if candidates and len(candidates) > 0:
                        content = candidates[0].get("content")
                        if content and 'parts' in content and len(content['parts']) > 0:
                            text_response = content['parts'][0].get('text', text_response)

                    st.markdown("### 📤 Kết quả từ AI")
                    st.markdown(text_response)
                else:
                    st.error(f"Lỗi khi gọi AI: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Lỗi trong quá trình gọi AI: {e}")

# ===== 📣 10. Phân tích Promote =====
# ===== 📣 10. Phân tích Promote =====
with tabs[9]:
    st.subheader("📣 10. Phân tích Promote (Quảng bá trả phí)")
    st.markdown(
        "Chức năng: Đo **tỷ lệ hashtag được TikTok đánh dấu là promoted/ads** theo thời gian và theo quốc gia "
        "(nếu có), rồi đưa ra **nhận định + gợi ý chiến lược**."
    )

    df_prom = pd.DataFrame()
    prom_cols = table_columns("gold.trend_promoted_share")

    # 1) Ưu tiên dùng bảng Gold nếu có (nhanh, đã tổng hợp)
    if len(prom_cols) > 0:
        has_country = "country_code" in prom_cols
        cnt_col = (
            "hashtag_cnt"
            if "hashtag_cnt" in prom_cols
            else ("total_cnt" if "total_cnt" in prom_cols else None)
        )

        where_sql = build_where(
            dt_col="dt",
            country_col="country_code" if has_country else None,
            industry_col=None,
            hashtag_expr=None,  # bảng gold không có hashtag => không lọc KEYWORD
        )

        sql_prom = ""
        if cnt_col and has_country:
            # Case 1: có country_code
            sql_prom = f"""
                SELECT
                  dt,
                  country_code,
                  {cnt_col} AS hashtag_cnt,
                  promoted_cnt,
                  promoted_share
                FROM gold.trend_promoted_share
                {where_sql}
                ORDER BY dt, country_code
            """
        elif cnt_col:
            # Case 2: chỉ tổng toàn hệ thống
            sql_prom = f"""
                SELECT
                  dt,
                  {cnt_col} AS hashtag_cnt,
                  promoted_cnt,
                  promoted_share
                FROM gold.trend_promoted_share
                {where_sql}
                ORDER BY dt
            """

        if sql_prom:
            df_prom = run_sql_safe(sql_prom)

    # 2) Fallback: tính trực tiếp từ Silver nếu Gold không có / rỗng
    if df_prom.empty:
        sql_prom_fb = f"""
        SELECT
          DATE(dt) AS dt,
          country_code,
          COUNT(*) AS hashtag_cnt,
          SUM(CASE WHEN is_promoted THEN 1 ELSE 0 END) AS promoted_cnt,
          SUM(CASE WHEN is_promoted THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS promoted_share
        FROM silver.silver_trend
        {build_where(dt_col='dt', country_col='country_code', industry_col=None,
                     hashtag_expr='COALESCE(hashtag_raw, hashtag)')}
        GROUP BY DATE(dt), country_code
        ORDER BY dt, country_code
        """
        df_prom = run_sql_safe(sql_prom_fb)

    df_prom = dedup_cols(df_prom)
    df_prom = uniquify_columns(df_prom)

    if df_prom.empty:
        st.info(
            "Không có dữ liệu Promote (chưa có cột `is_promoted` hoặc chưa có hashtag nào được đánh dấu)."
        )
    else:
        # Chuẩn hoá kiểu dữ liệu
        for col in ["hashtag_cnt", "promoted_cnt", "promoted_share"]:
            if col in df_prom.columns:
                df_prom[col] = pd.to_numeric(df_prom[col], errors="coerce")

        if "promoted_share" not in df_prom.columns and all(
            c in df_prom.columns for c in ["promoted_cnt", "hashtag_cnt"]
        ):
            df_prom["promoted_share"] = df_prom["promoted_cnt"] / df_prom["hashtag_cnt"].replace(0, pd.NA)

        df_prom["promoted_share_pct"] = df_prom["promoted_share"] * 100

        # Chuẩn hoá dt
        if "dt" in df_prom.columns:
            df_prom["dt"] = pd.to_datetime(df_prom["dt"])

        # 3) KPI: toàn bộ & 7 ngày gần nhất
        total_hashtags = df_prom["hashtag_cnt"].sum() if "hashtag_cnt" in df_prom.columns else None
        total_promoted = df_prom["promoted_cnt"].sum() if "promoted_cnt" in df_prom.columns else None
        global_share = None
        if (
            total_hashtags is not None
            and total_promoted is not None
            and total_hashtags > 0
        ):
            global_share = total_promoted / total_hashtags

        latest_dt = df_prom["dt"].max() if "dt" in df_prom.columns else None
        from datetime import timedelta

        last7_share = None
        df_last7 = pd.DataFrame()
        if latest_dt is not None:
            last7_start = latest_dt - timedelta(days=6)
            df_last7 = df_prom[(df_prom["dt"] >= last7_start) & (df_prom["dt"] <= latest_dt)]
            if not df_last7.empty and "hashtag_cnt" in df_last7.columns:
                total7 = df_last7["hashtag_cnt"].sum()
                prom7 = df_last7["promoted_cnt"].sum() if "promoted_cnt" in df_last7.columns else None
                if prom7 is not None and total7 > 0:
                    last7_share = prom7 / total7

        def classify_paid_level(x):
            import pandas as _pd
            if x is None or _pd.isna(x):
                return "Không rõ"
            if x < 0.02:
                return "✨ Chủ yếu Organic"
            if x < 0.10:
                return "⚖️ Organic + Paid cân bằng"
            return "🔥 Ads-heavy (Promote nhiều)"

        col1p, col2p, col3p = st.columns(3)
        col1p.metric(
            "Tỷ lệ promoted (toàn bộ giai đoạn)",
            f"{global_share*100:.1f}%" if global_share is not None else "N/A",
        )
        col2p.metric(
            "Tỷ lệ promoted 7 ngày gần nhất",
            f"{last7_share*100:.1f}%" if last7_share is not None else "N/A",
        )
        col3p.metric(
            "Nhận định thị trường",
            classify_paid_level(last7_share if last7_share is not None else global_share),
        )

        # 4) Biểu đồ theo thời gian
        st.markdown("#### ⏱️ Tỷ lệ hashtag promoted theo thời gian")
        if px is not None and "dt" in df_prom.columns:
            if "country_code" in df_prom.columns:
                fig_ps = px.line(
                    df_prom,
                    x="dt",
                    y="promoted_share_pct",
                    color="country_code",
                    title="% hashtag promoted (theo quốc gia)",
                    labels={
                        "promoted_share_pct": "% hashtag có flag promoted",
                        "country_code": "Quốc gia",
                    },
                )
            else:
                fig_ps = px.line(
                    df_prom,
                    x="dt",
                    y="promoted_share_pct",
                    title="% hashtag promoted (toàn hệ thống)",
                    labels={"promoted_share_pct": "% hashtag có flag promoted"},
                )
            fig_ps.update_yaxes(ticksuffix="%")
            plot_stretch(fig_ps)

        # 5) Snapshot theo ngày mới nhất (nếu có country_code)
        if "country_code" in df_prom.columns and latest_dt is not None and px is not None:
            st.markdown("#### 📍 Snapshot theo ngày mới nhất theo quốc gia")
            latest_df = df_prom[df_prom["dt"] == latest_dt].copy()
            latest_df = latest_df.sort_values("promoted_share_pct", ascending=False)
            if not latest_df.empty:
                fig_latest = px.bar(
                    latest_df,
                    x="promoted_share_pct",
                    y="country_code",
                    orientation="h",
                    title=f"Tỷ lệ hashtag promoted theo quốc gia — {latest_dt.date()}",
                    labels={
                        "promoted_share_pct": "% hashtag promoted",
                        "country_code": "Quốc gia",
                    },
                    hover_data=(
                        ["hashtag_cnt", "promoted_cnt"]
                        if "hashtag_cnt" in latest_df.columns
                        else None
                    ),
                )
                fig_latest.update_xaxes(ticksuffix="%")
                fig_latest.update_yaxes(autorange="reversed")
                plot_stretch(fig_latest)

        # 6) Gợi ý chiến lược (tự động)
        st.markdown("#### 🧠 Gợi ý chiến lược (tự động)")
        if global_share is None or (
            global_share == 0 and (last7_share is None or last7_share == 0)
        ):
            st.write(
                "- Dữ liệu hiện tại hầu như **không có hashtag Promote** → thị trường đang chủ yếu organic.\n"
                "- Hãy xem đây là **baseline**: sau này khi chạy Promote, đường biểu đồ sẽ nhảy lên để so sánh trước/sau chiến dịch.\n"
                "- Gợi ý: tập trung tối ưu **nội dung & hashtag organic** ở các tab 1–8 trước, rồi quay lại tab này để đo hiệu quả quảng cáo."
            )
        else:
            lines = []
            if last7_share is not None and global_share is not None:
                diff = last7_share - global_share
                if abs(diff) < 0.005:
                    lines.append(
                        f"- Tỷ lệ Promote 7 ngày gần đây **ổn định** quanh mức trung bình ({global_share*100:.1f}%)."
                    )
                elif diff > 0:
                    lines.append(
                        f"- Tỷ lệ Promote 7 ngày gần đây **tăng** so với trung bình (↑ {diff*100:.1f} điểm phần trăm)."
                    )
                else:
                    lines.append(
                        f"- Tỷ lệ Promote 7 ngày gần đây **giảm** so với trung bình (↓ {abs(diff)*100:.1f} điểm phần trăm)."
                    )

            if "country_code" in df_prom.columns:
                country_agg = (
                    df_prom.groupby("country_code", as_index=False)
                    .agg(
                        total_hashtag=("hashtag_cnt", "sum"),
                        total_promoted=("promoted_cnt", "sum"),
                    )
                )
                country_agg["share"] = country_agg["total_promoted"] / country_agg[
                    "total_hashtag"
                ].replace(0, pd.NA)
                country_agg = country_agg.dropna(subset=["share"]).sort_values(
                    "share", ascending=False
                )
                if not country_agg.empty:
                    top_row = country_agg.iloc[0]
                    lines.append(
                        f"- Quốc gia có tỷ lệ Promote cao nhất toàn giai đoạn: **{top_row['country_code']} — {top_row['share']*100:.1f}%**."
                    )

            lines.append(
                "- Nếu tỷ lệ Promote cao → nên tập trung ngân sách vào các hashtag **đã chứng minh hiệu quả** (tab 7) "
                "và có **Momentum tốt** (tab 2)."
            )
            lines.append(
                "- Nếu tỷ lệ Promote thấp nhưng bạn muốn push nhanh → chọn các hashtag **cơ hội** (tab 1) "
                "để chạy Promote, vì cạnh tranh còn thấp."
            )
            st.write("\n".join(lines))

        show_data_expander(df_prom, "Xem dữ liệu Promote chi tiết")
        csv_download(df_prom, "promoted_share_by_country.csv")


# ---- Gợi ý cài plotly nếu thiếu ----
if px is None:
    st.warning("Plotly chưa được cài. Chạy: pip install plotly==5.24.1 để xem biểu đồ.")
