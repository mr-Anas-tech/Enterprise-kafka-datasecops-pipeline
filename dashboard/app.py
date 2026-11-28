import io
import json
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import snowflake.connector
import streamlit as st

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import RobustScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from pptx import Presentation
    from pptx.dml.color import RGBColor
    from pptx.util import Inches, Pt
    PPTX_AVAILABLE = True
except ImportError:
    PPTX_AVAILABLE = False

# ---------------------------------------------------------
# 1. Page Config & Dark-Mode Theme (#000000 + Grey/Light-Blue)
# ---------------------------------------------------------
st.set_page_config(
    page_title="SecOps Command Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .stApp {
            background-color: #000000;
            color: #e2e8f0;
        }
        [data-testid="stSidebar"] {
            background-color: #090d14;
            border-right: 1px solid #1e293b;
        }
        .sidebar-status {
            background-color: #0f172a;
            border: 1px solid #1e293b;
            border-left: 4px solid #38bdf8;
            border-radius: 8px;
            padding: 12px 14px;
            margin-bottom: 14px;
            font-size: 0.85rem;
            color: #e2e8f0;
        }
        .sidebar-ai-box {
            background-color: #0a0f18;
            border: 1px solid #1e293b;
            border-left: 3px solid #38bdf8;
            border-radius: 8px;
            padding: 10px 12px;
            margin-bottom: 10px;
            font-size: 0.82rem;
            color: #cbd5e1;
        }
        .ai-reply-box {
            background-color: #0a0f18;
            border: 1px solid #1e293b;
            border-left: 3px solid #38bdf8;
            border-radius: 8px;
            padding: 11px 13px;
            margin-top: 8px;
            font-size: 0.84rem;
            line-height: 1.5;
            color: #e2e8f0;
        }
        .page-banner {
            background-color: #0a0f18;
            border: 1px solid #1e293b;
            border-left: 4px solid #38bdf8;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 16px;
        }
        .page-kicker {
            color: #7dd3fc;
            font-size: 0.78rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .page-heading {
            color: #f8fafc;
            font-size: 1.55rem;
            font-weight: 700;
            margin: 4px 0 0 0;
        }
        .page-desc {
            color: #94a3b8;
            font-size: 0.9rem;
            margin-top: 4px;
        }
        [data-testid="stMetric"] {
            background-color: #0a0f18;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 12px 14px;
            min-height: 98px;
        }
        [data-testid="stMetricLabel"] {
            color: #94a3b8;
            font-size: 0.8rem !important;
            white-space: normal !important;
            line-height: 1.25 !important;
        }
        [data-testid="stMetricLabel"] > div,
        [data-testid="stMetricLabel"] p {
            white-space: normal !important;
            overflow: visible !important;
            text-overflow: unset !important;
        }
        [data-testid="stMetricValue"] {
            color: #7dd3fc;
            font-size: clamp(1.1rem, 1.45vw, 1.5rem) !important;
            line-height: 1.2 !important;
            white-space: nowrap !important;
        }
        [data-testid="stMetricValue"] > div {
            overflow: visible !important;
            text-overflow: unset !important;
        }
        .chart-note {
            color: #cbd5e1;
            font-size: 0.85rem;
            line-height: 1.55;
            background-color: #090d14;
            border: 1px solid #1e293b;
            border-left: 3px solid #38bdf8;
            border-radius: 6px;
            padding: 10px 14px;
            margin-top: -4px;
            margin-bottom: 18px;
        }
        .chart-note strong {
            color: #7dd3fc;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def apply_dark_style(fig, height=360):
    fig.update_layout(
        height=height,
        template="plotly_dark",
        paper_bgcolor="#000000",
        plot_bgcolor="#090d14",
        font=dict(color="#e2e8f0", size=12),
        margin=dict(l=18, r=18, t=48, b=18),
        legend=dict(
            bgcolor="rgba(9, 13, 20, 0.8)",
            bordercolor="#1e293b",
            borderwidth=1,
        ),
    )
    return fig


def render_plain_note(what_it_shows: str, what_it_means: str):
    st.markdown(
        f"""
        <div class="chart-note">
            <strong>What It Shows:</strong> {what_it_shows}<br>
            <strong>What It Means:</strong> {what_it_means}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------
# 2. Snowflake Connection & Mart Loader
# ---------------------------------------------------------
def get_snowflake_connection():
    sf = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=sf["user"],
        password=sf["password"],
        account=sf["account"],
        warehouse=sf["warehouse"],
        database=sf["database"],
        schema=sf["schema"],
    )


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df


@st.cache_data(ttl=300)
def load_secops_marts():
    conn = get_snowflake_connection()
    try:
        minute_df = pd.read_sql(
            "SELECT * FROM SECOPPS_DB.DBT_SECOPS.MARTS_SECOPS_MINUTES_ANOMALY_FAETURES",
            conn,
        )
        hourly_df = pd.read_sql(
            "SELECT * FROM SECOPPS_DB.DBT_SECOPS.MARTS_SECOPS_HOURLY_TRAFFIC_SUMMARY",
            conn,
        )
        ip_df = pd.read_sql(
            "SELECT * FROM SECOPPS_DB.DBT_SECOPS.MARTS_SECOPS_IP_THREAT_PROFILE",
            conn,
        )
    finally:
        conn.close()

    return _clean_df(minute_df), _clean_df(hourly_df), _clean_df(ip_df)


# ---------------------------------------------------------
# 3. AI Insights Generator (Groq + Rule Fallback) & PPTX Builder
# ---------------------------------------------------------
def call_groq_chat(system_prompt: str, user_prompt: str, max_tokens: int = 450) -> str:
    api_key = st.secrets.get("GROQ_API_KEY", "")
    if not api_key:
        return ""

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code != 200:
            return ""
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()
    except Exception:
        return ""


def build_secops_context(h_df: pd.DataFrame, i_df: pd.DataFrame, m_df: pd.DataFrame) -> dict:
    summary = {}

    if not h_df.empty:
        h_tmp = h_df.copy()
        if "event_hour" in h_tmp.columns:
            h_tmp["event_hour"] = pd.to_datetime(h_tmp["event_hour"], errors="coerce")
        peak_idx = h_tmp["total_events"].idxmax() if "total_events" in h_tmp.columns else None
        summary["hourly"] = {
            "hours_count": int(len(h_tmp)),
            "total_events": int(h_tmp["total_events"].sum()) if "total_events" in h_tmp.columns else 0,
            "total_failed_logins": int(h_tmp["failed_login_attempts_count"].sum()) if "failed_login_attempts_count" in h_tmp.columns else 0,
            "avg_response_ms": round(float(h_tmp["avg_response_time_ms"].mean()), 2) if "avg_response_time_ms" in h_tmp.columns else 0,
            "peak_hour": str(h_tmp.loc[peak_idx, "event_hour"])[:16] if peak_idx is not None and "event_hour" in h_tmp.columns else "N/A",
            "peak_events": int(h_tmp.loc[peak_idx, "total_events"]) if peak_idx is not None else 0,
        }

    if not i_df.empty:
        top_ips = i_df.sort_values("failed_login_attempts_count", ascending=False).head(5)
        top_countries = (
            i_df.groupby("country_name", dropna=False)["failed_login_attempts_count"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )
        summary["ip_threat"] = {
            "tracked_ips": int(len(i_df)),
            "total_requests": int(i_df["total_requests"].sum()) if "total_requests" in i_df.columns else 0,
            "total_failed_logins": int(i_df["failed_login_attempts_count"].sum()) if "failed_login_attempts_count" in i_df.columns else 0,
            "top_5_ips": [
                {
                    "ip": str(r.get("source_ip", "")),
                    "country": str(r.get("country_name", "")),
                    "failed_logins": int(r.get("failed_login_attempts_count", 0)),
                    "admin_hits": int(r.get("admin_endpoint_hits", 0)),
                }
                for _, r in top_ips.iterrows()
            ],
            "top_countries_failed_logins": {str(k): int(v) for k, v in top_countries.items()},
        }

    if not m_df.empty:
        m_tmp = m_df.copy()
        for col in ["failed_login_rate", "admin_endpoint_hits", "total_events", "traffic_spike_ratio", "latency_spike_ratio"]:
            if col in m_tmp.columns:
                m_tmp[col] = pd.to_numeric(m_tmp[col], errors="coerce").fillna(0)
        t_ev = m_tmp["total_events"] if "total_events" in m_tmp.columns else pd.Series(1, index=m_tmp.index)
        a_share = (m_tmp.get("admin_endpoint_hits", 0) / t_ev.replace(0, np.nan)).fillna(0)
        m_tmp["threat_score"] = (
            (m_tmp.get("failed_login_rate", pd.Series(0, index=m_tmp.index)).rank(pct=True) * 0.40)
            + (a_share.rank(pct=True) * 0.20)
            + (m_tmp.get("traffic_spike_ratio", pd.Series(1, index=m_tmp.index)).rank(pct=True) * 0.25)
            + (m_tmp.get("latency_spike_ratio", pd.Series(1, index=m_tmp.index)).rank(pct=True) * 0.15)
        ) * 100
        summary["minute_anomaly"] = {
            "monitored_minutes": int(len(m_tmp)),
            "high_threat_minutes_ge_85": int((m_tmp["threat_score"] >= 85).sum()),
            "peak_threat_score": round(float(m_tmp["threat_score"].max()), 1),
            "max_traffic_spike": round(float(m_tmp["traffic_spike_ratio"].max()), 2) if "traffic_spike_ratio" in m_tmp.columns else 0,
            "max_latency_spike": round(float(m_tmp["latency_spike_ratio"].max()), 2) if "latency_spike_ratio" in m_tmp.columns else 0,
        }

    return summary


@st.cache_data(ttl=600)
def generate_ai_page_summary(page_name: str, context_json_str: str) -> str:
    ctx = json.loads(context_json_str)
    system_prompt = (
        "You are a senior SecOps analyst writing clear, plain-English executive summaries for dashboards and presentations. "
        "Summarize the cards, charts, and security risks concisely in English. "
        "Use short bullet points: Key Findings, Risk Signals, and Recommended Action. Keep it under 160 words."
    )
    user_prompt = (
        f"Generate an automatic summary of all cards and charts for: {page_name}\n"
        f"Data Summary:\n{json.dumps(ctx, indent=2)}"
    )
    ai_text = call_groq_chat(system_prompt, user_prompt, max_tokens=380)
    if ai_text:
        return ai_text

    h = ctx.get("hourly", {})
    ip = ctx.get("ip_threat", {})
    mn = ctx.get("minute_anomaly", {})
    top_ip = ip.get("top_5_ips", [{}])[0] if ip.get("top_5_ips") else {}
    return (
        f"- **Key Findings ({page_name}):** Total hourly events reached {h.get('total_events', 0):,} with peak traffic at {h.get('peak_hour', 'N/A')}; "
        f"{ip.get('tracked_ips', 0):,} source IPs and {mn.get('monitored_minutes', 0):,} monitored minutes were analyzed.\n"
        f"- **Risk Signals:** Highest failed-login IP is {top_ip.get('ip', 'N/A')} ({top_ip.get('country', 'N/A')}) with {top_ip.get('failed_logins', 0):,} failed logins; "
        f"minute threat score peaked at {mn.get('peak_threat_score', 0)} with {mn.get('high_threat_minutes_ge_85', 0)} high-threat minutes.\n"
        f"- **Recommended Action:** Review high-failure IPs, verify off-peak traffic spikes, and inspect minutes where both ML and rule scores spike together."
    )


def build_secops_pptx(ctx: dict, page_summaries: dict) -> bytes:
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6]

    def add_dark_slide(title_text: str, subtitle_text: str, body_bullets: list[str]):
        slide = prs.slides.add_slide(blank_layout)
        bg = slide.shapes.add_shape(1, Inches(0), Inches(0), Inches(13.333), Inches(7.5))
        bg.fill.solid()
        bg.fill.fore_color.rgb = RGBColor(0x05, 0x08, 0x0F)
        bg.line.fill.background()

        bar = slide.shapes.add_shape(1, Inches(0.6), Inches(0.55), Inches(0.12), Inches(0.85))
        bar.fill.solid()
        bar.fill.fore_color.rgb = RGBColor(0x38, 0xBD, 0xF8)
        bar.line.fill.background()

        t_box = slide.shapes.add_textbox(Inches(0.9), Inches(0.45), Inches(11.8), Inches(0.55))
        tf = t_box.text_frame
        tf.word_wrap = True
        p0 = tf.paragraphs[0]
        p0.text = title_text
        p0.font.size = Pt(28)
        p0.font.bold = True
        p0.font.color.rgb = RGBColor(0xF8, 0xFA, 0xFC)

        s_box = slide.shapes.add_textbox(Inches(0.9), Inches(1.0), Inches(11.8), Inches(0.4))
        stf = s_box.text_frame
        stf.word_wrap = True
        ps = stf.paragraphs[0]
        ps.text = subtitle_text
        ps.font.size = Pt(13)
        ps.font.color.rgb = RGBColor(0x7D, 0xD3, 0xFC)

        b_box = slide.shapes.add_textbox(Inches(0.9), Inches(1.65), Inches(11.5), Inches(5.2))
        btf = b_box.text_frame
        btf.word_wrap = True
        for idx, bullet in enumerate(body_bullets):
            p = btf.paragraphs[0] if idx == 0 else btf.add_paragraph()
            p.text = f"• {bullet}"
            p.font.size = Pt(16)
            p.font.color.rgb = RGBColor(0xE2, 0xE8, 0xF0)
            p.space_after = Pt(14)

    h = ctx.get("hourly", {})
    ip = ctx.get("ip_threat", {})
    mn = ctx.get("minute_anomaly", {})

    add_dark_slide(
        "SecOps Command Center — Executive Overview",
        "Automated AI Summary Across Hourly Traffic, IP Threats & Minute Anomalies",
        [
            f"Hourly Traffic: {h.get('total_events', 0):,} total events across {h.get('hours_count', 0):,} hours (Peak: {h.get('peak_events', 0):,} events at {h.get('peak_hour', 'N/A')}).",
            f"IP Threat Landscape: {ip.get('tracked_ips', 0):,} tracked IPs generating {ip.get('total_failed_logins', 0):,} total failed login attempts.",
            f"Minute Anomalies: {mn.get('monitored_minutes', 0):,} monitored minutes, {mn.get('high_threat_minutes_ge_85', 0):,} high-threat minutes (>=85), peak score {mn.get('peak_threat_score', 0)}.",
            f"Peak Multipliers: Max traffic spike {mn.get('max_traffic_spike', 0)}x and max latency spike {mn.get('max_latency_spike', 0)}x.",
        ],
    )

    for view_title in [
        "Page 1: Hourly Traffic Summary",
        "Page 2: IP Threat Profile",
        "Page 3: Minute Anomaly Features",
        "Page 4: ML Anomaly Detection",
    ]:
        raw_sum = page_summaries.get(view_title, "Summary unavailable.")
        lines = [line.lstrip("-•* ").strip() for line in raw_sum.splitlines() if line.strip()]
        add_dark_slide(
            view_title,
            "AI-Generated Card & Chart Summary",
            lines[:5] if lines else [raw_sum],
        )

    out = io.BytesIO()
    prs.save(out)
    out.seek(0)
    return out.getvalue()


try:
    minute_df, hourly_df, ip_df = load_secops_marts()
    st.session_state["minute_df"] = minute_df
    st.session_state["hourly_df"] = hourly_df
    st.session_state["ip_df"] = ip_df
    data_status = "Snowflake Live Marts Connected"
except Exception as e:
    st.error(f"Snowflake load error: {e}")
    st.stop()


# ---------------------------------------------------------
# 4. Sidebar Navigation, AI Agent & PPTX Generator
# ---------------------------------------------------------
with st.sidebar:
    st.markdown("### 🛡️ SecOps Command")
    st.markdown(
        f"""
        <div class="sidebar-status">
            <strong>Data Source:</strong> {data_status}<br>
            <span style="color:#94a3b8;font-size:0.78rem;">4 Isolated SecOps Views</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    selected_page = st.selectbox(
        "Select SecOps View",
        [
            "Page 1: Hourly Traffic Summary",
            "Page 2: IP Threat Profile",
            "Page 3: Minute Anomaly Features",
            "Page 4: ML Anomaly Detection",
        ],
        index=0,
    )

    st.divider()
    st.markdown("#### 📊 Auto AI Summary & Deck")
    secops_ctx = build_secops_context(hourly_df, ip_df, minute_df)
    secops_ctx_json = json.dumps(secops_ctx, sort_keys=True)

    if st.button("Generate AI Summaries (All 4 Views)", use_container_width=True):
        with st.spinner("Generating AI insights for all views..."):
            all_views = [
                "Page 1: Hourly Traffic Summary",
                "Page 2: IP Threat Profile",
                "Page 3: Minute Anomaly Features",
                "Page 4: ML Anomaly Detection",
            ]
            st.session_state["ai_page_summaries"] = {
                v: generate_ai_page_summary(v, secops_ctx_json) for v in all_views
            }

    if "ai_page_summaries" not in st.session_state:
        st.session_state["ai_page_summaries"] = {
            selected_page: generate_ai_page_summary(selected_page, secops_ctx_json)
        }
    elif selected_page not in st.session_state["ai_page_summaries"]:
        st.session_state["ai_page_summaries"][selected_page] = generate_ai_page_summary(
            selected_page, secops_ctx_json
        )

    if PPTX_AVAILABLE:
        pptx_bytes = build_secops_pptx(secops_ctx, st.session_state["ai_page_summaries"])
        st.download_button(
            label="📥 Download PPTX Presentation",
            data=pptx_bytes,
            file_name="SecOps_Command_Center_Report.pptx",
            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
            use_container_width=True,
        )
    else:
        st.caption("Install `python-pptx` to enable `.pptx` export.")

    st.divider()
    st.markdown("#### 🤖 SecOps AI Q&A (Groq)")
    groq_ready = bool(st.secrets.get("GROQ_API_KEY", ""))
    st.markdown(
        f"""
        <div class="sidebar-ai-box">
            <strong>Engine:</strong> {'Groq Live (gpt-oss-120b)' if groq_ready else 'Fallback Mode (Add GROQ_API_KEY)'}<br>
            <span style="color:#94a3b8;">Ask about risky IPs, spikes, or anomalies.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    user_q = st.text_input(
        "Ask SecOps AI",
        placeholder="e.g. Which IP looks riskiest right now?",
        label_visibility="collapsed",
    )

    if st.button("Ask AI", use_container_width=True) and user_q.strip():
        with st.spinner("Analyzing SecOps marts..."):
            ans = call_groq_chat(
                "You are a concise, plain-English SecOps assistant. Answer only from the provided live summary.",
                f"View: {selected_page}\nSummary:\n{secops_ctx_json}\nQuestion: {user_q.strip()}",
                max_tokens=300,
            )
            st.session_state["ai_last_reply"] = ans or "Groq API key missing or unavailable; check `.streamlit/secrets.toml`."

    if st.session_state.get("ai_last_reply"):
        st.markdown(
            f'<div class="ai-reply-box">{st.session_state["ai_last_reply"]}</div>',
            unsafe_allow_html=True,
        )


def render_auto_ai_summary_banner(view_name: str):
    summary_text = st.session_state.get("ai_page_summaries", {}).get(view_name, "")
    if not summary_text:
        summary_text = generate_ai_page_summary(view_name, secops_ctx_json)
        st.session_state["ai_page_summaries"][view_name] = summary_text

    st.markdown("#### ✨ AI Card & Chart Summary")
    st.markdown(summary_text)


# =========================================================
# PAGE 1: HOURLY TRAFFIC SUMMARY
# =========================================================
if selected_page == "Page 1: Hourly Traffic Summary":
    st.markdown(
        """
        <div class="page-banner">
            <div class="page-kicker">Page 1 of 4 • Hourly Mart</div>
            <h1 class="page-heading">Hourly Traffic Summary</h1>
            <div class="page-desc">Hourly event volume, HTTP status distribution, server latency, and login/endpoint risk trends.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    h_df = st.session_state.get("hourly_df", hourly_df).copy()
    if "event_hour" in h_df.columns:
        h_df["event_hour"] = pd.to_datetime(h_df["event_hour"], errors="coerce")
        h_df = h_df.sort_values("event_hour").reset_index(drop=True)
        if "event_date" not in h_df.columns:
            h_df["event_date"] = h_df["event_hour"].dt.date
        h_df["hour_of_day"] = h_df["event_hour"].dt.hour
        h_df["events_per_ip"] = (h_df["total_events"] / h_df["unique_source_ips"].replace(0, np.nan)).fillna(0).round(2)
        h_df["success_2xx_pct"] = (h_df["success_2xx_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)
        h_df["redirect_3xx_pct"] = (h_df["redirect_3xx_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)
        h_df["client_error_4xx_pct"] = (h_df["client_error_4xx_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)
        h_df["slow_response_pct"] = (h_df["slow_response_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)
        h_df["sensitive_endpoint_pct"] = (h_df["sensitive_endpoint_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)
        h_df["failed_login_attempts_pct"] = (h_df["failed_login_attempts_count"] / h_df["total_events"].replace(0, np.nan) * 100).fillna(0).round(2)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Events", f"{h_df['total_events'].sum():,.0f}")
    k2.metric("Avg Events / IP", f"{h_df['events_per_ip'].mean():.2f}")
    k3.metric("Avg Failed Login %", f"{h_df['failed_login_attempts_pct'].mean():.2f}%")
    k4.metric("Avg Slow Response %", f"{h_df['slow_response_pct'].mean():.2f}%")

    render_auto_ai_summary_banner(selected_page)

    peak_row = h_df.loc[h_df["total_events"].idxmax()] if not h_df.empty else None
    peak_time_str = str(peak_row["event_hour"])[:16] if peak_row is not None else "N/A"
    peak_events_val = f"{peak_row['total_events']:,.0f}" if peak_row is not None else "0"

    c1, c2 = st.columns(2)

    with c1:
        fig_vol = px.bar(
            h_df,
            x="event_hour",
            y="total_events",
            color="failed_login_attempts_count",
            title="Hourly Event Volume & Failed Login Intensity",
            labels={
                "event_hour": "Event Hour",
                "total_events": "Total Events",
                "failed_login_attempts_count": "Failed Logins",
            },
        )
        st.plotly_chart(apply_dark_style(fig_vol), use_container_width=True)
        render_plain_note(
            f"How many requests hit the system each hour and how many login attempts failed. The highest traffic was <b>{peak_events_val} events</b> at <b>{peak_time_str}</b>.",
            "Tall, bright bars point to hours with heavy activity or many failed logins, which can mean normal peak traffic or automated login guessing."
        )

    with c2:
        fig_scatter = px.scatter(
            h_df,
            x="total_events",
            y="avg_response_time_ms",
            color="slow_response_count",
            title="Traffic Volume vs. Average Server Response Time (ms)",
            labels={
                "total_events": "Total Events",
                "avg_response_time_ms": "Avg Response Time (ms)",
                "slow_response_count": "Slow Responses",
            },
        )
        fig_scatter.update_traces(marker=dict(size=9, opacity=0.88))
        st.plotly_chart(apply_dark_style(fig_scatter), use_container_width=True)
        render_plain_note(
            f"How server response time changes as traffic grows, with an average of <b>{h_df['avg_response_time_ms'].mean():.1f} ms</b>.",
            "Points higher up mean the server took longer to reply under load; points lower down show fast, healthy response times."
        )

    c3, c4 = st.columns(2)

    with c3:
        fig_status = px.bar(
            h_df,
            x="event_hour",
            y=["success_2xx_count", "redirect_3xx_count", "client_error_4xx_count"],
            title="Hourly HTTP Status Breakdown (2xx vs. 3xx vs. 4xx)",
            labels={"event_hour": "Event Hour", "value": "Event Count", "variable": "HTTP Status"},
        )
        st.plotly_chart(apply_dark_style(fig_status), use_container_width=True)
        render_plain_note(
            f"On average, <b>{h_df['success_2xx_pct'].mean():.1f}%</b> of requests succeeded (2xx) while <b>{h_df['client_error_4xx_pct'].mean():.1f}%</b> returned client errors (4xx).",
            "2xx means normal page loads. A sudden jump in 4xx can indicate broken links, invalid requests, or unauthorized access attempts."
        )

    with c4:
        fig_risk = px.line(
            h_df,
            x="event_hour",
            y=["total_events", "sensitive_endpoint_count", "failed_login_attempts_count"],
            title="Total Traffic vs. Sensitive Endpoints & Failed Logins",
            labels={"event_hour": "Event Hour", "value": "Event Count", "variable": "Metric"},
        )
        st.plotly_chart(apply_dark_style(fig_risk), use_container_width=True)
        render_plain_note(
            "How sensitive page hits and failed logins move compared to overall hourly traffic.",
            "If failed logins or sensitive page hits rise without a matching rise in normal traffic, someone may be targeting logins or protected areas."
        )

    c5, c6 = st.columns([1.25, 1])

    with c5:
        fig_hod = px.bar(
            h_df,
            x="hour_of_day",
            y="total_events",
            color="event_date",
            barmode="group",
            title="Traffic Distribution by Hour of Day Across Dates",
            labels={"hour_of_day": "Hour of Day (0–23)", "total_events": "Total Events", "event_date": "Date"},
        )
        st.plotly_chart(apply_dark_style(fig_hod, height=370), use_container_width=True)
        render_plain_note(
            "Traffic across each hour of the day (0 to 23) for every recorded date.",
            "This helps separate normal daily busy hours from unusual off-peak spikes that may come from automated scripts."
        )

    rate_cols = [
        "events_per_ip",
        "success_2xx_pct",
        "redirect_3xx_pct",
        "client_error_4xx_pct",
        "slow_response_pct",
        "sensitive_endpoint_pct",
        "failed_login_attempts_pct",
    ]

    with c6:
        st.subheader("Hourly Rate & Ratio Summary")
        if rate_cols:
            st.dataframe(
                h_df[rate_cols].describe().T[["min", "max", "50%", "mean", "std"]].round(3),
                use_container_width=True,
                height=330,
            )
        render_plain_note(
            "Minimum, median (50%), average (mean), and maximum values for key traffic and risk percentages.",
            "Use this table as a quick reference to see what typical behavior looks like versus extreme peaks."
        )

    daily_cols = [
        "total_events",
        "sensitive_endpoint_count",
        "failed_login_attempts_count",
        "slow_response_count",
    ]
    available_daily_cols = [c for c in daily_cols if c in h_df.columns]

    if "event_date" in h_df.columns and available_daily_cols:
        daily_summary_df = h_df.groupby("event_date", as_index=False)[available_daily_cols].sum()
        st.subheader("Daily Aggregated Security & Traffic Summary")
        st.dataframe(daily_summary_df, use_container_width=True)
        render_plain_note(
            f"Day-by-day totals for traffic, sensitive hits, failed logins, and slow responses across <b>{len(daily_summary_df)} days</b>.",
            "This makes it easy to spot which specific dates had the heaviest traffic or highest security/latency pressure."
        )


# =========================================================
# PAGE 2: IP THREAT PROFILE
# =========================================================
elif selected_page == "Page 2: IP Threat Profile":
    st.markdown(
        """
        <div class="page-banner">
            <div class="page-kicker">Page 2 of 4 • IP Threat Mart</div>
            <h1 class="page-heading">IP Threat Profile</h1>
            <div class="page-desc">Source IP risk profiling across countries, failed logins, admin/sensitive endpoints, and latency.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    i_df = st.session_state.get("ip_df", ip_df).copy()
    i_df.columns = [str(c).lower().strip() for c in i_df.columns]

    if not i_df.empty:
        i_df["failed_login_attempts_pct"] = (
            (i_df["failed_login_attempts_count"] / i_df["total_requests"].replace(0, pd.NA)) * 100
        ).fillna(0).round(3)

        i_df["sensitive_ratio_pct"] = (
            (i_df["sensitive_endpoint_count"] / i_df["total_requests"].replace(0, pd.NA)) * 100
        ).fillna(0).round(3)

        p2_k1, p2_k2, p2_k3, p2_k4 = st.columns(4)
        p2_k1.metric("Tracked Source IPs", f"{len(i_df):,}")
        p2_k2.metric("Total IP Requests", f"{i_df['total_requests'].sum():,.0f}" if "total_requests" in i_df.columns else "N/A")
        p2_k3.metric(
            "Total Failed Logins",
            f"{i_df['failed_login_attempts_count'].sum():,.0f}"
            if "failed_login_attempts_count" in i_df.columns
            else "N/A",
        )
        p2_k4.metric(
            "Avg IP Response (ms)",
            f"{i_df['avg_response_time_ms'].mean():.2f}"
            if "avg_response_time_ms" in i_df.columns
            else "N/A",
        )

        render_auto_ai_summary_banner(selected_page)

        country_summary = (
            i_df.groupby(["country_name", "country_code"], as_index=False, dropna=False)
            .agg(
                ip_count=("source_ip", "count"),
                failed_login=("failed_login_attempts_count", "sum"),
                total_requests=("total_requests", "sum"),
                slow_response=("slow_response_count", "sum"),
                sensitive_hits=("sensitive_endpoint_count", "sum"),
                admin_hits=("admin_endpoint_hits", "sum"),
            )
            .sort_values("total_requests", ascending=False)
        )

        top_country = country_summary.iloc[0]["country_name"] if not country_summary.empty else "N/A"
        top_country_fails = f"{country_summary.iloc[0]['failed_login']:,.0f}" if not country_summary.empty else "0"

        c2_1, c2_2 = st.columns([1.15, 1])

        with c2_1:
            fig_country = px.bar(
                country_summary.head(20),
                x="country_name",
                y="total_requests",
                color="failed_login",
                title="Top Countries by Total Requests (Color = Failed Logins)",
                labels={
                    "country_name": "Country",
                    "total_requests": "Total Requests",
                    "failed_login": "Failed Logins",
                },
            )
            st.plotly_chart(apply_dark_style(fig_country, height=370), use_container_width=True)
            render_plain_note(
                f"Which countries generate the most requests and failed logins. <b>{top_country}</b> leads with <b>{top_country_fails}</b> failed logins.",
                "Countries with high requests and bright failed-login shading deserve closer review, especially if you do not expect active users from that region."
            )

        with c2_2:
            st.subheader("Country Threat Summary")
            st.dataframe(country_summary, use_container_width=True, height=330)
            render_plain_note(
                "A country-level breakdown of active IPs, total requests, failed logins, slow responses, and admin/sensitive hits.",
                "This helps compare overall traffic volume against risky login or endpoint activity by country."
            )

        c2_3, c2_4 = st.columns([1.25, 1])

        with c2_3:
            size_series = (
                i_df["slow_response_count"].clip(lower=0).fillna(0)
                if "slow_response_count" in i_df.columns
                else None
            )
            fig_ip_scatter = px.scatter(
                i_df,
                x="failed_login_attempts_count",
                y="avg_response_time_ms",
                color="country_name",
                size=size_series,
                size_max=22,
                hover_data=["source_ip", "total_requests", "admin_endpoint_hits", "failed_login_attempts_pct"],
                title="IP Threat Profile: Failed Logins vs. Avg Response Time (Size = Slow Responses)",
                labels={
                    "failed_login_attempts_count": "Failed Login Attempts",
                    "avg_response_time_ms": "Avg Response Time (ms)",
                    "country_name": "Country",
                },
            )
            st.plotly_chart(apply_dark_style(fig_ip_scatter, height=370), use_container_width=True)
            render_plain_note(
                "Each IP's failed login count, average server response time, and slow-response volume (bubble size).",
                "Large bubbles further to the right represent IPs with many failed logins that also experience high slow-response counts."
            )

        with c2_4:
            st.subheader("Failed Login Rate Distribution")
            zero_pct_ips = int((i_df["failed_login_attempts_pct"] == 0).sum())
            full_pct_ips = int((i_df["failed_login_attempts_pct"] == 100).sum())
            mid_pct_ips = int(
                ((i_df["failed_login_attempts_pct"] > 0) & (i_df["failed_login_attempts_pct"] < 100)).sum()
            )

            b1, b2, b3 = st.columns(3)
            b1.metric("0% Failed IPs", f"{zero_pct_ips:,}")
            b2.metric("0–100% Partial", f"{mid_pct_ips:,}")
            b3.metric("100% Failed IPs", f"{full_pct_ips:,}")

            st.dataframe(
                i_df["failed_login_attempts_pct"].describe().to_frame().round(3),
                use_container_width=True,
                height=245,
            )
            render_plain_note(
                f"Out of all tracked IPs, <b>{zero_pct_ips:,}</b> had 0% failed logins while <b>{full_pct_ips:,}</b> had 100% failed logins.",
                "IPs with 100% failed logins often indicate automated login attempts or credential-stuffing bots rather than normal users."
            )

        st.subheader("Top 15 High-Risk IPs (Failed Logins & Admin Hits)")
        top_ips = i_df.sort_values(
            ["failed_login_attempts_count", "admin_endpoint_hits"], ascending=False
        )[
            [
                "source_ip",
                "country_name",
                "total_requests",
                "failed_login_attempts_count",
                "failed_login_attempts_pct",
                "admin_endpoint_hits",
                "sensitive_endpoint_count",
                "slow_response_count",
                "avg_response_time_ms",
            ]
        ].head(15)
        st.dataframe(top_ips, use_container_width=True)
        render_plain_note(
            "The 15 source IPs with the highest counts of failed logins and admin endpoint hits.",
            "These addresses are prime candidates for rate limiting, MFA enforcement, or firewall blocking after verification."
        )

        c2_5, c2_6 = st.columns(2)

        with c2_5:
            fig_admin_scatter = px.scatter(
                i_df,
                x="admin_endpoint_hits",
                y="sensitive_ratio_pct",
                color="country_name",
                hover_data=["source_ip", "total_requests", "failed_login_attempts_count"],
                title="Admin Endpoint Hits vs. Sensitive Endpoint Ratio (%)",
                labels={
                    "admin_endpoint_hits": "Admin Endpoint Hits",
                    "sensitive_ratio_pct": "Sensitive Endpoint Ratio (%)",
                    "country_name": "Country",
                },
            )
            st.plotly_chart(apply_dark_style(fig_admin_scatter, height=370), use_container_width=True)
            render_plain_note(
                "Which IPs hit admin routes most often and what share of their traffic goes to sensitive endpoints.",
                "High admin hits or a high sensitive ratio can signal automated scanning or probing of restricted areas."
            )

        with c2_6:
            fig_lat_hist = px.histogram(
                i_df,
                x="avg_response_time_ms",
                color="country_name",
                nbins=30,
                title="Distribution of IP Average Response Time (ms) by Country",
                labels={
                    "avg_response_time_ms": "Avg Response Time (ms)",
                    "country_name": "Country",
                },
            )
            st.plotly_chart(apply_dark_style(fig_lat_hist, height=370), use_container_width=True)
            render_plain_note(
                "How average response times are spread across IPs from different countries.",
                "Most IPs cluster in a normal range, while outliers further right experienced noticeably slower responses."
            )

        c2_7, c2_8 = st.columns(2)

        with c2_7:
            st.subheader("Top 15 Admin & Sensitive Endpoint IPs")
            top_admin_ips = i_df.sort_values(
                ["admin_endpoint_hits", "sensitive_ratio_pct"], ascending=False
            )[
                [
                    "source_ip",
                    "country_name",
                    "total_requests",
                    "failed_login_attempts_count",
                    "failed_login_attempts_pct",
                    "admin_endpoint_hits",
                    "sensitive_endpoint_count",
                    "slow_response_count",
                    "avg_response_time_ms",
                ]
            ].head(15)
            st.dataframe(top_admin_ips, use_container_width=True)
            render_plain_note(
                "The top 15 IPs with the highest admin endpoint activity and sensitive endpoint share.",
                "Repeated requests to admin or sensitive routes warrant extra access-log inspection."
            )

        with c2_8:
            st.subheader("Top 15 Slow-Response Source IPs")
            slow_ips = i_df.sort_values(
                ["slow_response_count", "avg_response_time_ms"], ascending=False
            )[
                [
                    "source_ip",
                    "country_name",
                    "total_requests",
                    "failed_login_attempts_count",
                    "admin_endpoint_hits",
                    "sensitive_endpoint_count",
                    "slow_response_count",
                    "avg_response_time_ms",
                ]
            ].head(15)
            st.dataframe(slow_ips, use_container_width=True)
            render_plain_note(
                "The top 15 IPs associated with the highest slow-response counts and latency.",
                "These sources may be sending heavy requests or hitting slow endpoints during peak load."
            )

        with st.expander("Full IP Mart Statistical Summary (describe().T)", expanded=False):
            st.dataframe(i_df.select_dtypes(include=[np.number]).describe().T.round(3), use_container_width=True)
            render_plain_note(
                "Complete statistical summary across all numeric IP profile columns.",
                "Helpful for checking baseline averages, spread, and extreme values during an audit."
            )


# =========================================================
# PAGE 3: MINUTE ANOMALY FEATURES
# =========================================================
elif selected_page == "Page 3: Minute Anomaly Features":
    def _format_minute_display(series: pd.Series) -> pd.Series:
        dt = pd.to_datetime(series, errors="coerce")
        if dt.notna().sum() > 0:
            return dt.dt.strftime("%H:%M")
        num = pd.to_numeric(series, errors="coerce")
        if num.notna().sum() > 0:
            return num.fillna(0).astype(int).astype(str)
        return series.astype(str)

    def _format_ts_display(series: pd.Series) -> pd.Series:
        dt = pd.to_datetime(series, errors="coerce")
        return dt.dt.strftime("%Y-%m-%d %H:%M")

    st.markdown(
        """
        <div class="page-banner">
            <div class="page-kicker">Page 3 of 4 • Minute Mart</div>
            <h1 class="page-heading">Minute Anomaly Features</h1>
            <div class="page-desc">Minute-level traffic spikes, anomaly indicators, rolling baselines, and time-series security signals.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    m_df = st.session_state.get("minute_df", minute_df).copy()
    m_df.columns = [str(c).lower().strip() for c in m_df.columns]

    numeric_candidates = [
        "event_hour", "event_minute", "total_events", "unique_source_ips",
        "unique_endpoints", "avg_requests_per_ip", "error_4xx_count",
        "error_5xx_count", "sensitive_endpoint_hits", "admin_endpoint_hits",
        "slow_response_count", "error_4xx_rate", "error_5xx_rate",
        "sensitive_endpoint_rate", "failed_login_count", "failed_login_rate",
        "avg_response_time_ms", "p95_response_time_ms", "max_response_time_ms",
        "pct_of_hourly_traffic", "pct_of_hourly_failed_logins",
        "rolling_15m_avg_events", "rolling_15m_stddev_events",
        "rolling_15m_avg_latency_ms", "traffic_spike_ratio",
        "traffic_z_score", "latency_spike_ratio",
    ]
    for col in numeric_candidates:
        if col in m_df.columns:
            m_df[col] = pd.to_numeric(m_df[col], errors="coerce")

    if "minute_ts" in m_df.columns:
        m_df["ts"] = pd.to_datetime(m_df["minute_ts"], errors="coerce")
    elif {"event_date", "event_hour", "event_minute"}.issubset(m_df.columns):
        m_df["ts"] = (
            pd.to_datetime(m_df["event_date"], errors="coerce")
            + pd.to_timedelta(m_df["event_hour"].fillna(0), unit="h")
            + pd.to_timedelta(m_df["event_minute"].fillna(0), unit="m")
        )
    else:
        m_df["ts"] = pd.RangeIndex(start=0, stop=len(m_df), step=1)

    with st.expander("Full Minutes Level Summary (describe().T)", expanded=False):
        st.dataframe(m_df.select_dtypes(include=[np.number]).describe().T.round(3), use_container_width=True)
        render_plain_note(
            "Summary statistics for every numeric minute-level metric.",
            "Shows normal operating ranges and how far spike minutes move above baseline."
        )

    sort_col = "ts" if "ts" in m_df.columns and m_df["ts"].notna().any() else "event_minute"
    df = m_df.sort_values(sort_col).copy()

    failed_rate = df["failed_login_rate"].fillna(0) if "failed_login_rate" in df.columns else pd.Series(0.0, index=df.index)
    avg_req_ip = df["avg_requests_per_ip"].fillna(0) if "avg_requests_per_ip" in df.columns else pd.Series(0.0, index=df.index)
    uniq_ips = df["unique_source_ips"].fillna(0) if "unique_source_ips" in df.columns else pd.Series(0.0, index=df.index)
    admin_hits = df["admin_endpoint_hits"].fillna(0) if "admin_endpoint_hits" in df.columns else pd.Series(0.0, index=df.index)
    total_ev = df["total_events"].fillna(0) if "total_events" in df.columns else pd.Series(0.0, index=df.index)
    latency_ratio = df["latency_spike_ratio"].fillna(1.0) if "latency_spike_ratio" in df.columns else pd.Series(1.0, index=df.index)
    traffic_ratio = df["traffic_spike_ratio"].fillna(1.0) if "traffic_spike_ratio" in df.columns else pd.Series(1.0, index=df.index)

    req_q75 = avg_req_ip.quantile(0.75) if len(avg_req_ip) else 0
    ip_q75 = uniq_ips.quantile(0.75) if len(uniq_ips) else 0
    admin_q90 = admin_hits.quantile(0.90) if len(admin_hits) else 0
    events_med = total_ev.median() if len(total_ev) else 0

    df["attack_pattern"] = np.select(
        [
            (failed_rate > 0.20) & (avg_req_ip >= req_q75),
            (failed_rate > 0.20) & (uniq_ips >= ip_q75),
            (admin_hits >= admin_q90) & (total_ev < events_med),
            (latency_ratio > 1.15),
        ],
        [
            "Targeted Brute Force",
            "Distributed Credential Stuffing",
            "Stealth Admin Recon",
            "Latency / Resource Execution",
        ],
        default="Baseline / Normal",
    )

    admin_share = (admin_hits / total_ev.replace(0, np.nan)).fillna(0)

    df["threat_score"] = (
        (failed_rate.rank(pct=True) * 0.40)
        + (admin_share.rank(pct=True) * 0.20)
        + (traffic_ratio.rank(pct=True) * 0.25)
        + (latency_ratio.rank(pct=True) * 0.15)
    ) * 100

    df["is_incident"] = df["threat_score"] >= 85
    df["episode_id"] = (df["is_incident"] != df["is_incident"].shift()).cumsum()

    episodes = (
        df[df["is_incident"]]
        .groupby("episode_id")
        .agg(
            start_time=("ts", "min"),
            end_time=("ts", "max"),
            duration_mins=("ts", "count"),
            peak_threat_score=("threat_score", "max"),
            total_failed_logins=("failed_login_count", "sum") if "failed_login_count" in df.columns else ("threat_score", "count"),
            total_admin_hits=("admin_endpoint_hits", "sum") if "admin_endpoint_hits" in df.columns else ("threat_score", "count"),
            peak_latency_ratio=("latency_spike_ratio", "max") if "latency_spike_ratio" in df.columns else ("threat_score", "max"),
        )
        .sort_values(["duration_mins", "total_failed_logins"], ascending=False)
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Monitored Minutes", f"{len(df):,}")
    k2.metric("High Threat (≥85)", f"{int(df['is_incident'].sum()):,}")
    k3.metric("Sustained Episodes", f"{len(episodes):,}")
    k4.metric("Peak Threat Score", f"{df['threat_score'].max():.1f}")
    k5.metric("Max Traffic Spike", f"{traffic_ratio.max():.2f}x")

    render_auto_ai_summary_banner(selected_page)

    st.markdown("<br>", unsafe_allow_html=True)

    c1, c2 = st.columns([1.05, 1.45])

    pattern_counts = (
        df["attack_pattern"]
        .value_counts()
        .rename_axis("attack_pattern")
        .reset_index(name="minute_count")
    )

    with c1:
        st.markdown("#### Attack Pattern Breakdown")
        fig_patterns = px.bar(
            pattern_counts.sort_values("minute_count", ascending=True),
            x="minute_count",
            y="attack_pattern",
            orientation="h",
            text="minute_count",
            template="plotly_dark",
            color="attack_pattern",
            color_discrete_map={
                "Baseline / Normal": "#334155",
                "Targeted Brute Force": "#38bdf8",
                "Distributed Credential Stuffing": "#0284c7",
                "Stealth Admin Recon": "#7dd3fc",
                "Latency / Resource Execution": "#94a3b8",
            },
        )
        fig_patterns.update_traces(textposition="outside", marker_line_width=0)
        fig_patterns.update_layout(
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#e2e8f0"),
            showlegend=False,
            xaxis_title="Minutes",
            yaxis_title="",
            margin=dict(l=10, r=35, t=20, b=30),
            height=340,
        )
        fig_patterns.update_xaxes(showgrid=True, gridcolor="#1e293b")
        fig_patterns.update_yaxes(showgrid=False)
        st.plotly_chart(fig_patterns, use_container_width=True)
        render_plain_note(
            "How many minutes fell into normal baseline traffic versus specific rule-based risk patterns.",
            "Higher counts in non-baseline categories show recurring windows of login failures, admin probes, or latency spikes."
        )

    with c2:
        st.markdown("#### Threat Score & Incident Threshold (85)")
        fig_threat = go.Figure()
        fig_threat.add_trace(
            go.Scatter(
                x=df["ts"],
                y=df["threat_score"],
                mode="lines",
                name="Threat Score",
                line=dict(color="#38bdf8", width=1.8),
                fill="tozeroy",
                fillcolor="rgba(56, 189, 248, 0.12)",
            )
        )
        fig_threat.add_hline(
            y=85,
            line_dash="dash",
            line_color="#7dd3fc",
            annotation_text="Incident Threshold (85)",
            annotation_position="top left",
            annotation_font_color="#7dd3fc",
        )
        fig_threat.update_layout(
            template="plotly_dark",
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#e2e8f0"),
            xaxis_title="Timeline",
            yaxis_title="Threat Score (0–100)",
            yaxis=dict(range=[0, 105], showgrid=True, gridcolor="#1e293b"),
            xaxis=dict(showgrid=False),
            margin=dict(l=10, r=15, t=25, b=30),
            height=340,
        )
        st.plotly_chart(fig_threat, use_container_width=True)
        render_plain_note(
            f"Each minute's combined risk score (0–100) compared against the <b>85</b> alert line, crossed by <b>{int(df['is_incident'].sum()):,} minutes</b>.",
            "Whenever the score rises above 85, that minute had elevated login failures, admin hits, traffic spikes, or latency."
        )

    c3, c4 = st.columns(2)

    with c3:
        st.markdown("#### Traffic vs. Latency Spike Ratios")
        fig_spikes = go.Figure()
        fig_spikes.add_trace(
            go.Scatter(
                x=df["ts"],
                y=traffic_ratio,
                mode="lines",
                name="Traffic Spike Ratio",
                line=dict(color="#38bdf8", width=1.6),
            )
        )
        fig_spikes.add_trace(
            go.Scatter(
                x=df["ts"],
                y=latency_ratio,
                mode="lines",
                name="Latency Spike Ratio",
                line=dict(color="#94a3b8", width=1.6),
            )
        )
        fig_spikes.update_layout(
            template="plotly_dark",
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#e2e8f0"),
            legend=dict(orientation="h", y=1.12, x=0),
            xaxis_title="Timeline",
            yaxis_title="Spike Ratio",
            margin=dict(l=10, r=15, t=35, b=30),
            height=320,
        )
        fig_spikes.update_xaxes(showgrid=False)
        fig_spikes.update_yaxes(showgrid=True, gridcolor="#1e293b")
        st.plotly_chart(fig_spikes, use_container_width=True)
        render_plain_note(
            f"How much traffic (peak <b>{traffic_ratio.max():.2f}x</b>) and server delay (peak <b>{latency_ratio.max():.2f}x</b>) multiplied above normal levels.",
            "Values near 1.0x are normal; sharp peaks show sudden traffic bursts or slow server responses."
        )

    with c4:
        st.markdown("#### Events vs. Rolling 15m Average")
        fig_roll = go.Figure()
        fig_roll.add_trace(
            go.Scatter(
                x=df["ts"],
                y=total_ev,
                mode="lines",
                name="Total Events / Min",
                line=dict(color="#38bdf8", width=1.5),
            )
        )
        if "rolling_15m_avg_events" in df.columns:
            fig_roll.add_trace(
                go.Scatter(
                    x=df["ts"],
                    y=df["rolling_15m_avg_events"],
                    mode="lines",
                    name="Rolling 15m Avg Events",
                    line=dict(color="#bae6fd", width=1.8, dash="dot"),
                )
            )
        fig_roll.update_layout(
            template="plotly_dark",
            paper_bgcolor="#000000",
            plot_bgcolor="#000000",
            font=dict(color="#e2e8f0"),
            legend=dict(orientation="h", y=1.12, x=0),
            xaxis_title="Timeline",
            yaxis_title="Events",
            margin=dict(l=10, r=15, t=35, b=30),
            height=320,
        )
        fig_roll.update_xaxes(showgrid=False)
        fig_roll.update_yaxes(showgrid=True, gridcolor="#1e293b")
        st.plotly_chart(fig_roll, use_container_width=True)
        render_plain_note(
            "Actual events per minute plotted alongside the 15-minute rolling average baseline.",
            "When the solid line jumps well above the dotted baseline, traffic surged faster than the recent 15-minute average."
        )

    t1, t2 = st.columns(2)

    with t1:
        st.markdown("#### Top 10 Highest Threat Minutes")
        top_minutes = df.sort_values("threat_score", ascending=False).head(10).copy()
        if "ts" in top_minutes.columns:
            top_minutes["ts"] = _format_ts_display(top_minutes["ts"])
        if "event_minute" in top_minutes.columns:
            top_minutes["event_minute"] = _format_minute_display(top_minutes["event_minute"])

        cols = [
            c for c in [
                "ts", "event_minute", "attack_pattern",
                "failed_login_count", "admin_endpoint_hits",
                "latency_spike_ratio", "threat_score",
            ]
            if c in top_minutes.columns
        ]
        st.dataframe(
            top_minutes[cols].round(3),
            use_container_width=True,
            hide_index=True,
        )
        render_plain_note(
            "The 10 individual minutes with the highest rule-based threat scores.",
            "These timestamps are a practical starting point when investigating peak login failures, admin hits, or latency spikes."
        )

    with t2:
        st.markdown("#### Top Sustained Incident Episodes")
        top_episodes = episodes.head(10).copy()
        for t_col in ["start_time", "end_time"]:
            if t_col in top_episodes.columns:
                top_episodes[t_col] = _format_ts_display(top_episodes[t_col])
        st.dataframe(
            top_episodes.round(2),
            use_container_width=True,
        )
        render_plain_note(
            f"Grouped windows (<b>{len(episodes):,}</b> total) where high threat scores (`>= 85`) persisted across consecutive minutes.",
            "Sustained episodes matter more than isolated one-off spikes because they show ongoing activity over time."
        )


# =========================================================
# PAGE 4: ML ANOMALY DETECTION (MINUTE MART)
# =========================================================
elif selected_page == "Page 4: ML Anomaly Detection":
    st.markdown(
        """
        <div class="page-banner">
            <div class="page-kicker">Page 4 of 4 • Unsupervised ML on Minute Mart</div>
            <h1 class="page-heading">ML Anomaly Detection</h1>
            <div class="page-desc">Multi-feature unsupervised anomaly scoring compared against rule-based security thresholds.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    ml_df = st.session_state.get("minute_df", minute_df).copy()
    ml_df.columns = [str(c).lower().strip() for c in ml_df.columns]

    feature_candidates = [
        "total_events",
        "unique_source_ips",
        "avg_requests_per_ip",
        "error_4xx_rate",
        "error_5xx_rate",
        "sensitive_endpoint_rate",
        "failed_login_rate",
        "p95_response_time_ms",
        "traffic_spike_ratio",
        "traffic_z_score",
        "latency_spike_ratio",
    ]
    used_features = [c for c in feature_candidates if c in ml_df.columns]

    for c in used_features:
        ml_df[c] = pd.to_numeric(ml_df[c], errors="coerce")

    if "minute_ts" in ml_df.columns:
        ml_df["ts"] = pd.to_datetime(ml_df["minute_ts"], errors="coerce")
    elif {"event_date", "event_hour", "event_minute"}.issubset(ml_df.columns):
        ml_df["ts"] = (
            pd.to_datetime(ml_df["event_date"], errors="coerce")
            + pd.to_timedelta(pd.to_numeric(ml_df["event_hour"], errors="coerce").fillna(0), unit="h")
            + pd.to_timedelta(pd.to_numeric(ml_df["event_minute"], errors="coerce").fillna(0), unit="m")
        )
    else:
        ml_df["ts"] = pd.RangeIndex(start=0, stop=len(ml_df), step=1)

    ml_df = ml_df.sort_values("ts").reset_index(drop=True)

    contamination_rate = st.slider(
        "ML Sensitivity (Expected Anomaly %)",
        min_value=1,
        max_value=15,
        value=5,
        step=1,
        help="Lower values catch only the most extreme outliers; higher values also flag more subtle unusual minutes.",
    ) / 100.0

    X = ml_df[used_features].replace([np.inf, -np.inf], np.nan)
    X = X.fillna(X.median()).fillna(0)

    if SKLEARN_AVAILABLE and len(X) >= 20 and len(used_features) >= 2:
        scaler = RobustScaler()
        X_scaled = scaler.fit_transform(X)
        iso = IsolationForest(
            n_estimators=150,
            contamination=contamination_rate,
            random_state=42,
        )
        preds = iso.fit_predict(X_scaled)
        raw_scores = -iso.decision_function(X_scaled)
        ml_df["ml_anomaly_score"] = (pd.Series(raw_scores).rank(pct=True) * 100).round(2)
        ml_df["ml_flag"] = np.where(preds == -1, "ML Anomaly", "Normal")
        model_label = "Isolation Forest (RobustScaler)"
    else:
        z_mat = (X - X.mean()) / X.std().replace(0, 1)
        composite_z = z_mat.abs().mean(axis=1)
        ml_df["ml_anomaly_score"] = (composite_z.rank(pct=True) * 100).round(2)
        cutoff = ml_df["ml_anomaly_score"].quantile(1 - contamination_rate)
        ml_df["ml_flag"] = np.where(ml_df["ml_anomaly_score"] >= cutoff, "ML Anomaly", "Normal")
        model_label = "Multi-Feature Z-Score Fallback"

    f_rate = pd.to_numeric(ml_df.get("failed_login_rate", 0), errors="coerce").fillna(0)
    a_hits = pd.to_numeric(ml_df.get("admin_endpoint_hits", 0), errors="coerce").fillna(0)
    t_ev = pd.to_numeric(ml_df.get("total_events", 0), errors="coerce").fillna(0)
    t_rat = pd.to_numeric(ml_df.get("traffic_spike_ratio", 1), errors="coerce").fillna(1)
    l_rat = pd.to_numeric(ml_df.get("latency_spike_ratio", 1), errors="coerce").fillna(1)
    a_share = (a_hits / t_ev.replace(0, np.nan)).fillna(0)

    ml_df["rule_threat_score"] = (
        (f_rate.rank(pct=True) * 0.40)
        + (a_share.rank(pct=True) * 0.20)
        + (t_rat.rank(pct=True) * 0.25)
        + (l_rat.rank(pct=True) * 0.15)
    ) * 100

    ml_df["consensus_status"] = np.select(
        [
            (ml_df["ml_flag"] == "ML Anomaly") & (ml_df["rule_threat_score"] >= 85),
            (ml_df["ml_flag"] == "ML Anomaly") & (ml_df["rule_threat_score"] < 85),
            (ml_df["ml_flag"] != "ML Anomaly") & (ml_df["rule_threat_score"] >= 85),
        ],
        [
            "Both ML & Rules Flagged",
            "ML-Only Subtle Anomaly",
            "Rule-Only High Threat",
        ],
        default="Normal",
    )

    anom_count = int((ml_df["ml_flag"] == "ML Anomaly").sum())
    both_count = int((ml_df["consensus_status"] == "Both ML & Rules Flagged").sum())
    ml_only_count = int((ml_df["consensus_status"] == "ML-Only Subtle Anomaly").sum())

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("ML Engine", "Isolation Forest" if "Isolation Forest" in model_label else "Z-Score Model")
    m2.metric("ML Flagged Minutes", f"{anom_count:,}")
    m3.metric("ML + Rules Match", f"{both_count:,}")
    m4.metric("ML-Only Anomalies", f"{ml_only_count:,}")

    render_auto_ai_summary_banner(selected_page)

    st.markdown("<br>", unsafe_allow_html=True)

    mc1, mc2 = st.columns([1.4, 1])

    with mc1:
        fig_ml_ts = go.Figure()
        fig_ml_ts.add_trace(
            go.Scatter(
                x=ml_df["ts"],
                y=ml_df["ml_anomaly_score"],
                mode="lines",
                name="ML Anomaly Score",
                line=dict(color="#38bdf8", width=1.6),
            )
        )
        anom_pts = ml_df[ml_df["ml_flag"] == "ML Anomaly"]
        fig_ml_ts.add_trace(
            go.Scatter(
                x=anom_pts["ts"],
                y=anom_pts["ml_anomaly_score"],
                mode="markers",
                name="Flagged Anomaly",
                marker=dict(color="#bae6fd", size=7, symbol="circle"),
            )
        )
        fig_ml_ts.update_layout(
            title="ML Anomaly Score Timeline & Detected Outlier Minutes",
            xaxis_title="Timeline",
            yaxis_title="ML Anomaly Percentile (0–100)",
        )
        st.plotly_chart(apply_dark_style(fig_ml_ts, height=360), use_container_width=True)
        render_plain_note(
            f"The ML model (<b>{model_label}</b>) scored every minute from 0 to 100 and flagged <b>{anom_count:,} minutes</b> as unusual outliers.",
            "Highlighted markers show minutes where multiple metrics together looked different from normal traffic—even if a single rule was not triggered."
        )

    with mc2:
        fig_ml_cmp = px.scatter(
            ml_df,
            x="rule_threat_score",
            y="ml_anomaly_score",
            color="consensus_status",
            title="Rule Threat Score vs. ML Anomaly Score",
            labels={
                "rule_threat_score": "Rule Threat Score (0–100)",
                "ml_anomaly_score": "ML Anomaly Score (0–100)",
                "consensus_status": "Detection Status",
            },
            color_discrete_map={
                "Normal": "#334155",
                "Both ML & Rules Flagged": "#38bdf8",
                "ML-Only Subtle Anomaly": "#7dd3fc",
                "Rule-Only High Threat": "#94a3b8",
            },
        )
        st.plotly_chart(apply_dark_style(fig_ml_cmp, height=360), use_container_width=True)
        render_plain_note(
            f"Compares rule alerts against ML detections: <b>{both_count:,}</b> minutes were flagged by both, while <b>{ml_only_count:,}</b> were subtle ML-only anomalies.",
            "Minutes flagged by both ML and rules are high-priority checks, while ML-only points help spot unusual multi-metric shifts early."
        )

    st.subheader("Top 15 ML-Flagged Anomalous Minutes")
    display_cols = [
        c for c in [
            "ts", "consensus_status", "ml_anomaly_score", "rule_threat_score",
            "total_events", "failed_login_rate", "error_5xx_rate",
            "traffic_spike_ratio", "latency_spike_ratio",
        ]
        if c in ml_df.columns
    ]
    top_ml = ml_df.sort_values("ml_anomaly_score", ascending=False)[display_cols].head(15).copy()
    if "ts" in top_ml.columns:
        top_ml["ts"] = pd.to_datetime(top_ml["ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

    st.dataframe(top_ml.round(3), use_container_width=True, hide_index=True)
    render_plain_note(
        "The 15 minutes with the highest ML anomaly scores and their underlying traffic, error, and spike values.",
        "Anomalies highlight unusual patterns statistically—always check the underlying columns to confirm whether a spike is a security issue or normal load."
    )