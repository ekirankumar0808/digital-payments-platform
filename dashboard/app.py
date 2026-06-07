import streamlit as st
import pandas as pd
import plotly.express as px

from streamlit_autorefresh import st_autorefresh

from utils.athena_client import run_query


# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="Digital Payments Fraud Dashboard",
    page_icon="💳",
    layout="wide"
)

# ---------------------------------------------------
# AUTO REFRESH DASHBOARD
# ---------------------------------------------------

st_autorefresh(
    interval=60000,   # Refresh every 60 seconds
    key="dashboard_refresh"
)

st.title("💳 Digital Payments Fraud Analytics Dashboard")

st.markdown(
    "Real-Time Fraud Monitoring & Analytics Platform"
)

# ---------------------------------------------------
# QUERY
# ---------------------------------------------------

query = """
SELECT
    ingestion_date,
    total_transactions,
    unique_senders,
    unique_receivers,
    total_frauds,
    total_flagged_frauds,
    fraud_rate,
    total_transaction_value,
    fraud_amount,
    avg_fraud_amount,
    high_value_txns,
    full_depletion_events,
    cash_out_count,
    transfer_count,
    avg_risk_score,
    max_risk_score,
    fraud_ratio_percent,
    CAST(gold_processed_timestamp AS timestamp) AS gold_processed_timestamp
FROM gold_fraud_daily_summary
ORDER BY ingestion_date DESC
"""

# ---------------------------------------------------
# LOAD DATA WITH CACHING
# ---------------------------------------------------

@st.cache_data(ttl=60)
def load_data():
    return run_query(query)

df = load_data()

# ---------------------------------------------------
# DATE CONVERSION
# ---------------------------------------------------

df["ingestion_date"] = pd.to_datetime(
    df["ingestion_date"]
)

# ---------------------------------------------------
# SIDEBAR FILTERS
# ---------------------------------------------------

st.sidebar.header("📌 Dashboard Filters")

selected_dates = st.sidebar.multiselect(
    "Select Dates",
    options=df["ingestion_date"]
    .dt.date
    .astype(str)
    .unique(),
    default=df["ingestion_date"]
    .dt.date
    .astype(str)
    .unique()
)

filtered_df = df[
    df["ingestion_date"]
    .dt.date
    .astype(str)
    .isin(selected_dates)
]

# ---------------------------------------------------
# METRICS
# ---------------------------------------------------

total_transactions = (
    filtered_df["total_transactions"].sum()
)

total_frauds = (
    filtered_df["total_frauds"].sum()
)

fraud_amount = (
    filtered_df["fraud_amount"].sum()
)

fraud_rate = (
    filtered_df["fraud_ratio_percent"].mean()
)

total_transaction_value = (
    filtered_df["total_transaction_value"].sum()
)

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Transactions",
    f"{total_transactions:,.0f}"
)

col2.metric(
    "Frauds",
    f"{total_frauds:,.0f}"
)

col3.metric(
    "Fraud Amount",
    f"${fraud_amount:,.2f}"
)

col4.metric(
    "Fraud %",
    f"{fraud_rate:.2f}%"
)

col5.metric(
    "Transaction Value",
    f"${total_transaction_value:,.0f}"
)

st.divider()

# ---------------------------------------------------
# ROW 1 CHARTS
# ---------------------------------------------------

col_left, col_right = st.columns(2)

with col_left:

    st.subheader(
        "📈 Daily Transaction Volume"
    )

    fig1 = px.line(
        filtered_df,
        x="ingestion_date",
        y="total_transactions",
        markers=True
    )

    st.plotly_chart(
        fig1,
        use_container_width=True
    )

with col_right:

    st.subheader(
        "🚨 Fraud Trend"
    )

    fig2 = px.bar(
        filtered_df,
        x="ingestion_date",
        y="total_frauds"
    )

    st.plotly_chart(
        fig2,
        use_container_width=True
    )

# ---------------------------------------------------
# ROW 2 CHARTS
# ---------------------------------------------------

col_left2, col_right2 = st.columns(2)

with col_left2:

    st.subheader(
        "⚠️ Average Risk Score"
    )

    fig3 = px.line(
        filtered_df,
        x="ingestion_date",
        y="avg_risk_score",
        markers=True
    )

    st.plotly_chart(
        fig3,
        use_container_width=True
    )

with col_right2:

    st.subheader(
        "💰 Fraud Amount Trend"
    )

    fig4 = px.bar(
        filtered_df,
        x="ingestion_date",
        y="fraud_amount"
    )

    st.plotly_chart(
        fig4,
        use_container_width=True
    )

# ---------------------------------------------------
# ROW 3 CHARTS
# ---------------------------------------------------

col_left3, col_right3 = st.columns(2)

with col_left3:

    st.subheader(
        "🏦 High Value Transactions"
    )

    fig5 = px.line(
        filtered_df,
        x="ingestion_date",
        y="high_value_txns",
        markers=True
    )

    st.plotly_chart(
        fig5,
        use_container_width=True
    )

with col_right3:

    st.subheader(
        "🧾 Fraud vs Non-Fraud"
    )

    fraud_vs_normal = pd.DataFrame({
        "Category": [
            "Fraud",
            "Non Fraud"
        ],
        "Count": [
            filtered_df["total_frauds"].sum(),
            (
                filtered_df["total_transactions"].sum()
                -
                filtered_df["total_frauds"].sum()
            )
        ]
    })

    fig6 = px.pie(
        fraud_vs_normal,
        names="Category",
        values="Count",
        hole=0.4
    )

    st.plotly_chart(
        fig6,
        use_container_width=True
    )

# ---------------------------------------------------
# DOWNLOAD BUTTON
# ---------------------------------------------------

st.subheader(
    "⬇️ Download Data"
)

csv = filtered_df.to_csv(index=False)

st.download_button(
    label="Download Gold Data as CSV",
    data=csv,
    file_name="gold_fraud_summary.csv",
    mime="text/csv"
)

# ---------------------------------------------------
# DATA TABLE
# ---------------------------------------------------

st.subheader(
    "📋 Gold Layer Data"
)

st.dataframe(
    filtered_df,
    use_container_width=True
)