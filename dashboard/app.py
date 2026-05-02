import time
import pandas as pd
import psycopg2
import plotly.express as px
import streamlit as st


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password",
}


st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="🚨",
    layout="wide"
)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_data(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df


def main():
    st.title("🚨 Real-Time Fraud Detection Dashboard")
    st.caption("Monitoring des transactions bancaires, scores de risque et alertes de fraude en temps réel.")

    refresh_seconds = st.sidebar.slider(
        "Rafraîchissement automatique en secondes",
        min_value=2,
        max_value=30,
        value=5
    )

    auto_refresh = st.sidebar.checkbox("Activer auto-refresh", value=True)

    transactions_count = load_data("""
        SELECT COUNT(*) AS total_transactions
        FROM transactions;
    """)

    fraud_count = load_data("""
        SELECT COUNT(*) AS total_fraud
        FROM transactions
        WHERE transaction_status = 'FRAUD';
    """)

    suspicious_count = load_data("""
        SELECT COUNT(*) AS total_suspicious
        FROM transactions
        WHERE transaction_status = 'SUSPICIOUS';
    """)

    normal_count = load_data("""
        SELECT COUNT(*) AS total_normal
        FROM transactions
        WHERE transaction_status = 'NORMAL';
    """)

    fraud_amount = load_data("""
        SELECT COALESCE(SUM(amount), 0) AS total_fraud_amount
        FROM transactions
        WHERE transaction_status = 'FRAUD';
    """)

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total transactions", int(transactions_count["total_transactions"].iloc[0]))
    col2.metric("Transactions normales", int(normal_count["total_normal"].iloc[0]))
    col3.metric("Transactions suspectes", int(suspicious_count["total_suspicious"].iloc[0]))
    col4.metric("Fraudes détectées", int(fraud_count["total_fraud"].iloc[0]))
    col5.metric("Montant frauduleux", f"{float(fraud_amount['total_fraud_amount'].iloc[0]):,.0f} $")

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Répartition des statuts")

        status_df = load_data("""
            SELECT transaction_status, COUNT(*) AS count
            FROM transactions
            GROUP BY transaction_status
            ORDER BY count DESC;
        """)

        if not status_df.empty:
            fig_status = px.pie(
                status_df,
                names="transaction_status",
                values="count",
                title="Transactions par statut"
            )
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.info("Aucune transaction disponible.")

    with right_col:
        st.subheader("Top pays suspects")

        countries_df = load_data("""
            SELECT country, COUNT(*) AS total_alerts
            FROM transactions
            WHERE transaction_status IN ('SUSPICIOUS', 'FRAUD')
            GROUP BY country
            ORDER BY total_alerts DESC
            LIMIT 10;
        """)

        if not countries_df.empty:
            fig_countries = px.bar(
                countries_df,
                x="country",
                y="total_alerts",
                title="Pays avec le plus d'alertes"
            )
            st.plotly_chart(fig_countries, use_container_width=True)
        else:
            st.info("Aucun pays suspect pour le moment.")

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Distribution des scores de risque")

        scores_df = load_data("""
            SELECT risk_score
            FROM transactions
            WHERE risk_score IS NOT NULL;
        """)

        if not scores_df.empty:
            fig_scores = px.histogram(
                scores_df,
                x="risk_score",
                nbins=20,
                title="Distribution des risk_score"
            )
            st.plotly_chart(fig_scores, use_container_width=True)
        else:
            st.info("Aucun score disponible.")

    with right_col:
        st.subheader("Fraudes par minute")

        timeline_df = load_data("""
            SELECT 
                DATE_TRUNC('minute', processed_at) AS minute,
                COUNT(*) AS fraud_count
            FROM transactions
            WHERE transaction_status = 'FRAUD'
            GROUP BY minute
            ORDER BY minute ASC;
        """)

        if not timeline_df.empty:
            fig_timeline = px.line(
                timeline_df,
                x="minute",
                y="fraud_count",
                title="Évolution des fraudes dans le temps"
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("Aucune fraude détectée pour le moment.")

    st.divider()

    st.subheader("Dernières transactions")

    latest_transactions = load_data("""
        SELECT 
            transaction_id,
            user_id,
            amount,
            currency,
            country,
            usual_country,
            merchant_category,
            payment_method,
            risk_score,
            transaction_status,
            transaction_timestamp,
            processed_at
        FROM transactions
        ORDER BY processed_at DESC
        LIMIT 20;
    """)

    st.dataframe(latest_transactions, use_container_width=True)

    st.divider()

    st.subheader("Dernières alertes de fraude")

    latest_alerts = load_data("""
        SELECT 
            alert_id,
            transaction_id,
            user_id,
            risk_score,
            alert_type,
            message,
            status,
            created_at
        FROM fraud_alerts
        ORDER BY created_at DESC
        LIMIT 20;
    """)

    st.dataframe(latest_alerts, use_container_width=True)

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()