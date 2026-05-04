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


def safe_divide(numerator, denominator):
    if denominator == 0:
        return 0.0
    return numerator / denominator


def main():
    st.title("Real-Time Fraud Detection Dashboard")
    st.caption(
        "Monitoring temps réel des transactions bancaires avec moteur hybride : "
        "Rule-Based Scoring + ML Probability + Behavior Analysis."
    )

    refresh_seconds = st.sidebar.slider(
        "Rafraîchissement automatique en secondes",
        min_value=2,
        max_value=30,
        value=5
    )

    auto_refresh = st.sidebar.checkbox("Activer auto-refresh", value=True)

    st.sidebar.divider()
    st.sidebar.markdown("### Filtres")

    selected_status = st.sidebar.multiselect(
        "Statut transaction",
        options=["NORMAL", "SUSPICIOUS", "FRAUD"],
        default=["NORMAL", "SUSPICIOUS", "FRAUD"]
    )

    if not selected_status:
        selected_status = ["NORMAL", "SUSPICIOUS", "FRAUD"]

    status_filter = ",".join([f"'{status}'" for status in selected_status])

    metrics_df = load_data(f"""
        SELECT
            COUNT(*) AS total_transactions,
            COUNT(*) FILTER (WHERE transaction_status = 'NORMAL') AS total_normal,
            COUNT(*) FILTER (WHERE transaction_status = 'SUSPICIOUS') AS total_suspicious,
            COUNT(*) FILTER (WHERE transaction_status = 'FRAUD') AS total_fraud,
            COALESCE(SUM(amount) FILTER (WHERE transaction_status = 'FRAUD'), 0) AS total_fraud_amount,
            COALESCE(AVG(ml_probability), 0) AS avg_ml_probability,
            COALESCE(AVG(behavior_score), 0) AS avg_behavior_score,
            COALESCE(AVG(final_score), 0) AS avg_final_score
        FROM transactions
        WHERE transaction_status IN ({status_filter});
    """)

    total_transactions = int(metrics_df["total_transactions"].iloc[0])
    total_normal = int(metrics_df["total_normal"].iloc[0])
    total_suspicious = int(metrics_df["total_suspicious"].iloc[0])
    total_fraud = int(metrics_df["total_fraud"].iloc[0])
    total_fraud_amount = float(metrics_df["total_fraud_amount"].iloc[0])
    avg_ml_probability = float(metrics_df["avg_ml_probability"].iloc[0])
    avg_behavior_score = float(metrics_df["avg_behavior_score"].iloc[0])
    avg_final_score = float(metrics_df["avg_final_score"].iloc[0])

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total transactions", total_transactions)
    col2.metric("Normales", total_normal)
    col3.metric("Suspectes", total_suspicious)
    col4.metric("Fraudes", total_fraud)
    col5.metric("Montant frauduleux", f"{total_fraud_amount:,.2f} $")

    col6, col7, col8 = st.columns(3)

    col6.metric("Risque ML moyen", f"{avg_ml_probability:.4f}")
    col7.metric("Score comportemental moyen", f"{avg_behavior_score:.2f}")
    col8.metric("Score hybride moyen", f"{avg_final_score:.2f}")

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Répartition des statuts")

        status_df = load_data(f"""
            SELECT transaction_status, COUNT(*) AS count
            FROM transactions
            WHERE transaction_status IN ({status_filter})
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

    st.subheader("Analyse du moteur hybride")

    col_left, col_right = st.columns(2)

    with col_left:
        final_score_df = load_data(f"""
            SELECT final_score
            FROM transactions
            WHERE final_score IS NOT NULL
            AND transaction_status IN ({status_filter});
        """)

        if not final_score_df.empty:
            fig_final_score = px.histogram(
                final_score_df,
                x="final_score",
                nbins=20,
                title="Distribution du final_score"
            )
            st.plotly_chart(fig_final_score, use_container_width=True)
        else:
            st.info("Aucun final_score disponible.")

    with col_right:
        ml_probability_df = load_data(f"""
            SELECT ml_probability
            FROM transactions
            WHERE ml_probability IS NOT NULL
            AND transaction_status IN ({status_filter});
        """)

        if not ml_probability_df.empty:
            fig_ml = px.histogram(
                ml_probability_df,
                x="ml_probability",
                nbins=20,
                title="Distribution de ml_probability"
            )
            st.plotly_chart(fig_ml, use_container_width=True)
        else:
            st.info("Aucune probabilité ML disponible.")

    col_left, col_right = st.columns(2)

    with col_left:
        behavior_df = load_data(f"""
            SELECT behavior_score, COUNT(*) AS count
            FROM transactions
            WHERE behavior_score IS NOT NULL
            AND transaction_status IN ({status_filter})
            GROUP BY behavior_score
            ORDER BY behavior_score ASC;
        """)

        if not behavior_df.empty:
            fig_behavior = px.bar(
                behavior_df,
                x="behavior_score",
                y="count",
                title="Distribution du behavior_score"
            )
            st.plotly_chart(fig_behavior, use_container_width=True)
        else:
            st.info("Aucun behavior_score disponible.")

    with col_right:
        detection_method_df = load_data(f"""
            SELECT detection_method, COUNT(*) AS count
            FROM transactions
            WHERE detection_method IS NOT NULL
            AND transaction_status IN ({status_filter})
            GROUP BY detection_method
            ORDER BY count DESC;
        """)

        if not detection_method_df.empty:
            fig_method = px.bar(
                detection_method_df,
                x="detection_method",
                y="count",
                title="Méthode de détection utilisée"
            )
            st.plotly_chart(fig_method, use_container_width=True)
        else:
            st.info("Aucune méthode de détection disponible.")

    st.divider()

    st.subheader("Model Evaluation")

    st.caption(
        "Cette section utilise actual_label uniquement pour évaluer le modèle avec le dataset. "
        "Dans un vrai système bancaire, ce label réel n'est pas connu au moment de la transaction."
    )

    evaluation_df = load_data("""
        SELECT
            SUM(CASE WHEN actual_label = 1 AND transaction_status IN ('SUSPICIOUS', 'FRAUD') THEN 1 ELSE 0 END) AS true_positives,
            SUM(CASE WHEN actual_label = 0 AND transaction_status IN ('SUSPICIOUS', 'FRAUD') THEN 1 ELSE 0 END) AS false_positives,
            SUM(CASE WHEN actual_label = 0 AND transaction_status = 'NORMAL' THEN 1 ELSE 0 END) AS true_negatives,
            SUM(CASE WHEN actual_label = 1 AND transaction_status = 'NORMAL' THEN 1 ELSE 0 END) AS false_negatives
        FROM transactions
        WHERE actual_label IS NOT NULL;
    """)

    if not evaluation_df.empty:
        tp = int(evaluation_df["true_positives"].fillna(0).iloc[0])
        fp = int(evaluation_df["false_positives"].fillna(0).iloc[0])
        tn = int(evaluation_df["true_negatives"].fillna(0).iloc[0])
        fn = int(evaluation_df["false_negatives"].fillna(0).iloc[0])

        precision = safe_divide(tp, tp + fp)
        recall = safe_divide(tp, tp + fn)
        f1_score = safe_divide(2 * precision * recall, precision + recall)
        accuracy = safe_divide(tp + tn, tp + fp + tn + fn)

        eval_col1, eval_col2, eval_col3, eval_col4 = st.columns(4)

        eval_col1.metric("Precision", f"{precision * 100:.2f}%")
        eval_col2.metric("Recall", f"{recall * 100:.2f}%")
        eval_col3.metric("F1-score", f"{f1_score * 100:.2f}%")
        eval_col4.metric("Accuracy", f"{accuracy * 100:.2f}%")

        cm_col1, cm_col2, cm_col3, cm_col4 = st.columns(4)

        cm_col1.metric("True Positives", tp)
        cm_col2.metric("False Positives", fp)
        cm_col3.metric("True Negatives", tn)
        cm_col4.metric("False Negatives", fn)

        st.markdown(
            """
            **Interprétation rapide :**
            - **Precision** : parmi les transactions signalées, combien sont réellement frauduleuses.
            - **Recall** : parmi les vraies fraudes, combien le système détecte.
            - **F1-score** : équilibre entre precision et recall.
            - **False Negatives** : fraudes ratées. C'est la métrique la plus critique.
            """
        )

        matrix_df = load_data("""
            SELECT
                actual_label,
                transaction_status,
                COUNT(*) AS total
            FROM transactions
            WHERE actual_label IS NOT NULL
            GROUP BY actual_label, transaction_status
            ORDER BY actual_label, transaction_status;
        """)

        if not matrix_df.empty:
            matrix_col1, matrix_col2 = st.columns(2)

            with matrix_col1:
                st.markdown("#### Matrice actual_label vs transaction_status")
                st.dataframe(matrix_df, use_container_width=True)

            with matrix_col2:
                fig_matrix = px.bar(
                    matrix_df,
                    x="transaction_status",
                    y="total",
                    color="actual_label",
                    barmode="group",
                    title="Décision du système par label réel"
                )
                st.plotly_chart(fig_matrix, use_container_width=True)

        performance_by_label_df = load_data("""
            SELECT
                actual_label,
                COUNT(*) AS total,
                COALESCE(AVG(ml_probability), 0) AS avg_ml_probability,
                COALESCE(AVG(final_score), 0) AS avg_final_score,
                COALESCE(MAX(final_score), 0) AS max_final_score
            FROM transactions
            WHERE actual_label IS NOT NULL
            GROUP BY actual_label
            ORDER BY actual_label;
        """)

        if not performance_by_label_df.empty:
            st.markdown("#### Analyse des scores par label réel")
            st.dataframe(performance_by_label_df, use_container_width=True)

    else:
        st.info("Aucune donnée avec actual_label disponible pour évaluer le modèle.")

    st.divider()

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

    st.subheader("Dernières transactions traitées")

    latest_transactions = load_data(f"""
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
            ml_probability,
            behavior_score,
            final_score,
            transaction_status,
            actual_label,
            detection_method,
            transaction_timestamp,
            processed_at
        FROM transactions
        WHERE transaction_status IN ({status_filter})
        ORDER BY processed_at DESC
        LIMIT 30;
    """)

    st.dataframe(latest_transactions, use_container_width=True)

    st.divider()

    st.subheader("Dernières alertes")

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
        LIMIT 30;
    """)

    st.dataframe(latest_alerts, use_container_width=True)

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()