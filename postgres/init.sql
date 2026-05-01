CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    usual_country VARCHAR(100),
    risk_level VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id VARCHAR(100) PRIMARY KEY,
    merchant_name VARCHAR(150),
    merchant_category VARCHAR(100),
    risk_level VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(150) PRIMARY KEY,
    user_id VARCHAR(100),
    amount DECIMAL(12, 2),
    currency VARCHAR(10),
    country VARCHAR(100),
    usual_country VARCHAR(100),
    merchant_category VARCHAR(100),
    payment_method VARCHAR(50),
    device_id VARCHAR(100),
    ip_address VARCHAR(100),
    transaction_timestamp TIMESTAMP,
    transaction_status VARCHAR(50),
    risk_score INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS risk_scores (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(150),
    amount_score INTEGER DEFAULT 0,
    country_score INTEGER DEFAULT 0,
    merchant_score INTEGER DEFAULT 0,
    time_score INTEGER DEFAULT 0,
    device_score INTEGER DEFAULT 0,
    frequency_score INTEGER DEFAULT 0,
    final_score INTEGER,
    decision VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(150),
    user_id VARCHAR(100),
    risk_score INTEGER,
    alert_type VARCHAR(100),
    message TEXT,
    status VARCHAR(50) DEFAULT 'OPEN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_reports (
    report_id SERIAL PRIMARY KEY,
    report_date DATE,
    total_transactions INTEGER,
    total_normal INTEGER,
    total_suspicious INTEGER,
    total_fraud INTEGER,
    total_fraud_amount DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id 
ON transactions(user_id);

CREATE INDEX IF NOT EXISTS idx_transactions_status 
ON transactions(transaction_status);

CREATE INDEX IF NOT EXISTS idx_transactions_risk_score 
ON transactions(risk_score);

CREATE INDEX IF NOT EXISTS idx_transactions_processed_at 
ON transactions(processed_at);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_status 
ON fraud_alerts(status);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_created_at 
ON fraud_alerts(created_at);