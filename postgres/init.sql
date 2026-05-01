CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    full_name VARCHAR(150),
    usual_country VARCHAR(100),
    risk_level VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id VARCHAR(100) PRIMARY KEY,
    merchant_name VARCHAR(150),
    category VARCHAR(100),
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
    transaction_status VARCHAR(50),
    risk_score INTEGER,
    created_at TIMESTAMP
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



INSERT INTO users (user_id, full_name, usual_country, risk_level)
VALUES
('U001', 'User One', 'Morocco', 'LOW'),
('U002', 'User Two', 'France', 'LOW'),
('U003', 'User Three', 'Spain', 'MEDIUM'),
('U004', 'User Four', 'Germany', 'LOW'),
('U005', 'User Five', 'Morocco', 'MEDIUM')
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO merchants (merchant_id, merchant_name, category, risk_level)
VALUES
('M001', 'Local Grocery Store', 'Grocery', 'LOW'),
('M002', 'Online Shop', 'Online Shopping', 'LOW'),
('M003', 'Crypto Exchange', 'Crypto', 'HIGH'),
('M004', 'Luxury Market', 'Luxury', 'HIGH'),
('M005', 'Gaming Platform', 'Gaming', 'MEDIUM')
ON CONFLICT (merchant_id) DO NOTHING;