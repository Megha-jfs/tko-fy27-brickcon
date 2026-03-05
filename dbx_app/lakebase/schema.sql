-- =============================================================================
-- LAKEBASE (Serverless Postgres) SCHEMA
-- State Store for the Personalized Loyalty Engine
-- =============================================================================
-- Lakebase provides sub-second reads for the frontend app.
-- These tables are populated by the recommendation pipeline and queried
-- by the Databricks App for real-time offer serving.
-- =============================================================================

-- Run this in your Lakebase (Serverless Postgres) instance:
--   Catalog > your_catalog > Create Lakebase Database

-- ---------------------------------------------------------------------------
-- 1. Personalized Offers — the "Offer Basket" for each customer
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS personalized_offers (
    id                  SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    offer_code          VARCHAR(50) NOT NULL,
    product_id          VARCHAR(20),
    product_name        VARCHAR(200),
    category            VARCHAR(50),
    relevance_score     FLOAT NOT NULL,          -- 0.0 to 1.0, higher = better match
    offer_type          VARCHAR(30) NOT NULL,     -- 'discount', 'bonus_points', 'early_access', 'bundle'
    offer_value         VARCHAR(50),              -- e.g., '20%', '500 pts', 'Early Access'
    reason              TEXT,                     -- why this offer was generated
    expires_at          TIMESTAMP NOT NULL,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    served_at           TIMESTAMP,                -- when the customer actually saw it
    redeemed_at         TIMESTAMP                 -- when/if the customer used it
);

CREATE INDEX idx_offers_customer ON personalized_offers (customer_id, is_active);
CREATE INDEX idx_offers_relevance ON personalized_offers (customer_id, relevance_score DESC);
CREATE INDEX idx_offers_expiry ON personalized_offers (expires_at) WHERE is_active = TRUE;

-- ---------------------------------------------------------------------------
-- 2. Active Sessions — tracks what customers are doing RIGHT NOW
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS active_sessions (
    session_id          VARCHAR(30) PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    device_type         VARCHAR(20),
    current_page        VARCHAR(200),
    categories_viewed   JSONB DEFAULT '[]'::JSONB,
    products_viewed     JSONB DEFAULT '[]'::JSONB,
    cart_items          JSONB DEFAULT '[]'::JSONB,
    session_intent      JSONB DEFAULT '{}'::JSONB    -- real-time intent scores for this session
);

CREATE INDEX idx_sessions_customer ON active_sessions (customer_id);
CREATE INDEX idx_sessions_activity ON active_sessions (last_activity_at DESC);

-- ---------------------------------------------------------------------------
-- 3. Recommendation Scores — pre-computed "Live Recommendation Scores"
--    Written by the streaming pipeline, read by the frontend
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recommendation_scores (
    id                  SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    product_id          VARCHAR(20) NOT NULL,
    score               FLOAT NOT NULL,            -- similarity/relevance score
    score_type          VARCHAR(30) NOT NULL,       -- 'collaborative', 'content_based', 'trending', 'vibe_match'
    reasoning           TEXT,                       -- short explanation for why recommended
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (customer_id, product_id, score_type)
);

CREATE INDEX idx_reco_customer ON recommendation_scores (customer_id, score DESC);
CREATE INDEX idx_reco_type ON recommendation_scores (score_type);

-- ---------------------------------------------------------------------------
-- 4. Loyalty Ledger — real-time points balance (faster than querying Delta)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS loyalty_ledger (
    id                  SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    event_type          VARCHAR(30) NOT NULL,       -- 'earn', 'redeem', 'bonus', 'expire'
    points              INTEGER NOT NULL,            -- positive for earn, negative for redeem
    balance_after       INTEGER NOT NULL,
    description         VARCHAR(200),
    reference_id        VARCHAR(50),                 -- transaction_id or offer_code
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ledger_customer ON loyalty_ledger (customer_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- 5. Customer State Cache — denormalized snapshot for sub-second portal loads
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_state (
    customer_id         VARCHAR(20) PRIMARY KEY,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    loyalty_tier        VARCHAR(20),
    loyalty_points      INTEGER DEFAULT 0,
    lifetime_value      FLOAT DEFAULT 0.0,
    segment             VARCHAR(30),
    churn_risk          VARCHAR(10),
    top_category        VARCHAR(50),                 -- current #1 interest
    active_offers_count INTEGER DEFAULT 0,
    last_session_at     TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Sample Insert — demonstrates how the pipeline populates offers
-- ---------------------------------------------------------------------------
-- INSERT INTO personalized_offers
--     (customer_id, offer_code, product_id, product_name, category,
--      relevance_score, offer_type, offer_value, reason, expires_at)
-- VALUES
--     ('CUST-0042', 'DENIM-20OFF-0042', 'PROD-1023', 'UrbanEdge Slim Jeans - Black',
--      'Denim', 0.94, 'discount', '20% Off',
--      'You''ve been browsing denim for the last 2 hours — here''s an exclusive deal!',
--      CURRENT_TIMESTAMP + INTERVAL '24 hours'),
--
--     ('CUST-0042', 'BONUS-500PTS-0042', NULL, NULL,
--      NULL, 0.87, 'bonus_points', '500 Bonus Points',
--      'Complete a purchase today and earn 500 bonus loyalty points!',
--      CURRENT_TIMESTAMP + INTERVAL '12 hours');
