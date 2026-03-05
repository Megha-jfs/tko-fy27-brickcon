-- =============================================================================
-- GENIE SPACE INSTRUCTIONS — "Certified Retail SQL"
-- =============================================================================
-- Paste these instructions into the Genie Space configuration panel.
-- They teach Genie how to interpret retail metrics using the simulated data.
-- =============================================================================

-- GENIE SPACE NAME: Certified Retail SQL — Loyalty & Personalization
-- DESCRIPTION: Ask questions about customer loyalty, shopping behavior,
--              product performance, and personalized recommendations using
--              simulated retail data.

-- ---------------------------------------------------------------------------
-- TABLE DESCRIPTIONS (paste into Genie "Table Instructions")
-- ---------------------------------------------------------------------------

-- TABLE: gold_category_intent_scores
-- Contains real-time interest signals for each customer × category pair.
-- Columns:
--   customer_id        — unique customer identifier (e.g. CUST-0001)
--   resolved_category  — product category (Denim, Tops, Shoes, etc.)
--   intent_score       — weighted engagement score (higher = more interested NOW)
--   event_count        — number of interactions in the last 48 hours
--   latest_interaction — timestamp of the most recent event
--   category_rank      — 1 = top interest category for this customer

-- TABLE: gold_high_value_segments
-- Customer profiles enriched with segment tags for marketing campaigns.
-- Columns:
--   customer_id, first_name, last_name, loyalty_tier, loyalty_points,
--   lifetime_value (LTV in USD), total_orders, days_since_last_purchase,
--   churn_risk (High/Medium/Low), total_intent_score, segment,
--   preferred_categories (JSON array), city, state

-- TABLE: silver_clickstream_enriched
-- Detailed browsing events joined with product metadata.
-- Use this for granular session analysis and browsing pattern discovery.

-- TABLE: silver_customer_golden_record
-- The single source of truth for customer data with masked PII.
-- Includes aggregated purchase stats and churn risk calculation.

-- TABLE: bronze_purchases
-- Raw purchase history — transaction_id, customer_id, product_id,
-- purchase_date, quantity, unit_price, total_amount, payment_method, channel.

-- TABLE: bronze_products
-- Product catalog — product_id, product_name, category, subcategory,
-- brand, price, description, style_tags, color, rating, review_count.


-- ---------------------------------------------------------------------------
-- METRIC DEFINITIONS (paste into Genie "General Instructions")
-- ---------------------------------------------------------------------------

-- When a user asks about "Recent Interest" or "Current Interest":
--   → Query gold_category_intent_scores WHERE category_rank = 1
--   → This shows each customer's #1 category interest right now
--   → Intent scores are computed from the last 48 hours of clickstream data
--   → Scoring: add_to_cart=3pts, wishlist_add=2pts, search=1.5pts,
--              product_view=1pt, category_browse=0.5pts
--   → Recency boost: events < 6h ago get 2x, < 12h get 1.5x, < 24h get 1.2x

-- When a user asks about "LTV" or "Lifetime Value":
--   → Use lifetime_value from gold_high_value_segments
--   → This is the total historical spend in USD for the customer
--   → "High LTV" means lifetime_value > $5,000
--   → "Top 10% of shoppers" = ORDER BY lifetime_value DESC LIMIT (count * 0.10)

-- When a user asks about "Churn Risk":
--   → Use churn_risk from silver_customer_golden_record or gold_high_value_segments
--   → Calculation: >60 days since last purchase = High,
--                  >30 days = Medium, ≤30 days = Low
--   → "At Risk" segment in gold_high_value_segments also flags high-churn customers

-- When a user asks about "Top Shoppers" or "VIP Customers":
--   → Filter gold_high_value_segments WHERE loyalty_tier IN ('Platinum', 'Gold')
--   → Or ORDER BY lifetime_value DESC

-- When a user asks "who hasn't bought in X days but browsed Y recently":
--   → JOIN gold_high_value_segments (for days_since_last_purchase)
--     WITH gold_category_intent_scores (for resolved_category and intent_score)
--   → Example: "Top 10% shoppers who haven't bought in 30 days but browsed denim"
--     SELECT s.customer_id, s.first_name, s.last_name,
--            s.lifetime_value, s.days_since_last_purchase,
--            i.intent_score as denim_interest
--     FROM gold_high_value_segments s
--     JOIN gold_category_intent_scores i
--       ON s.customer_id = i.customer_id
--     WHERE s.days_since_last_purchase > 30
--       AND i.resolved_category = 'Denim'
--       AND i.intent_score > 0
--     ORDER BY s.lifetime_value DESC
--     LIMIT (SELECT CAST(COUNT(*) * 0.10 AS INT) FROM gold_high_value_segments);

-- When a user asks about "Segments":
--   → Use the segment column from gold_high_value_segments
--   → Segment definitions:
--       Win-Back     = High LTV (>$3k) but no purchase in 30+ days
--       VIP Active   = Platinum/Gold tier with purchase in last 14 days
--       Rising Star  = Silver tier with high recent engagement (intent > 15)
--       At Risk      = High churn_risk score
--       New & Engaged = Few orders or no purchase history yet
--       Steady       = Default / healthy baseline

-- When a user asks about "Conversion" or "Add to Cart Rate":
--   → Calculate from silver_clickstream_enriched:
--     add_to_cart_rate = COUNT(event_type='add_to_cart') / COUNT(event_type='product_view')
--   → Group by resolved_category for category-level conversion rates

-- When a user asks about "Trending Categories" or "What's hot":
--   → SELECT resolved_category, SUM(intent_score) as total_interest,
--            COUNT(DISTINCT customer_id) as unique_shoppers
--     FROM gold_category_intent_scores
--     GROUP BY resolved_category
--     ORDER BY total_interest DESC

-- When a user asks about product performance or "Best Sellers":
--   → JOIN bronze_purchases WITH bronze_products on product_id
--   → Aggregate by product_name, category, brand

-- IMPORTANT RULES FOR GENIE:
-- 1. Always mask or exclude credit_card_last4 — never show raw PII
-- 2. Use silver_customer_golden_record as the canonical customer table (PII already masked)
-- 3. When showing customer names, always use first_name + last_name (never email)
-- 4. Date calculations should use CURRENT_DATE() for "days ago" logic
-- 5. Default time window for "recent" activity = last 48 hours unless specified
-- 6. Format currency values with $ and 2 decimal places
-- 7. Format large numbers with commas for readability
