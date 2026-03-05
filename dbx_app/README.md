# Hyper-Personalized Loyalty App — Databricks Demo

## Overview

A **Databricks App** demo that simulates a hyper-personalized retail loyalty engine. The app serves as a mock "Customer Portal" showing real-time personalized offers, live product recommendations, loyalty status, and an AI-powered Style Assistant — all built on the Databricks platform.

**Industry:** Retail | **Use Case:** Personalized Shopper Recommendations & Loyalty Engine

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Databricks Platform                                  │
│                                                                             │
│  ┌──────────────┐    ┌────────────────────┐    ┌─────────────────────────┐  │
│  │ Unity Catalog │    │ Lakeflow Pipeline   │    │ Model Serving           │  │
│  │ ─────────────│    │ ──────────────────  │    │ ───────────────────     │  │
│  │ • Volumes     │───▶│ Bronze → Silver     │───▶│ RAG Agent (Style       │  │
│  │   (CSV/JSON)  │    │   → Gold            │    │  Assistant) + Vector   │  │
│  │ • Tables      │    │ • Intent Scores     │    │  Search                │  │
│  │ • Tags        │    │ • Churn Risk        │    └──────────┬──────────────┘  │
│  └──────────────┘    │ • Segments          │               │                │
│                      └────────┬───────────┘               │                │
│                               │                            │                │
│                               ▼                            │                │
│  ┌────────────────────────────────────────┐                │                │
│  │ Lakebase (Serverless Postgres)         │                │                │
│  │ ──────────────────────────────         │                │                │
│  │ • personalized_offers                  │◀───────────────┘                │
│  │ • recommendation_scores               │                                 │
│  │ • active_sessions                      │                                 │
│  │ • loyalty_ledger                       │                                 │
│  │ • customer_state (cache)               │                                 │
│  └────────────────┬───────────────────────┘                                │
│                   │                                                         │
│                   ▼                                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   Databricks App (Streamlit)                         │   │
│  │  ┌──────────────────────┐  ┌─────────────────────────────────────┐  │   │
│  │  │ Sidebar              │  │ Main Area                           │  │   │
│  │  │ • Customer Selector  │  │ • Loyalty Dashboard (tier, points)  │  │   │
│  │  │ • Style Assistant    │  │ • Personalized Offers               │  │   │
│  │  │   Chat (AI Agent)    │  │ • Product Recommendations           │  │   │
│  │  └──────────────────────┘  │ • Live Session Activity             │  │   │
│  │                            │ • Interest Profile Charts           │  │   │
│  │                            └─────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Genie Space — "Certified Retail SQL"                                │   │
│  │ Marketers query: "Top 10% shoppers who haven't bought in 30 days    │   │
│  │   but browsed denim recently"                                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
dbx_app/
├── app.py                                # Databricks App — Customer Portal (Streamlit)
├── app.yaml                              # App launch configuration
├── requirements.txt                      # Python dependencies
├── README.md                             # This file
│
├── mock_data/
│   ├── generate_mock_data.py             # Script to generate all mock CSVs
│   └── output/
│       ├── products_catalog.csv          # 200 products across 8 categories
│       ├── customer_profiles.csv         # 500 customer profiles
│       ├── purchase_history.csv          # 3,000 transactions
│       └── clickstream_events.csv        # 5,000 browsing events (72h window)
│
├── pipelines/
│   └── clickstream_pipeline.py           # Lakeflow Spark Declarative Pipeline (DLT)
│
├── genie/
│   └── genie_instructions.sql            # Instructions for Genie Space setup
│
├── lakebase/
│   └── schema.sql                        # Lakebase (Serverless Postgres) DDL
│
└── recommendations/
    └── recommendation_engine.py          # Databricks Connect + Vector Search
```

---

## Quick Start (Demo Setup)

### 1. Generate & Upload Mock Data

```bash
# Generate CSVs locally
python3 dbx_app/mock_data/generate_mock_data.py

# Upload to Unity Catalog Volume (via Databricks UI or CLI)
# Target: /Volumes/<catalog>/<schema>/raw_data/
```

Upload the four CSVs from `mock_data/output/` to your Unity Catalog Volume.

### 2. Create Unity Catalog Resources

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS tko_fy27_brickcon;
CREATE SCHEMA IF NOT EXISTS tko_fy27_brickcon.loyalty_engine;

-- Create a Volume for raw data files
CREATE VOLUME IF NOT EXISTS tko_fy27_brickcon.loyalty_engine.raw_data;

-- After uploading CSVs, register them as tables (pipeline handles this too)
```

### 3. Run the Lakeflow Pipeline

Import `pipelines/clickstream_pipeline.py` as a DLT pipeline notebook:
- Workspace > Create > Pipeline
- Source: select the notebook
- Target catalog: `tko_fy27_brickcon`
- Target schema: `loyalty_engine`

This creates:
| Layer | Table | Description |
|---|---|---|
| Bronze | `bronze_clickstream` | Raw clickstream events |
| Bronze | `bronze_products` | Raw product catalog |
| Bronze | `bronze_customers` | Raw customer profiles |
| Bronze | `bronze_purchases` | Raw purchase history |
| Silver | `silver_clickstream_enriched` | Events joined with product details |
| Silver | `silver_customer_golden_record` | Customer SoT with masked PII + churn risk |
| Gold | `gold_category_intent_scores` | Real-time intent scores per customer × category |
| Gold | `gold_high_value_segments` | Segment-tagged customers for marketing campaigns |

### 4. Set Up Genie Space

1. Go to **Genie** in the Databricks sidebar
2. Create a new space: "Certified Retail SQL"
3. Add tables: `gold_category_intent_scores`, `gold_high_value_segments`, `silver_clickstream_enriched`, etc.
4. Paste instructions from `genie/genie_instructions.sql` into the **General Instructions** panel

Example queries marketers can ask:
- "Show me our top 10% of shoppers who haven't bought in 30 days but have browsed denim recently"
- "What are the trending categories this week?"
- "Which Win-Back segment customers have the highest LTV?"

### 5. Set Up Lakebase

1. In your catalog, create a Lakebase database
2. Run the DDL from `lakebase/schema.sql`
3. Tables created: `personalized_offers`, `active_sessions`, `recommendation_scores`, `loyalty_ledger`, `customer_state`

### 6. Deploy the Databricks App

1. **Compute > Apps > Create App** — name it (e.g., `loyalty-portal-app`)
2. Upload: `app.py`, `app.yaml`, `requirements.txt`
3. Add app resources (serving endpoint with "Can Query" permission)
4. Deploy — wait 2-3 minutes
5. Open the app URL

---

## Component Details

### Mock Data Generator (`mock_data/generate_mock_data.py`)

| Dataset | Rows | Key Fields |
|---|---|---|
| `products_catalog.csv` | 200 | product_id, category, brand, price, style_tags, rating |
| `customer_profiles.csv` | 500 | customer_id, loyalty_tier, points, LTV, preferred_categories |
| `purchase_history.csv` | 3,000 | transaction_id, customer_id, product_id, amount, channel |
| `clickstream_events.csv` | 5,000 | event_id, customer_id, session_id, event_type, category_browsed |

8 product categories: Denim, Tops, Shoes, Accessories, Outerwear, Activewear, Dresses, Swimwear

### Lakeflow Pipeline (`pipelines/clickstream_pipeline.py`)

**Intent Score Calculation:**
- `add_to_cart` → 3 points
- `wishlist_add` → 2 points
- `search` → 1.5 points
- `product_view` → 1 point
- `category_browse` → 0.5 points
- Recency boost: <6h = 2x, <12h = 1.5x, <24h = 1.2x

**Customer Segments:**
| Segment | Criteria |
|---|---|
| Win-Back | LTV > $3k, no purchase in 30+ days |
| VIP Active | Platinum/Gold, purchased within 14 days |
| Rising Star | Silver tier, intent score > 15 |
| At Risk | High churn risk (60+ days) |
| New & Engaged | ≤3 orders or new signup |

### Databricks App (`app.py`)

The Customer Portal displays:
- **Loyalty Dashboard**: Tier badge, points balance, progress to next tier, LTV, churn risk
- **Personalized Offers**: Dynamic offer cards (discounts, bonus points, early access) based on browsing
- **Product Recommendations**: 9 product cards ranked by match score from Vector Search
- **Interest Profile**: Live bar chart of category intent scores
- **Session Activity Feed**: Real-time browsing activity timeline
- **Style Assistant** (sidebar): AI chatbot for personalized fashion advice

### Recommendation Engine (`recommendations/recommendation_engine.py`)

Uses **Databricks Connect** for Spark queries and **Vector Search** for semantic product matching:
- `get_customer_top_interests()` — pulls top 3 category interests via Spark SQL
- `find_similar_products_by_vibe()` — Vector Search for "vibe matching" products
- `get_style_recommendations()` — full pipeline: interests → matching products
- `compute_recommendation_scores()` — pre-computes scores for Lakebase storage

---

## Privacy & Security

- **PII Masking**: `credit_card_last4` is stripped in the Silver layer pipeline. The golden record never exposes raw PII.
- **Unity Catalog Permissions**: Row-level and column-level security enforced — marketing agents cannot see unmasked sensitive fields.
- **OAuth 2.0**: App authentication uses client credentials flow (no passwords). Tokens auto-refresh every 50 minutes.

---

## Databricks Services Used

| Service | Role in Demo |
|---|---|
| **Unity Catalog** | Golden Record schema, Volumes for raw data, Tags for segments |
| **Lakeflow Spark Declarative Pipelines** | Bronze → Silver → Gold medallion pipeline with intent scoring |
| **Databricks Genie** | Conversational BI for marketers — natural language SQL queries |
| **Lakebase (Serverless Postgres)** | Sub-second state store for offers, sessions, recommendation scores |
| **Model Serving** | RAG-powered Style Assistant agent with Vector Search |
| **Databricks Apps** | Customer Portal frontend (Streamlit) |
| **Databricks Connect** | Local development of Spark recommendation queries in Cursor |
| **Vector Search** | Semantic "vibe matching" of products to shopper preferences |

---

**Author**: Megha Upadhyay
**Last Updated**: March 2026
