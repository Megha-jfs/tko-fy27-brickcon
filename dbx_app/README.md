# Personalized Shopper Recommendations & Loyalty Engine

**Industry:** Retail | **Databricks App Demo** | TKO FY27 BrickCon

---

## The Problem

Retailers face three compounding challenges that erode revenue and customer loyalty:

1. **Relevance Fatigue** — Customers are bombarded with generic marketing. Click-through rates decline, and brand trust erodes when every shopper gets the same email blast regardless of their preferences.

2. **Static Loyalty Programs** — Legacy systems update points once a day and fail to capitalize on real-time shopper intent. A customer actively browsing denim right now won't see a relevant offer until tomorrow — if at all.

3. **The Conversion Gap** — The "perfect offer" consistently arrives too late. Without a unified view of browsing behavior, purchase history, and preferences, retailers miss the narrow window where a personalized nudge converts a browser into a buyer.

The result: missed conversion opportunities, declining customer lifetime value, and an inability to build high-value relationships in a hyper-competitive market.

---

## How We Solved It

We built a **Hyper-Personalized Loyalty Engine** on Databricks that transforms retail from generic to real-time personalized — unifying the entire shopper journey into a single platform.

### Data Foundation — Single Source of Truth

Mock datasets simulating real retail data (200 products, 500 customers, 3,000 transactions, 5,000 clickstream events) are ingested through a **Lakeflow Spark Declarative Pipeline** that builds a medallion architecture:

- **Bronze**: Raw ingestion from Unity Catalog Volumes
- **Silver**: Enriched clickstream joined with product metadata, plus a **Customer Golden Record** with masked PII and computed churn risk
- **Gold**: Real-time **Category Intent Scores** (weighted by action type + recency decay) and **Customer Segments** (Win-Back, VIP Active, Rising Star, At Risk, New & Engaged)

### Real-Time Personalization

Intent scores update based on what a customer is doing *right now*:
- Adding to cart scores 3x higher than a page view
- Events from the last 6 hours get a 2x recency boost
- Each customer's top interests are ranked and fed to the recommendation engine

A **Databricks Connect + Vector Search** recommendation engine matches products to a shopper's "vibe" — not just keywords, but the semantic meaning of their browsing patterns and style preferences.

### Operational Speed

**Lakebase (Serverless Postgres)** serves as the state store, holding live offer baskets, session data, and pre-computed recommendation scores for sub-second retrieval by the frontend — handling millions of concurrent product lookups.

### Conversational BI for Marketers

A **Genie Space** labeled "Certified Retail SQL" lets marketing teams query the data naturally:
> *"Show me our top 10% of shoppers who haven't bought in 30 days but have browsed denim recently."*

No SQL knowledge required — Genie understands retail metrics like LTV, Churn Risk, and Intent Scores out of the box.

### Customer-Facing App

A **Databricks App** serves as the customer portal, showing each shopper:
- Their loyalty tier, points balance, and progress to the next tier
- Personalized offers generated from their browsing behavior (discounts, bonus points, early access)
- AI-ranked product recommendations with match scores
- A live session activity feed
- An **AI Style Assistant** chatbot for personalized fashion advice

### Privacy by Design

PII (credit card numbers, full phone numbers) is stripped at the Silver layer. Unity Catalog enforces column-level permissions so marketing agents never see raw sensitive data. The app authenticates via OAuth 2.0 client credentials — no passwords, no tokens in code.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Databricks Platform                              │
│                                                                          │
│  ┌─────────────────┐                                                     │
│  │  Unity Catalog   │    Raw CSVs uploaded to Volumes                     │
│  │  (Volumes/Tags)  │                                                     │
│  └───────┬─────────┘                                                     │
│          │                                                               │
│          ▼                                                               │
│  ┌──────────────────────────────────────────┐                            │
│  │  Lakeflow Spark Declarative Pipeline      │                            │
│  │                                           │                            │
│  │  Bronze ──▶ Silver ──▶ Gold               │                            │
│  │  (Raw)     (Enriched,  (Intent Scores,    │                            │
│  │             Golden      Segments)          │                            │
│  │             Record)                        │                            │
│  └──────┬──────────────────┬────────────────┘                            │
│         │                  │                                              │
│         ▼                  ▼                                              │
│  ┌──────────────┐   ┌──────────────────────┐   ┌──────────────────────┐  │
│  │ Genie Space  │   │ Model Serving         │   │ Lakebase             │  │
│  │ ───────────  │   │ ─────────────         │   │ ────────             │  │
│  │ Conversational│   │ RAG Style Assistant   │   │ Personalized Offers  │  │
│  │ BI for       │   │ + Vector Search       │   │ Session State        │  │
│  │ Marketers    │   │ (Vibe Matching)       │   │ Recommendation       │  │
│  └──────────────┘   └──────────┬───────────┘   │ Scores               │  │
│                                │               └──────────┬───────────┘  │
│                                │                          │              │
│                                ▼                          ▼              │
│                     ┌──────────────────────────────────────────────┐     │
│                     │          Databricks App (Streamlit)           │     │
│                     │                                              │     │
│                     │  Sidebar:              Main Area:             │     │
│                     │  • Customer Selector   • Loyalty Dashboard    │     │
│                     │  • AI Style Assistant   • Personalized Offers │     │
│                     │    Chat                 • Product Recs (9)    │     │
│                     │                        • Live Activity Feed   │     │
│                     │                        • Interest Charts      │     │
│                     └──────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Databricks Services Used

| Service | What It Does Here |
|---|---|
| **Unity Catalog** | Single source of truth — Volumes for raw data, tables for the Golden Record, Tags for segment-based AI prioritization |
| **Lakeflow Declarative Pipelines** | Medallion pipeline (Bronze/Silver/Gold) with real-time intent scoring and customer segmentation |
| **Genie** | Natural language BI — marketers ask questions like "who's at risk of churning?" without writing SQL |
| **Lakebase** | Serverless Postgres state store for sub-second offer serving and session tracking |
| **Model Serving** | Hosts the RAG-powered Style Assistant agent |
| **Vector Search** | Semantic "vibe matching" — finds products that match a shopper's style, not just keywords |
| **Databricks Apps** | Customer Portal frontend deployed as a managed Streamlit app |
| **Databricks Connect** | Develop and test Spark recommendation queries locally in Cursor IDE |

---

## Project Structure

```
dbx_app/
├── app.py                              # Customer Portal (Databricks App)
├── app.yaml                            # App launch config
├── requirements.txt                    # Python dependencies
├── mock_data/
│   ├── generate_mock_data.py           # Generates all mock retail CSVs
│   └── output/                         # 200 products, 500 customers, 3K txns, 5K events
├── pipelines/
│   └── clickstream_pipeline.py         # Lakeflow pipeline (Bronze → Silver → Gold)
├── genie/
│   └── genie_instructions.sql          # Genie Space setup instructions
├── lakebase/
│   └── schema.sql                      # Lakebase DDL (5 operational tables)
└── recommendations/
    └── recommendation_engine.py        # Databricks Connect + Vector Search
```

---

**Author:** Megha Upadhyay | **March 2026**
