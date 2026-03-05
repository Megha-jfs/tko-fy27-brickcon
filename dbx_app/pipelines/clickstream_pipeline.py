# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Spark Declarative Pipeline — Clickstream Ingestion & Intent Scoring
# MAGIC
# MAGIC This pipeline reads mock clickstream events from a Unity Catalog Volume,
# MAGIC joins with the products catalog, and calculates real-time **Category Intent Scores**
# MAGIC for each customer.
# MAGIC
# MAGIC **Pipeline layers:**
# MAGIC - **Bronze**: Raw ingestion from CSV files in the Volume
# MAGIC - **Silver**: Cleaned, enriched clickstream joined with products
# MAGIC - **Gold**: Aggregated intent scores per customer × category

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Update these to match your Unity Catalog configuration
CATALOG = "catalog_tkofy27_9ev672"
SCHEMA = "shopper_recommendation"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer — Raw Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested from CSV volume",
    table_properties={"quality": "bronze"},
)
def bronze_clickstream():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/clickstream_events.csv")
    )


@dlt.table(
    name="bronze_products",
    comment="Raw products catalog ingested from CSV volume",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/products_catalog.csv")
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer profiles ingested from CSV volume",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/customer_profiles.csv")
    )


@dlt.table(
    name="bronze_purchases",
    comment="Raw purchase history ingested from CSV volume",
    table_properties={"quality": "bronze"},
)
def bronze_purchases():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{VOLUME_PATH}/purchase_history.csv")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Cleaned & Enriched

# COMMAND ----------

@dlt.table(
    name="silver_clickstream_enriched",
    comment="Clickstream events joined with product details — cleaned and typed",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_clickstream_enriched():
    clickstream = dlt.read("bronze_clickstream").withColumn(
        "event_timestamp", F.to_timestamp("event_timestamp")
    )

    products = dlt.read("bronze_products").select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category").alias("product_category"),
        F.col("subcategory").alias("product_subcategory"),
        F.col("brand").alias("product_brand"),
        F.col("price").alias("product_price"),
        F.col("style_tags").alias("product_style_tags"),
    )

    enriched = clickstream.join(
        products,
        on="product_id",
        how="left",
    )

    return enriched.withColumn(
        "resolved_category",
        F.coalesce(F.col("product_category"), F.col("category_browsed")),
    )


@dlt.table(
    name="silver_customer_golden_record",
    comment="Customer golden record with masked PII — the single source of truth",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_customer_golden_record():
    customers = dlt.read("bronze_customers")
    purchases = dlt.read("bronze_purchases")

    purchase_agg = purchases.groupBy("customer_id").agg(
        F.count("*").alias("total_transactions"),
        F.sum("total_amount").alias("total_spend"),
        F.avg("total_amount").alias("avg_order_value"),
        F.max("purchase_date").alias("most_recent_purchase"),
        F.countDistinct("product_id").alias("unique_products_purchased"),
    )

    return (
        customers
        .drop("credit_card_last4")  # strip raw PII
        .join(purchase_agg, on="customer_id", how="left")
        .withColumn(
            "days_since_last_purchase",
            F.datediff(F.current_date(), F.to_date("most_recent_purchase")),
        )
        .withColumn(
            "churn_risk",
            F.when(F.col("days_since_last_purchase") > 60, "High")
            .when(F.col("days_since_last_purchase") > 30, "Medium")
            .otherwise("Low"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer — Intent Scores & Segment Tags

# COMMAND ----------

@dlt.table(
    name="gold_category_intent_scores",
    comment="Real-time category intent scores per customer based on last 48h clickstream",
    table_properties={"quality": "gold"},
)
def gold_category_intent_scores():
    """
    Calculates how interested each customer is in each category right now.

    Scoring weights:
      - product_view  → 1 point
      - add_to_cart   → 3 points
      - wishlist_add  → 2 points
      - category_browse → 0.5 points
      - search (with matching category) → 1.5 points

    Only considers events from the last 48 hours for recency.
    """
    events = dlt.read("silver_clickstream_enriched").filter(
        F.col("event_timestamp") >= F.date_sub(F.current_timestamp(), 2)
    )

    scored = events.withColumn(
        "intent_weight",
        F.when(F.col("event_type") == "add_to_cart", 3.0)
        .when(F.col("event_type") == "wishlist_add", 2.0)
        .when(F.col("event_type") == "search", 1.5)
        .when(F.col("event_type") == "product_view", 1.0)
        .when(F.col("event_type") == "category_browse", 0.5)
        .otherwise(0.0),
    )

    # Recency decay: events closer to now score higher
    scored = scored.withColumn(
        "hours_ago",
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("event_timestamp")) / 3600,
    ).withColumn(
        "recency_multiplier",
        F.when(F.col("hours_ago") <= 6, 2.0)
        .when(F.col("hours_ago") <= 12, 1.5)
        .when(F.col("hours_ago") <= 24, 1.2)
        .otherwise(1.0),
    ).withColumn(
        "weighted_score", F.col("intent_weight") * F.col("recency_multiplier")
    )

    intent = scored.filter(F.col("resolved_category").isNotNull()).groupBy(
        "customer_id", "resolved_category"
    ).agg(
        F.round(F.sum("weighted_score"), 2).alias("intent_score"),
        F.count("*").alias("event_count"),
        F.max("event_timestamp").alias("latest_interaction"),
    )

    # Rank categories per customer
    window = Window.partitionBy("customer_id").orderBy(F.desc("intent_score"))
    return intent.withColumn("category_rank", F.row_number().over(window))


@dlt.table(
    name="gold_high_value_segments",
    comment="Customer segments tagged for AI prioritization",
    table_properties={"quality": "gold"},
)
def gold_high_value_segments():
    """
    Tags customers into actionable segments:
      - 'Win-Back'          : High LTV but no purchase in 30+ days
      - 'VIP Active'        : Platinum/Gold tier with recent purchases
      - 'Rising Star'       : Silver tier with high recent engagement
      - 'At Risk'           : High churn_risk score
      - 'New & Engaged'     : Signed up recently with multiple sessions
    """
    customers = dlt.read("silver_customer_golden_record")
    intent = dlt.read("gold_category_intent_scores")

    engagement = intent.groupBy("customer_id").agg(
        F.sum("intent_score").alias("total_intent_score"),
        F.max("intent_score").alias("top_category_score"),
    )

    enriched = customers.join(engagement, on="customer_id", how="left")

    return enriched.withColumn(
        "segment",
        F.when(
            (F.col("days_since_last_purchase") > 30) & (F.col("lifetime_value") > 3000),
            "Win-Back",
        )
        .when(
            F.col("loyalty_tier").isin("Platinum", "Gold") & (F.col("days_since_last_purchase") <= 14),
            "VIP Active",
        )
        .when(
            (F.col("loyalty_tier") == "Silver") & (F.col("total_intent_score") > 15),
            "Rising Star",
        )
        .when(F.col("churn_risk") == "High", "At Risk")
        .when(
            (F.col("days_since_last_purchase").isNull()) | (F.col("total_orders") <= 3),
            "New & Engaged",
        )
        .otherwise("Steady"),
    ).select(
        "customer_id",
        "first_name",
        "last_name",
        "loyalty_tier",
        "loyalty_points",
        "lifetime_value",
        "total_orders",
        "days_since_last_purchase",
        "churn_risk",
        "total_intent_score",
        "segment",
        "preferred_categories",
        "city",
        "state",
    )
