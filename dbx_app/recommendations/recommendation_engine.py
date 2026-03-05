"""
Recommendation Engine — Databricks Connect + Vector Search

This module uses Databricks Connect to run Spark queries for feature engineering
against simulated data, and uses Vector Search to find products that match
a shopper's "vibe" based on their recent browsing and preferences.

Usage (from Cursor with Databricks Connect configured):
    from recommendations.recommendation_engine import get_style_recommendations
    recs = get_style_recommendations("CUST-0042")
"""

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

CATALOG = "tko_fy27_brickcon"
SCHEMA = "loyalty_engine"
VECTOR_SEARCH_ENDPOINT = "loyalty_vector_search"
VECTOR_INDEX_NAME = f"{CATALOG}.{SCHEMA}.product_embeddings_index"


def get_spark():
    """Initialize a Databricks Connect Spark session."""
    return DatabricksSession.builder.getOrCreate()


def get_customer_top_interests(customer_id: str, top_n: int = 3) -> list[dict]:
    """
    Pull a customer's top N category interests from their simulated profile
    and recent clickstream behavior using Spark.

    Returns a list of dicts: [{"category": "Denim", "intent_score": 12.5}, ...]
    """
    spark = get_spark()

    intent_df = spark.sql(f"""
        SELECT resolved_category AS category,
               intent_score,
               event_count,
               latest_interaction
        FROM {CATALOG}.{SCHEMA}.gold_category_intent_scores
        WHERE customer_id = '{customer_id}'
        ORDER BY intent_score DESC
        LIMIT {top_n}
    """)

    results = [row.asDict() for row in intent_df.collect()]

    if not results:
        profile_df = spark.sql(f"""
            SELECT preferred_categories
            FROM {CATALOG}.{SCHEMA}.silver_customer_golden_record
            WHERE customer_id = '{customer_id}'
        """)
        rows = profile_df.collect()
        if rows and rows[0]["preferred_categories"]:
            import json
            prefs = json.loads(rows[0]["preferred_categories"])
            results = [{"category": cat, "intent_score": 0.0, "event_count": 0} for cat in prefs[:top_n]]

    return results


def find_similar_products_by_vibe(
    search_query: str,
    num_results: int = 5,
    filters: dict | None = None,
) -> list[dict]:
    """
    Use Databricks Vector Search to find products matching the "vibe"
    of a shopper's recent search terms or style preferences.

    Args:
        search_query: Natural language query (e.g., "casual black denim for weekend")
        num_results: Number of similar products to return
        filters: Optional column filters (e.g., {"is_new_arrival": True})

    Returns:
        List of product dicts with similarity scores
    """
    w = WorkspaceClient()

    filter_conditions = filters or {}

    results = w.vector_search_indexes.query_index(
        index_name=VECTOR_INDEX_NAME,
        columns=[
            "product_id", "product_name", "category", "subcategory",
            "brand", "price", "style_tags", "color", "rating", "description",
        ],
        query_text=search_query,
        num_results=num_results,
        filters_json=str(filter_conditions) if filter_conditions else None,
    )

    products = []
    if results and results.result and results.result.data_array:
        columns = [c.name for c in results.manifest.columns]
        for row in results.result.data_array:
            product = dict(zip(columns, row))
            products.append(product)

    return products


def get_style_recommendations(
    customer_id: str,
    include_new_arrivals: bool = True,
) -> dict:
    """
    Full recommendation pipeline: get customer interests → find matching products.

    Returns:
        {
            "customer_id": "CUST-0042",
            "interests": [...],
            "recommendations": {
                "Denim": [...products...],
                "Shoes": [...products...],
            }
        }
    """
    interests = get_customer_top_interests(customer_id, top_n=3)

    recommendations = {}
    for interest in interests:
        category = interest["category"]
        query = f"{category} style trending fashion"

        filters = {"category": category}
        if include_new_arrivals:
            filters["is_new_arrival"] = "true"

        products = find_similar_products_by_vibe(
            search_query=query,
            num_results=5,
            filters=filters if include_new_arrivals else {"category": category},
        )

        if not products and include_new_arrivals:
            products = find_similar_products_by_vibe(
                search_query=query,
                num_results=5,
                filters={"category": category},
            )

        recommendations[category] = products

    return {
        "customer_id": customer_id,
        "interests": interests,
        "recommendations": recommendations,
    }


def compute_recommendation_scores(customer_id: str) -> list[dict]:
    """
    Compute and return recommendation scores for storage in Lakebase.
    These scores power the sub-second retrieval in the frontend app.
    """
    spark = get_spark()

    interests = get_customer_top_interests(customer_id, top_n=3)
    scores = []

    for interest in interests:
        category = interest["category"]
        intent_score = interest.get("intent_score", 0)

        products_df = spark.sql(f"""
            SELECT product_id, product_name, category, price, rating
            FROM {CATALOG}.{SCHEMA}.bronze_products
            WHERE category = '{category}'
            ORDER BY rating DESC, review_count DESC
            LIMIT 10
        """)

        for row in products_df.collect():
            relevance = min(1.0, (intent_score / 20.0) * (row["rating"] / 5.0))
            scores.append({
                "customer_id": customer_id,
                "product_id": row["product_id"],
                "product_name": row["product_name"],
                "score": round(relevance, 4),
                "score_type": "content_based",
                "reasoning": f"High intent ({intent_score:.1f}) in {category} + product rating {row['rating']}",
            })

    return sorted(scores, key=lambda x: x["score"], reverse=True)


if __name__ == "__main__":
    print("Testing recommendation engine for CUST-0042...\n")

    print("1. Top Interests:")
    interests = get_customer_top_interests("CUST-0042")
    for i in interests:
        print(f"   {i['category']}: intent_score={i['intent_score']}")

    print("\n2. Full Recommendations:")
    recs = get_style_recommendations("CUST-0042")
    for cat, products in recs["recommendations"].items():
        print(f"\n   [{cat}] — {len(products)} products")
        for p in products[:2]:
            print(f"     - {p.get('product_name', 'N/A')} (${p.get('price', 'N/A')})")
