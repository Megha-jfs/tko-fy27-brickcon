"""
Generate mock retail datasets for the Hyper-Personalized Loyalty App demo.

Outputs (CSV):
  - products_catalog.csv      (~200 products)
  - customer_profiles.csv     (~500 customers)
  - purchase_history.csv      (~3,000 transactions)
  - clickstream_events.csv    (~5,000 browsing events)

Upload these to a Unity Catalog Volume:
  /Volumes/<catalog>/<schema>/raw_data/
"""

import csv
import json
import random
import uuid
import os
from datetime import datetime, timedelta

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------
CATEGORIES = {
    "Denim": ["Slim Jeans", "Bootcut Jeans", "Denim Jacket", "Denim Shorts", "Denim Skirt", "Wide-Leg Jeans"],
    "Tops": ["Graphic Tee", "Henley", "Oxford Shirt", "Polo", "Crop Top", "Blouse", "Tank Top"],
    "Shoes": ["Running Sneakers", "Loafers", "Ankle Boots", "Sandals", "High-Top Sneakers", "Slip-Ons"],
    "Accessories": ["Leather Belt", "Crossbody Bag", "Sunglasses", "Watch", "Scarf", "Beanie"],
    "Outerwear": ["Puffer Jacket", "Trench Coat", "Bomber Jacket", "Windbreaker", "Fleece Hoodie"],
    "Activewear": ["Yoga Pants", "Sports Bra", "Track Jacket", "Compression Shorts", "Running Tank"],
    "Dresses": ["Maxi Dress", "Cocktail Dress", "Wrap Dress", "Shirt Dress", "Sundress"],
    "Swimwear": ["Bikini Set", "One-Piece Swimsuit", "Board Shorts", "Rash Guard", "Cover-Up"],
}

BRANDS = [
    "UrbanEdge", "CoastalThreads", "PeakForm", "NovaStitch", "VerdantCo",
    "IronLoom", "SoleCraft", "AuraWear", "PrismLine", "TerraKnit",
]

COLORS = ["Black", "White", "Navy", "Olive", "Burgundy", "Charcoal", "Sand", "Rust", "Teal", "Blush"]

STYLE_TAGS = [
    "casual", "streetwear", "athleisure", "minimalist", "boho", "classic",
    "edgy", "preppy", "retro", "eco-friendly", "luxury", "resort",
]

FIRST_NAMES = [
    "Olivia", "Liam", "Emma", "Noah", "Ava", "Sophia", "Jackson", "Isabella",
    "Aiden", "Mia", "Lucas", "Harper", "Ethan", "Amelia", "Mason", "Ella",
    "Logan", "Chloe", "James", "Aria", "Benjamin", "Luna", "Henry", "Zoe",
    "Alexander", "Nora", "Sebastian", "Lily", "Daniel", "Riley", "Mateo",
    "Layla", "Jack", "Penelope", "Owen", "Camila", "Elijah", "Hazel",
    "Leo", "Scarlett", "Kai", "Priya", "Ravi", "Mei", "Tariq", "Fatima",
    "Yuki", "Andre", "Sofia", "Diego",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
    "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts",
]

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("Austin", "TX"), ("Seattle", "WA"), ("Denver", "CO"),
    ("Portland", "OR"), ("Nashville", "TN"), ("Atlanta", "GA"),
    ("Miami", "FL"), ("Boston", "MA"), ("San Francisco", "CA"),
    ("Minneapolis", "MN"), ("Charlotte", "NC"),
]

DEVICES = ["mobile", "desktop", "tablet"]
CHANNELS = ["online", "in-store", "mobile_app"]
PAYMENT_METHODS = ["credit_card", "debit_card", "apple_pay", "paypal", "gift_card"]
REFERRERS = ["google", "instagram", "tiktok", "direct", "email_campaign", "facebook", "affiliate"]

EVENT_TYPES = [
    "page_view", "product_view", "product_view", "product_view",
    "category_browse", "category_browse",
    "search", "add_to_cart", "wishlist_add", "remove_from_cart",
]

SEARCH_TERMS = [
    "black jeans", "summer dress", "running shoes", "leather jacket",
    "yoga pants", "denim jacket", "white sneakers", "crossbody bag",
    "workout gear", "casual tee", "beach outfit", "date night dress",
    "hiking boots", "cozy hoodie", "linen pants", "boho skirt",
    "sustainable fashion", "vintage denim", "athleisure set", "resort wear",
]


def _ts(start: datetime, end: datetime) -> str:
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    return (start + timedelta(seconds=rand_seconds)).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 1. Products Catalog
# ---------------------------------------------------------------------------
def generate_products(n: int = 200) -> list[dict]:
    products = []
    pid = 1000
    for _ in range(n):
        cat = random.choice(list(CATEGORIES.keys()))
        subcat = random.choice(CATEGORIES[cat])
        brand = random.choice(BRANDS)
        color = random.choice(COLORS)
        tags = random.sample(STYLE_TAGS, k=random.randint(2, 4))
        price = round(random.uniform(19.99, 299.99), 2)
        rating = round(random.uniform(3.0, 5.0), 1)

        products.append({
            "product_id": f"PROD-{pid}",
            "product_name": f"{brand} {subcat} - {color}",
            "category": cat,
            "subcategory": subcat,
            "brand": brand,
            "price": price,
            "description": f"A {random.choice(['trendy','classic','versatile','premium','eco-friendly'])} {subcat.lower()} by {brand}. Perfect for {random.choice(['everyday wear','special occasions','active lifestyles','weekend getaways'])}.",
            "style_tags": json.dumps(tags),
            "color": color,
            "sizes_available": json.dumps(random.sample(["XS", "S", "M", "L", "XL", "XXL"], k=random.randint(3, 6))),
            "rating": rating,
            "review_count": random.randint(5, 800),
            "created_at": _ts(datetime(2024, 1, 1), datetime(2026, 3, 1)),
            "is_new_arrival": random.random() < 0.25,
        })
        pid += 1
    return products


# ---------------------------------------------------------------------------
# 2. Customer Profiles
# ---------------------------------------------------------------------------
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
TIER_WEIGHTS = [0.40, 0.30, 0.20, 0.10]


def generate_customers(n: int = 500) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        tier = random.choices(TIERS, weights=TIER_WEIGHTS, k=1)[0]
        signup = _ts(datetime(2020, 1, 1), datetime(2025, 12, 31))
        city, state = random.choice(CITIES)
        pref_cats = random.sample(list(CATEGORIES.keys()), k=random.randint(2, 4))
        pref_brands = random.sample(BRANDS, k=random.randint(1, 3))
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        points_range = {"Bronze": (0, 2000), "Silver": (2001, 8000), "Gold": (8001, 20000), "Platinum": (20001, 50000)}
        lo, hi = points_range[tier]

        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "age": random.randint(18, 65),
            "gender": random.choice(["M", "F", "Non-binary", "Prefer not to say"]),
            "loyalty_tier": tier,
            "loyalty_points": random.randint(lo, hi),
            "signup_date": signup,
            "preferred_categories": json.dumps(pref_cats),
            "preferred_brands": json.dumps(pref_brands),
            "city": city,
            "state": state,
            "lifetime_value": round(random.uniform(50, 15000), 2),
            "total_orders": random.randint(1, 120),
            "last_purchase_date": _ts(datetime(2025, 6, 1), datetime(2026, 3, 1)),
            "credit_card_last4": f"{''.join([str(random.randint(0,9)) for _ in range(4)])}",
            "phone_masked": f"(***) ***-{random.randint(1000, 9999)}",
        })
    return customers


# ---------------------------------------------------------------------------
# 3. Purchase History
# ---------------------------------------------------------------------------
def generate_purchases(customers: list[dict], products: list[dict], n: int = 3000) -> list[dict]:
    purchases = []
    product_ids = [p["product_id"] for p in products]
    product_prices = {p["product_id"]: p["price"] for p in products}
    customer_ids = [c["customer_id"] for c in customers]

    for i in range(1, n + 1):
        cid = random.choice(customer_ids)
        pid = random.choice(product_ids)
        qty = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1], k=1)[0]
        unit_price = product_prices[pid]

        purchases.append({
            "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": cid,
            "product_id": pid,
            "purchase_date": _ts(datetime(2024, 1, 1), datetime(2026, 3, 1)),
            "quantity": qty,
            "unit_price": unit_price,
            "total_amount": round(unit_price * qty, 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "channel": random.choice(CHANNELS),
        })
    return purchases


# ---------------------------------------------------------------------------
# 4. Clickstream Events
# ---------------------------------------------------------------------------
def generate_clickstream(customers: list[dict], products: list[dict], n: int = 5000) -> list[dict]:
    events = []
    product_ids = [p["product_id"] for p in products]
    product_cats = {p["product_id"]: p["category"] for p in products}
    customer_ids = [c["customer_id"] for c in customers]
    categories = list(CATEGORIES.keys())

    # Generate sessions — each customer may have multiple sessions
    sessions = {}
    for cid in customer_ids:
        num_sessions = random.randint(1, 5)
        for _ in range(num_sessions):
            sid = f"SESS-{uuid.uuid4().hex[:10].upper()}"
            sessions[sid] = cid

    session_list = list(sessions.items())

    now = datetime(2026, 3, 5, 12, 0, 0)
    window_start = now - timedelta(hours=72)  # last 72 hours of activity

    for i in range(1, n + 1):
        sid, cid = random.choice(session_list)
        event_type = random.choice(EVENT_TYPES)
        ts = _ts(window_start, now)
        pid = None
        cat_browsed = None
        search_term = None

        if event_type in ("product_view", "add_to_cart", "wishlist_add", "remove_from_cart"):
            pid = random.choice(product_ids)
            cat_browsed = product_cats[pid]
        elif event_type == "category_browse":
            cat_browsed = random.choice(categories)
        elif event_type == "search":
            search_term = random.choice(SEARCH_TERMS)

        page = "/"
        if event_type == "product_view" and pid:
            page = f"/product/{pid}"
        elif event_type == "category_browse" and cat_browsed:
            page = f"/category/{cat_browsed.lower()}"
        elif event_type == "search":
            page = f"/search?q={search_term.replace(' ', '+')}" if search_term else "/search"
        elif event_type == "page_view":
            page = random.choice(["/", "/new-arrivals", "/sale", "/loyalty", "/account"])

        events.append({
            "event_id": f"EVT-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": cid,
            "session_id": sid,
            "event_timestamp": ts,
            "event_type": event_type,
            "page_url": page,
            "product_id": pid or "",
            "category_browsed": cat_browsed or "",
            "search_term": search_term or "",
            "device_type": random.choice(DEVICES),
            "referrer": random.choice(REFERRERS),
            "time_on_page_seconds": random.randint(3, 300),
        })
    return events


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------
def write_csv(data: list[dict], filename: str):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✓ {filename}: {len(data)} rows → {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Generating mock retail datasets...\n")

    products = generate_products(200)
    write_csv(products, "products_catalog.csv")

    customers = generate_customers(500)
    write_csv(customers, "customer_profiles.csv")

    purchases = generate_purchases(customers, products, 3000)
    write_csv(purchases, "purchase_history.csv")

    clickstream = generate_clickstream(customers, products, 5000)
    write_csv(clickstream, "clickstream_events.csv")

    print(f"\nAll files written to: {OUTPUT_DIR}/")
    print("\nNext step: Upload these CSVs to a Unity Catalog Volume:")
    print("  /Volumes/<catalog>/<schema>/raw_data/")
