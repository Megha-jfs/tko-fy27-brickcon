"""
Hyper-Personalized Loyalty App — Customer Portal
Databricks App serving real-time personalized offers and loyalty status.

This app simulates a customer-facing portal that shows:
  - Real-time loyalty dashboard (tier, points, progress)
  - Personalized product recommendations
  - Active offers based on browsing behavior
  - Style Assistant chat powered by a Databricks AI agent
"""

import streamlit as st
import os
import json
import time
import re
import random
import pandas as pd
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(
    layout="wide",
    page_title="ShopSmart — Your Personalized Portal",
    page_icon="🛍️",
)

# ===================== CUSTOM CSS =====================
st.markdown("""
<style>
    .main .block-container {
        max-width: 100%;
        padding: 1.5rem 2rem 2rem 2rem;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #fafbfc 0%, #f0f2f6 100%);
    }

    .hero-banner {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #a855f7 100%);
        color: white;
        padding: 1.5rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 30px rgba(99, 102, 241, 0.3);
    }
    .hero-banner h2 { margin: 0 0 0.3rem 0; font-size: 1.5rem; font-weight: 800; }
    .hero-banner p { margin: 0; opacity: 0.92; font-size: 0.95rem; }

    .metric-card {
        background: white;
        border-radius: 14px;
        padding: 1.25rem 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        border: 1px solid #f0f0f5;
        text-align: center;
        transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.1); }
    .metric-card .label { font-size: 0.78rem; color: #6b7280; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
    .metric-card .value { font-size: 1.7rem; font-weight: 800; color: #1f2937; margin: 0.25rem 0; }
    .metric-card .sub { font-size: 0.8rem; color: #9ca3af; }

    .tier-badge {
        display: inline-block;
        padding: 0.3rem 1rem;
        border-radius: 20px;
        font-weight: 700;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }
    .tier-bronze { background: linear-gradient(135deg, #cd7f32, #daa06d); color: white; }
    .tier-silver { background: linear-gradient(135deg, #8e9aaf, #bcc5d3); color: white; }
    .tier-gold { background: linear-gradient(135deg, #f59e0b, #fbbf24); color: #78350f; }
    .tier-platinum { background: linear-gradient(135deg, #6366f1, #8b5cf6); color: white; }

    .offer-card {
        background: white;
        border-radius: 14px;
        padding: 1.25rem;
        border-left: 4px solid #6366f1;
        box-shadow: 0 2px 12px rgba(0,0,0,0.05);
        margin-bottom: 0.75rem;
    }
    .offer-card .offer-type { font-size: 0.72rem; color: #6366f1; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }
    .offer-card .offer-value { font-size: 1.3rem; font-weight: 800; color: #1f2937; margin: 0.2rem 0; }
    .offer-card .offer-product { font-size: 0.88rem; color: #4b5563; }
    .offer-card .offer-reason { font-size: 0.78rem; color: #9ca3af; margin-top: 0.4rem; font-style: italic; }
    .offer-card .offer-expires { font-size: 0.72rem; color: #ef4444; margin-top: 0.3rem; }

    .product-card {
        background: white;
        border-radius: 14px;
        padding: 1.1rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.05);
        border: 1px solid #f0f0f5;
        text-align: center;
        height: 100%;
    }
    .product-card .product-cat { font-size: 0.7rem; color: #8b5cf6; font-weight: 600; text-transform: uppercase; }
    .product-card .product-name { font-size: 0.88rem; font-weight: 700; color: #1f2937; margin: 0.3rem 0; line-height: 1.3; }
    .product-card .product-price { font-size: 1.15rem; font-weight: 800; color: #6366f1; }
    .product-card .product-rating { font-size: 0.78rem; color: #f59e0b; margin-top: 0.2rem; }
    .product-card .match-score { display: inline-block; background: #ecfdf5; color: #059669; font-size: 0.72rem; font-weight: 700; padding: 0.15rem 0.6rem; border-radius: 10px; margin-top: 0.3rem; }

    .section-header {
        font-size: 1.15rem;
        font-weight: 700;
        color: #1f2937;
        margin: 1.5rem 0 0.75rem 0;
        padding-bottom: 0.4rem;
        border-bottom: 2px solid #e5e7eb;
    }

    .activity-item {
        display: flex;
        align-items: center;
        padding: 0.5rem 0;
        border-bottom: 1px solid #f3f4f6;
        font-size: 0.85rem;
    }
    .activity-item .time { color: #9ca3af; font-size: 0.75rem; min-width: 60px; }
    .activity-item .action { color: #4b5563; flex: 1; }

    .progress-bar-container {
        background: #e5e7eb;
        border-radius: 10px;
        height: 10px;
        overflow: hidden;
        margin: 0.3rem 0;
    }
    .progress-bar-fill {
        height: 100%;
        border-radius: 10px;
        background: linear-gradient(90deg, #6366f1, #a855f7);
        transition: width 0.5s ease;
    }

    .app-footer {
        text-align: center;
        padding: 1rem 0 0.5rem 0;
        color: #9ca3af;
        font-size: 0.75rem;
        border-top: 1px solid #e5e7eb;
        margin-top: 2rem;
    }
    .app-footer span { color: #6366f1; font-weight: 600; }

    [data-testid="stSidebar"] [data-testid="stForm"] {
        position: sticky;
        bottom: 0;
        background: linear-gradient(180deg, #f0f2f6, #eef1f5);
        padding: 0.6rem 1rem 0.4rem 1rem;
        border: none;
        border-top: 1px solid #e2e8f0;
        z-index: 10;
    }

    .chat-header {
        text-align: center;
        padding: 0.5rem 0 0.25rem 0;
    }
    .chat-header h3 { margin: 0; font-size: 1.05rem; font-weight: 700; color: #1f2937; }
    .chat-header .subtitle { font-size: 0.75rem; color: #6b7280; }
</style>
""", unsafe_allow_html=True)


# ===================== MOCK DATA (simulates Lakebase reads) =====================
# In production, these would be real-time queries to Lakebase (Serverless Postgres)
# and Databricks serving endpoints. For the demo, we use in-memory mock data
# that matches the schema from our pipeline and Lakebase tables.

MOCK_CUSTOMERS = {
    "CUST-0042": {
        "customer_id": "CUST-0042", "first_name": "Sophia", "last_name": "Martinez",
        "loyalty_tier": "Gold", "loyalty_points": 12450, "lifetime_value": 4820.50,
        "segment": "VIP Active", "churn_risk": "Low", "total_orders": 47,
        "preferred_categories": ["Denim", "Shoes", "Accessories"],
        "city": "Austin", "state": "TX",
        "days_since_last_purchase": 5, "email": "sophia.martinez42@example.com",
    },
    "CUST-0107": {
        "customer_id": "CUST-0107", "first_name": "Liam", "last_name": "Nguyen",
        "loyalty_tier": "Platinum", "loyalty_points": 34200, "lifetime_value": 12350.00,
        "segment": "VIP Active", "churn_risk": "Low", "total_orders": 89,
        "preferred_categories": ["Outerwear", "Activewear", "Tops"],
        "city": "Seattle", "state": "WA",
        "days_since_last_purchase": 2, "email": "liam.nguyen107@example.com",
    },
    "CUST-0215": {
        "customer_id": "CUST-0215", "first_name": "Emma", "last_name": "Johnson",
        "loyalty_tier": "Silver", "loyalty_points": 5600, "lifetime_value": 1890.25,
        "segment": "Rising Star", "churn_risk": "Low", "total_orders": 18,
        "preferred_categories": ["Dresses", "Accessories", "Shoes"],
        "city": "Nashville", "state": "TN",
        "days_since_last_purchase": 12, "email": "emma.johnson215@example.com",
    },
    "CUST-0331": {
        "customer_id": "CUST-0331", "first_name": "Mateo", "last_name": "Garcia",
        "loyalty_tier": "Gold", "loyalty_points": 9800, "lifetime_value": 6200.75,
        "segment": "Win-Back", "churn_risk": "Medium", "total_orders": 35,
        "preferred_categories": ["Denim", "Outerwear", "Shoes"],
        "city": "Denver", "state": "CO",
        "days_since_last_purchase": 38, "email": "mateo.garcia331@example.com",
    },
    "CUST-0489": {
        "customer_id": "CUST-0489", "first_name": "Priya", "last_name": "Patel",
        "loyalty_tier": "Bronze", "loyalty_points": 1200, "lifetime_value": 450.00,
        "segment": "New & Engaged", "churn_risk": "Low", "total_orders": 4,
        "preferred_categories": ["Activewear", "Tops", "Swimwear"],
        "city": "Miami", "state": "FL",
        "days_since_last_purchase": 8, "email": "priya.patel489@example.com",
    },
}

TIER_THRESHOLDS = {"Bronze": 2000, "Silver": 8000, "Gold": 20000, "Platinum": 50000}
TIER_COLORS = {"Bronze": "tier-bronze", "Silver": "tier-silver", "Gold": "tier-gold", "Platinum": "tier-platinum"}
TIER_ORDER = ["Bronze", "Silver", "Gold", "Platinum"]


def get_next_tier(current_tier: str) -> tuple[str, int]:
    idx = TIER_ORDER.index(current_tier)
    if idx >= len(TIER_ORDER) - 1:
        return "Platinum", TIER_THRESHOLDS["Platinum"]
    next_t = TIER_ORDER[idx + 1]
    return next_t, TIER_THRESHOLDS[next_t]


def get_mock_offers(customer: dict) -> list[dict]:
    """Simulate personalized offers from Lakebase personalized_offers table."""
    categories = customer["preferred_categories"]
    offers = []
    offer_templates = [
        {"offer_type": "discount", "offer_value": "20% Off", "border": "#6366f1"},
        {"offer_type": "bonus_points", "offer_value": "500 Bonus Points", "border": "#f59e0b"},
        {"offer_type": "early_access", "offer_value": "Early Access", "border": "#10b981"},
        {"offer_type": "bundle", "offer_value": "Buy 2, Save 30%", "border": "#ef4444"},
    ]
    for i, cat in enumerate(categories[:3]):
        tmpl = offer_templates[i % len(offer_templates)]
        offers.append({
            "offer_code": f"{cat.upper()[:3]}-{tmpl['offer_type'].upper()[:4]}-{customer['customer_id'][-4:]}",
            "category": cat,
            "product_name": f"Trending {cat} Collection",
            "relevance_score": round(random.uniform(0.82, 0.98), 2),
            "offer_type": tmpl["offer_type"],
            "offer_value": tmpl["offer_value"],
            "reason": f"Based on your recent browsing in {cat} and purchase history",
            "expires_in_hours": random.choice([6, 12, 24, 48]),
        })
    if customer.get("segment") == "Win-Back":
        offers.insert(0, {
            "offer_code": f"WINBACK-{customer['customer_id'][-4:]}",
            "category": "All Categories",
            "product_name": "We Miss You!",
            "relevance_score": 0.99,
            "offer_type": "discount",
            "offer_value": "25% Off Everything",
            "reason": f"It's been {customer['days_since_last_purchase']} days — welcome back with this exclusive offer",
            "expires_in_hours": 72,
        })
    return offers


def get_mock_recommendations(customer: dict) -> list[dict]:
    """Simulate recommendation_scores from Lakebase."""
    brands = ["UrbanEdge", "CoastalThreads", "PeakForm", "NovaStitch", "VerdantCo",
              "IronLoom", "SoleCraft", "AuraWear", "PrismLine", "TerraKnit"]
    colors = ["Black", "White", "Navy", "Olive", "Burgundy", "Charcoal", "Sand"]
    subcats = {
        "Denim": ["Slim Jeans", "Bootcut Jeans", "Denim Jacket", "Wide-Leg Jeans"],
        "Tops": ["Graphic Tee", "Henley", "Oxford Shirt", "Crop Top"],
        "Shoes": ["Running Sneakers", "Ankle Boots", "High-Top Sneakers", "Loafers"],
        "Accessories": ["Leather Belt", "Crossbody Bag", "Sunglasses", "Watch"],
        "Outerwear": ["Puffer Jacket", "Bomber Jacket", "Trench Coat", "Fleece Hoodie"],
        "Activewear": ["Yoga Pants", "Sports Bra", "Track Jacket", "Running Tank"],
        "Dresses": ["Maxi Dress", "Wrap Dress", "Cocktail Dress", "Sundress"],
        "Swimwear": ["Bikini Set", "One-Piece Swimsuit", "Board Shorts", "Rash Guard"],
    }
    recs = []
    for cat in customer["preferred_categories"]:
        items = subcats.get(cat, ["Item"])
        for _ in range(3):
            brand = random.choice(brands)
            subcat = random.choice(items)
            color = random.choice(colors)
            recs.append({
                "product_name": f"{brand} {subcat} — {color}",
                "category": cat,
                "price": round(random.uniform(29.99, 249.99), 2),
                "rating": round(random.uniform(3.8, 5.0), 1),
                "match_score": round(random.uniform(0.80, 0.99), 2),
                "is_new_arrival": random.random() < 0.4,
            })
    return sorted(recs, key=lambda x: x["match_score"], reverse=True)


def get_mock_session_activity(customer: dict) -> list[dict]:
    """Simulate active_sessions data from Lakebase."""
    actions = [
        ("2 min ago", "Viewed **Denim** category page", "category_browse"),
        ("5 min ago", "Viewed **UrbanEdge Slim Jeans — Black**", "product_view"),
        ("8 min ago", "Searched for *\"black jeans\"*", "search"),
        ("12 min ago", "Added **CoastalThreads Ankle Boots** to wishlist", "wishlist_add"),
        ("15 min ago", "Viewed **Shoes** category page", "category_browse"),
        ("22 min ago", "Viewed **NovaStitch Running Sneakers — White**", "product_view"),
        ("30 min ago", "Browsed **New Arrivals** page", "page_view"),
    ]
    return [{"time": a[0], "action": a[1], "type": a[2]} for a in actions]


def get_mock_intent_scores(customer: dict) -> list[dict]:
    """Simulate gold_category_intent_scores data."""
    categories = customer["preferred_categories"] + ["Tops", "Activewear"]
    seen = set()
    scores = []
    for i, cat in enumerate(categories):
        if cat in seen:
            continue
        seen.add(cat)
        scores.append({
            "category": cat,
            "intent_score": round(random.uniform(2.0, 18.0), 1) if i < 3 else round(random.uniform(0.5, 4.0), 1),
            "event_count": random.randint(2, 25) if i < 3 else random.randint(1, 5),
        })
    return sorted(scores, key=lambda x: x["intent_score"], reverse=True)


# ===================== AI STYLE ASSISTANT =====================

def get_openai_client():
    """Create OpenAI client for Databricks serving endpoint (Style Assistant agent)."""
    try:
        from openai import OpenAI
        import requests

        host = os.environ.get("DATABRICKS_HOST", "")
        client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

        if not all([host, client_id, client_secret]):
            return None

        if not host.startswith("http"):
            host = f"https://{host}"

        token_resp = requests.post(
            f"{host}/oidc/v1/token",
            data={"grant_type": "client_credentials", "scope": "all-apis"},
            auth=(client_id, client_secret),
            timeout=10,
        )
        if not token_resp.ok:
            return None

        access_token = token_resp.json().get("access_token")
        if not access_token:
            return None

        return OpenAI(api_key=access_token, base_url=f"{host}/serving-endpoints")
    except Exception:
        return None


def query_style_assistant(user_message: str, customer_context: dict) -> tuple[str, float]:
    """
    Query the AI Style Assistant with customer context.
    Falls back to a template response if the endpoint isn't available.
    """
    start = time.time()

    context = (
        f"You are a personal style assistant for {customer_context['first_name']}. "
        f"They are a {customer_context['loyalty_tier']} tier member who loves "
        f"{', '.join(customer_context['preferred_categories'][:3])}. "
        f"They have {customer_context['loyalty_points']} points and live in "
        f"{customer_context['city']}, {customer_context['state']}. "
        f"Provide personalized, enthusiastic fashion advice. Keep responses concise (2-3 paragraphs max)."
    )

    client = get_openai_client()
    if client:
        try:
            response = client.chat.completions.create(
                model=os.environ.get("STYLE_AGENT_ENDPOINT", "databricks-claude-sonnet-4"),
                messages=[
                    {"role": "system", "content": context},
                    {"role": "user", "content": user_message},
                ],
                max_tokens=500,
            )
            elapsed = time.time() - start
            return response.choices[0].message.content, elapsed
        except Exception:
            pass

    # Fallback: template response for demo when endpoint isn't configured
    elapsed = time.time() - start
    name = customer_context["first_name"]
    cats = customer_context["preferred_categories"]
    fallback = (
        f"Great question, {name}! Based on your style profile and recent browsing, "
        f"I'd recommend checking out our new {cats[0]} collection — we just got some "
        f"amazing pieces that match your aesthetic. \n\n"
        f"Since you're a **{customer_context['loyalty_tier']}** member, you have early access "
        f"to our spring arrivals. I noticed you've been looking at {cats[1] if len(cats) > 1 else cats[0]} "
        f"lately — try pairing those with items from our {cats[0]} line for a fresh look! "
        f"You have **{customer_context['loyalty_points']:,} points** you could use for an extra discount."
    )
    return fallback, elapsed + 0.3


# ===================== MAIN APP =====================

# Session state
if "selected_customer" not in st.session_state:
    st.session_state.selected_customer = "CUST-0042"
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Sidebar: Customer selector + Style Assistant
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 0.5rem 0;">
        <span style="font-size: 2rem;">🛍️</span>
        <h3 style="margin: 0.2rem 0 0 0; font-size: 1.1rem; font-weight: 800; color: #1f2937;">ShopSmart</h3>
        <p style="margin: 0; font-size: 0.75rem; color: #6b7280;">Personalized Loyalty Portal</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")

    customer_options = {
        f"{c['first_name']} {c['last_name']} ({cid})": cid
        for cid, c in MOCK_CUSTOMERS.items()
    }
    selected_label = st.selectbox(
        "Select Customer",
        options=list(customer_options.keys()),
        index=list(customer_options.values()).index(st.session_state.selected_customer),
    )
    new_cid = customer_options[selected_label]
    if new_cid != st.session_state.selected_customer:
        st.session_state.selected_customer = new_cid
        st.session_state.chat_history = []
        st.rerun()

    customer = MOCK_CUSTOMERS[st.session_state.selected_customer]

    st.markdown("---")

    # Style Assistant Chat
    @st.fragment
    def style_chat():
        st.markdown("""
        <div class="chat-header">
            <h3>✨ Style Assistant</h3>
            <p class="subtitle">AI-powered personal shopper</p>
        </div>
        """, unsafe_allow_html=True)

        SAMPLE_QUESTIONS = [
            "What should I wear this weekend?",
            "Suggest outfits for a casual brunch",
            "What's trending in my favorite categories?",
        ]

        clicked_q = None
        for i, q in enumerate(SAMPLE_QUESTIONS):
            if st.button(q, key=f"sq_{i}", use_container_width=True):
                clicked_q = q

        st.markdown("---")

        if not st.session_state.chat_history:
            st.markdown("""
            <div style="text-align:center; padding: 1.5rem 0.5rem; color: #9ca3af;">
                <div style="font-size: 1.5rem; margin-bottom: 0.3rem;">💬</div>
                <p style="font-size: 0.8rem; margin: 0;">Ask me for style advice, outfit ideas, or product recommendations!</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            for msg in st.session_state.chat_history:
                with st.chat_message("user"):
                    st.write(msg["user"])
                with st.chat_message("assistant"):
                    st.markdown(msg["agent"])
                    st.caption(f"⏱️ {msg['time']:.2f}s")

        processing_area = st.container()

        with st.form("chat_form", clear_on_submit=True):
            user_input = st.text_input(
                "Ask your Style Assistant...",
                placeholder=f"Hi {customer['first_name']}, ask me anything!",
                label_visibility="collapsed",
            )
            c1, c2 = st.columns([3, 1])
            with c1:
                submitted = st.form_submit_button("Send", use_container_width=True, type="primary")
            with c2:
                cleared = st.form_submit_button("🗑️", use_container_width=True)

        if cleared:
            st.session_state.chat_history = []
            st.rerun(scope="fragment")

        query = None
        if submitted and user_input:
            query = user_input
        elif clicked_q:
            query = clicked_q

        if query:
            with processing_area:
                with st.chat_message("user"):
                    st.write(query)
                with st.chat_message("assistant"):
                    with st.spinner("Thinking..."):
                        reply, elapsed = query_style_assistant(query, customer)
                    st.markdown(reply)
                    st.caption(f"⏱️ {elapsed:.2f}s")

            st.session_state.chat_history.append({"user": query, "agent": reply, "time": elapsed})
            st.rerun(scope="fragment")

    style_chat()


# ===================== MAIN CONTENT AREA =====================

customer = MOCK_CUSTOMERS[st.session_state.selected_customer]

# Hero Banner
st.markdown(f"""
<div class="hero-banner">
    <h2>Welcome back, {customer['first_name']}! 👋</h2>
    <p>Your personalized shopping experience — powered by real-time AI recommendations</p>
</div>
""", unsafe_allow_html=True)

# Loyalty Dashboard — Metric Cards
tier = customer["loyalty_tier"]
points = customer["loyalty_points"]
next_tier, next_threshold = get_next_tier(tier)
current_threshold = TIER_THRESHOLDS.get(tier, 0)
progress = min(100, int((points / next_threshold) * 100)) if tier != "Platinum" else 100
points_to_next = max(0, next_threshold - points)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Loyalty Tier</div>
        <div style="margin: 0.4rem 0;"><span class="tier-badge {TIER_COLORS[tier]}">{tier}</span></div>
        <div class="sub">{customer['segment']}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Points Balance</div>
        <div class="value">{points:,}</div>
        <div class="sub">Worth ${points * 0.01:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    if tier != "Platinum":
        st.markdown(f"""
        <div class="metric-card">
            <div class="label">Next Tier: {next_tier}</div>
            <div class="value">{points_to_next:,}</div>
            <div class="sub">points to go</div>
            <div class="progress-bar-container"><div class="progress-bar-fill" style="width: {progress}%"></div></div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="metric-card">
            <div class="label">Tier Status</div>
            <div class="value">MAX</div>
            <div class="sub">You've reached the top!</div>
        </div>
        """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Lifetime Value</div>
        <div class="value">${customer['lifetime_value']:,.0f}</div>
        <div class="sub">{customer['total_orders']} orders</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    risk_color = {"Low": "#10b981", "Medium": "#f59e0b", "High": "#ef4444"}.get(customer["churn_risk"], "#6b7280")
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">Last Purchase</div>
        <div class="value">{customer['days_since_last_purchase']}d</div>
        <div class="sub" style="color: {risk_color}; font-weight: 600;">Risk: {customer['churn_risk']}</div>
    </div>
    """, unsafe_allow_html=True)

# Two-column layout: Offers + Recommendations
left_col, right_col = st.columns([2, 3])

with left_col:
    # Active Offers
    st.markdown('<div class="section-header">🎯 Your Personalized Offers</div>', unsafe_allow_html=True)
    offers = get_mock_offers(customer)
    for offer in offers:
        st.markdown(f"""
        <div class="offer-card">
            <div class="offer-type">{offer['offer_type'].replace('_', ' ')}</div>
            <div class="offer-value">{offer['offer_value']}</div>
            <div class="offer-product">{offer['product_name']} — {offer['category']}</div>
            <div class="offer-reason">💡 {offer['reason']}</div>
            <div class="offer-expires">⏰ Expires in {offer['expires_in_hours']}h · Match: {offer['relevance_score']:.0%}</div>
        </div>
        """, unsafe_allow_html=True)

    # Current Intent Scores
    st.markdown('<div class="section-header">📊 Your Interest Profile (Live)</div>', unsafe_allow_html=True)
    intent_scores = get_mock_intent_scores(customer)
    intent_df = pd.DataFrame(intent_scores)
    st.bar_chart(intent_df.set_index("category")["intent_score"], color="#6366f1", horizontal=True)

with right_col:
    # Product Recommendations
    st.markdown('<div class="section-header">✨ Recommended For You</div>', unsafe_allow_html=True)
    recs = get_mock_recommendations(customer)

    rec_cols = st.columns(3)
    for i, rec in enumerate(recs[:9]):
        with rec_cols[i % 3]:
            stars = "⭐" * int(rec["rating"])
            new_badge = "🆕 " if rec.get("is_new_arrival") else ""
            st.markdown(f"""
            <div class="product-card">
                <div class="product-cat">{new_badge}{rec['category']}</div>
                <div class="product-name">{rec['product_name']}</div>
                <div class="product-price">${rec['price']:.2f}</div>
                <div class="product-rating">{stars} {rec['rating']}</div>
                <div class="match-score">{rec['match_score']:.0%} match</div>
            </div>
            <br>
            """, unsafe_allow_html=True)

# Session Activity Feed
st.markdown('<div class="section-header">🔴 Live Session Activity</div>', unsafe_allow_html=True)
activity = get_mock_session_activity(customer)
act_cols = st.columns(len(activity))
for i, act in enumerate(activity):
    emoji = {"product_view": "👁️", "category_browse": "📂", "search": "🔍", "wishlist_add": "❤️", "page_view": "📄"}.get(act["type"], "•")
    with act_cols[i]:
        st.markdown(f"""
        <div style="text-align:center; padding: 0.5rem; background: white; border-radius: 10px; box-shadow: 0 1px 6px rgba(0,0,0,0.05);">
            <div style="font-size: 1.2rem;">{emoji}</div>
            <div style="font-size: 0.72rem; color: #4b5563; margin-top: 0.2rem;">{act['action']}</div>
            <div style="font-size: 0.68rem; color: #9ca3af; margin-top: 0.15rem;">{act['time']}</div>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("""
<div class="app-footer">
    Powered by <span>Databricks</span> · Unity Catalog · Lakebase · Lakeflow · AI/ML
</div>
""", unsafe_allow_html=True)
