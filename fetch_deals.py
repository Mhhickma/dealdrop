"""
DealDrop — fetch_deals.py
Uses the official keepa Python library for reliable API access.
"""

import json
import os
import time
import datetime

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 50
MIN_DISCOUNT_PCT   = 20
HOT_DEAL_PCT       = 50

CATEGORY_NAMES = {
    281052:      "Electronics",
    1055398:     "Home & Kitchen",
    7141123011:  "Clothing, Shoes & Jewelry",
    3760901:     "Luggage & Travel",
    3375251:     "Sports & Outdoors",
    165793011:   "Toys & Games",
    2619525011:  "Tools & Home Improvement",
    51574011:    "Pet Supplies",
    165796011:   "Baby",
    172282:      "Electronics",
    1064954:     "Health & Household",
    3760911:     "Beauty & Personal Care",
    979455011:   "Garden & Outdoor",
    1285128:     "Office Products",
    468642:      "Video Games",
    283155:      "Books",
    16310101:    "Grocery & Gourmet Food",
    9482648011:  "Kitchen & Dining",
}

CATEGORY_EMOJI = {
    "Electronics":               "💻",
    "Home & Kitchen":            "🏠",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care":    "💄",
    "Health & Household":        "💊",
    "Toys & Games":              "🧸",
    "Sports & Outdoors":         "⚽",
    "Automotive":                "🚗",
    "Pet Supplies":              "🐾",
    "Baby":                      "🍼",
    "Garden & Outdoor":          "🌱",
    "Office Products":           "📎",
    "Tools & Home Improvement":  "🔧",
    "Kitchen & Dining":          "🍳",
    "Video Games":               "🎮",
    "Books":                     "📚",
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
}

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]
    title = (product.get("title") or "").lower()
    if any(w in title for w in ["laptop","phone","tablet","camera","headphone","speaker","monitor","tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt","shoe","dress","jacket","pants","bag","watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender","vacuum","mattress","pillow","cookware"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein","vitamin","supplement","fitness","yoga"]):
        return "Health & Household"
    return "Electronics"

def save_empty():
    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  0, "hotDeals": 0, "couponDeals": 0, "deals": [],
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print("  Saved empty deals.json")

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop...\n")

    try:
        import keepa
        print("  Connecting to Keepa...")
        api = keepa.Keepa(KEEPA_API_KEY)
        print(f"  Connected. Tokens available: {api.tokens_left}")
    except Exception as e:
        print(f"  Failed to connect to Keepa: {e}")
        save_empty()
        return

    # Use keepa library's deal finder
    try:
        print("  Fetching deals...")
        deal_parms = {
            "priceTypes": 0,
            "deltaPercent": MIN_DISCOUNT_PCT,
            "interval": 10080,
            "page": 0,
        }
        deal_asins = api.deals(deal_parms)
        print(f"  Got {len(deal_asins)} deal ASINs")
    except Exception as e:
        print(f"  Deal fetch failed: {e}")
        save_empty()
        return

    if not deal_asins:
        print("  No deals returned.")
        save_empty()
        return

    # Fetch product details using the library
    try:
        print(f"  Fetching product details for {min(len(deal_asins), MAX_DEALS)} ASINs...")
        products = api.query(
            deal_asins[:MAX_DEALS],
            stats=90,
            history=False,
        )
        print(f"  Got {len(products)} products")
    except Exception as e:
        print(f"  Product fetch failed: {e}")
        save_empty()
        return

    # Format deals
    formatted = []
    deal_id   = 1

    for p in products:
        try:
            asin  = p.get("asin", "")
            title = p.get("title", "")
            if not title or len(title) < 5:
                continue

            stats   = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90",   [])

            def to_d(v): return v / 100.0 if v and v > 0 else None
            current = to_d(cur_raw[0] if cur_raw and cur_raw[0] and cur_raw[0] > 0 else None)
            avg90   = to_d(avg_raw[0] if avg_raw and avg_raw[0] and avg_raw[0] > 0 else None)

            pct = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            if pct < MIN_DISCOUNT_PCT:
                continue

            cat   = get_category(p)
            emoji = CATEGORY_EMOJI.get(cat, "🛒")

            formatted.append({
                "id":            deal_id,
                "asin":          asin,
                "cat":           cat,
                "emoji":         emoji,
                "title":         title[:80] + ("..." if len(title) > 80 else ""),
                "desc":          f"{pct}% off recent price",
                "price":         "",
                "was":           "",
                "hasLivePrice":  False,
                "pct":           pct,
                "effectivePct":  pct,
                "hot":           pct >= HOT_DEAL_PCT,
                "discount":      f"{pct}% off",
                "hasCoupon":     False,
                "couponDisplay": None,
                "image":         "",
                "prime":         False,
                "link":          f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt":     datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1
        except Exception as e:
            print(f"  Skipping {p.get('asin','?')}: {e}")

    formatted.sort(key=lambda d: -d["effectivePct"])
    formatted = formatted[:MAX_DEALS]

    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  len(formatted),
        "hotDeals":    sum(1 for d in formatted if d["hot"]),
        "couponDeals": 0,
        "deals":       formatted,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(formatted)} deals")
    print(f"  Hot deals: {output['hotDeals']}")
    print(f"  Updated:   {output['updatedAt']}")

if __name__ == "__main__":
    build_deals_json()
