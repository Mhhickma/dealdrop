"""
DealDrop — fetch_deals.py
Uses same Keepa API call format as your working Google Sheets script.
"""

import json
import os
import time
import datetime
import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 50
MIN_DISCOUNT_PCT   = 5
HOT_DEAL_PCT       = 50
DOMAIN_ID          = "1"

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

def get_price_at_time(history, minutes_ago):
    if not history or len(history) < 2:
        return -1
    last_time = history[-2]
    target_time = last_time - minutes_ago
    i = len(history) - 2
    while i >= 0:
        t = history[i]
        p = history[i + 1]
        if t <= target_time:
            return p
        i -= 2
    return history[1]

def save_empty():
    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  0, "hotDeals": 0, "couponDeals": 0, "deals": [],
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print("  Saved empty deals.json")

def fetch_products(asins):
    chunk_size = 20
    all_products = []
    for i in range(0, len(asins), chunk_size):
        chunk = asins[i:i+chunk_size]
        url = (
            f"https://api.keepa.com/product"
            f"?key={KEEPA_API_KEY}"
            f"&domain={DOMAIN_ID}"
            f"&asin={','.join(chunk)}"
            f"&stats=1"
            f"&history=1"
            f"&days=2"
        )
        print(f"    Fetching {len(chunk)} products...")
        try:
            r = requests.get(url, timeout=30)
            print(f"    Status: {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                products = data.get("products", [])
                all_products.extend(products)
                print(f"    Got {len(products)} products")
            else:
                print(f"    Error: {r.text[:200]}")
            time.sleep(1)
        except Exception as e:
            print(f"    Request failed: {e}")
    return all_products

def fetch_deal_asins():
    print("  Fetching deals from Keepa...")
    url     = "https://api.keepa.com/deal"
    params  = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}
    body    = {
        "domainId":     1,
        "priceTypes":   [0],
        "deltaPercent": MIN_DISCOUNT_PCT,
        "interval":     10080,
        "page":         0,
    }
    try:
        r = requests.post(url, params=params, json=body, headers=headers, timeout=30)
        print(f"    Status: {r.status_code}")
        if r.status_code == 200:
            data   = r.json()
            deals  = data.get("deals", {}).get("dr", [])
            asins  = [d.get("asin") for d in deals if d.get("asin")]
            print(f"    Got {len(asins)} deal ASINs")
            return asins
        else:
            print(f"    Error: {r.text[:200]}")
            return []
    except Exception as e:
        print(f"    Failed: {e}")
        return []

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop...\n")

    deal_asins = fetch_deal_asins()
    if not deal_asins:
        print("  No deal ASINs. Saving empty.")
        save_empty()
        return

    print(f"\n  Fetching details for {min(len(deal_asins), MAX_DEALS)} products...")
    products = fetch_products(deal_asins[:MAX_DEALS])
    print(f"  Total products fetched: {len(products)}")

    if not products:
        print("  No products returned. Saving empty.")
        save_empty()
        return

    formatted = []
    deal_id   = 1

    for p in products:
        try:
            asin  = p.get("asin", "")
            title = p.get("title", "")
            if not title or len(title) < 5:
                continue

            current_stats = p.get("stats", {}).get("current", [])
            current_price = -1
            price_type    = -1

            if len(current_stats) > 18 and current_stats[18] > 0:
                current_price = current_stats[18]; price_type = 18
            elif len(current_stats) > 1 and current_stats[1] > 0:
                current_price = current_stats[1];  price_type = 1
            elif len(current_stats) > 0 and current_stats[0] > 0:
                current_price = current_stats[0];  price_type = 0

            yesterday_price = -1
            csv_data        = p.get("csv", [])
            if price_type != -1 and csv_data and len(csv_data) > price_type and csv_data[price_type]:
                yesterday_price = get_price_at_time(csv_data[price_type], 24 * 60)
            if yesterday_price == -1:
                yesterday_price = current_price

            pct = 0
            if current_price > 0 and yesterday_price > 0:
                drop = (yesterday_price - current_price) / yesterday_price
                if drop > 0:
                    pct = round(drop * 100)

            if pct < MIN_DISCOUNT_PCT:
                continue

            image_url = ""
            if p.get("imagesCSV"):
                image_url = "https://images-na.ssl-images-amazon.com/images/I/" + p["imagesCSV"].split(",")[0]

            cat   = get_category(p)
            emoji = CATEGORY_EMOJI.get(cat, "🛒")

            price_display = f"${current_price/100:.2f}"   if current_price   > 0 else ""
            was_display   = f"${yesterday_price/100:.2f}" if yesterday_price > 0 else ""

            formatted.append({
                "id":            deal_id,
                "asin":          asin,
                "cat":           cat,
                "emoji":         emoji,
                "title":         title[:80] + ("..." if len(title) > 80 else ""),
                "desc":          f"{pct}% off yesterday's price",
                "price":         price_display,
                "was":           was_display,
                "hasLivePrice":  bool(price_display),
                "pct":           pct,
                "effectivePct":  pct,
                "hot":           pct >= HOT_DEAL_PCT,
                "discount":      f"{pct}% off",
                "hasCoupon":     False,
                "couponDisplay": None,
                "image":         image_url,
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

    print(f"\n✓ Saved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals: {output['hotDeals']}")
    print(f"  Updated:   {output['updatedAt']}")

if __name__ == "__main__":
    build_deals_json()
