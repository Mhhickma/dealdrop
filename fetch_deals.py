"""
DealDrop — fetch_deals.py
--------------------------
Two-API pipeline:
  1. Keepa API     — finds deals using 90-day price history + coupon detection
  2. Amazon PA API — fetches live prices, images, titles (TOS compliant to display)

Both are merged into deals.json which the website reads.

Requirements:
    pip install requests

Setup:
    Add all keys to GitHub Secrets (see config.txt)
"""

import json
import os
import time
import hmac
import hashlib
import datetime
import requests

# ─── CONFIG (loaded from GitHub Secrets) ─────────────────────────────────────

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY",      "")
AMAZON_ACCESS_KEY  = os.environ.get("AMAZON_ACCESS_KEY",  "")
AMAZON_SECRET_KEY  = os.environ.get("AMAZON_SECRET_KEY",  "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG",      "")
AMAZON_HOST        = "webservices.amazon.com"
AMAZON_REGION      = "us-east-1"

OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 50
MIN_DISCOUNT_PCT   = 20
HOT_DEAL_PCT       = 50
MIN_COUPON_VALUE   = 3
MIN_COUPON_PCT     = 5

KEEPA_BASE         = "https://api.keepa.com"

# ─── CATEGORY MAPPING ─────────────────────────────────────────────────────────

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
    2238192011:  "Musical Instruments",
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
    "Musical Instruments":       "🎸",
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def keepa_request(endpoint, params):
    params["key"] = KEEPA_API_KEY
    r = requests.get(f"{KEEPA_BASE}/{endpoint}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]
    title = (product.get("title") or "").lower()
    if any(w in title for w in ["laptop","phone","tablet","camera","headphone","speaker","monitor"]):
        return "Electronics"
    if any(w in title for w in ["shirt","shoe","dress","jacket","pants","bag","watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender","vacuum","mattress","pillow","cookware"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein","vitamin","supplement","fitness","yoga"]):
        return "Health & Household"
    return "Electronics"

def parse_coupon(product):
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None
    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns      = coupon_history[idx + 2]
        for val, ctype in [(one_time, "clip"), (sns, "sns")]:
            if val and val != 0:
                if val > 0 and val >= MIN_COUPON_PCT:
                    return {"type": ctype, "kind": "percent", "value": val,
                            "display": f"{val}% off coupon"}
                elif val < 0:
                    dollars = abs(val) / 100.0
                    if dollars >= MIN_COUPON_VALUE:
                        return {"type": ctype, "kind": "dollars", "value": dollars,
                                "display": f"${dollars:.0f} off coupon"}
        idx -= 3
    return None

# ─── STEP 1: KEEPA — FIND DEALS ───────────────────────────────────────────────

def fetch_keepa_asins():
    print("  [Keepa] Fetching price-drop deals...")
    deal_asins = []
    params = {
        "page": 0, "domainId": 1, "priceType": 0,
        "deltaPercent": MIN_DISCOUNT_PCT,
        "deltaPercentInInterval": MIN_DISCOUNT_PCT,
        "interval": 1440, "dateRange": 1440,
        "isOutOfStock": 0, "mustHaveRating": 1,
        "minRating": 30, "minReviews": 10,
    }
    try:
        data = keepa_request("deal", params)
        deal_asins = [d.get("asin") for d in data.get("deals", {}).get("dr", []) if d.get("asin")]
        print(f"  [Keepa] {len(deal_asins)} price-drop candidates")
    except Exception as e:
        print(f"  [Keepa] ERROR: {e}")

    print("  [Keepa] Fetching coupon deals...")
    coupon_asins = []
    try:
        params["deltaPercent"] = 5
        params["interval"]     = 2880
        data = keepa_request("deal", params)
        coupon_asins = [d.get("asin") for d in data.get("deals", {}).get("dr", []) if d.get("asin")]
        print(f"  [Keepa] {len(coupon_asins)} coupon candidates")
    except Exception as e:
        print(f"  [Keepa] ERROR: {e}")

    all_asins = list(dict.fromkeys(deal_asins + coupon_asins))
    print(f"  [Keepa] {len(all_asins)} unique ASINs to process")
    return all_asins, set(coupon_asins)

def fetch_keepa_product_details(asins):
    if not asins:
        return []
    print(f"  [Keepa] Fetching product details ({len(asins)} ASINs)...")
    all_products = []
    for i in range(0, len(asins), 100):
        batch  = asins[i:i+100]
        params = {
            "asin": ",".join(batch), "domainId": 1,
            "stats": 90, "offers": 10, "update": 0, "history": 1,
        }
        try:
            data = keepa_request("product", params)
            all_products.extend(data.get("products", []))
            time.sleep(0.5)
        except Exception as e:
            print(f"  [Keepa] Batch error: {e}")
    print(f"  [Keepa] Got details for {len(all_products)} products")
    return all_products

# ─── STEP 2: AMAZON PA API — FETCH LIVE PRICES ────────────────────────────────

def sign_aws(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_aws_signing_key(secret, date_stamp, region, service):
    k = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k = sign_aws(k, region)
    k = sign_aws(k, service)
    k = sign_aws(k, "aws4_request")
    return k

def fetch_amazon_live_data(asin_batch):
    if not AMAZON_ACCESS_KEY:
        print("  [Amazon PA API] Not configured — skipping live prices.")
        return {}

    service  = "ProductAdvertisingAPI"
    path     = "/paapi5/getitems"
    endpoint = f"https://{AMAZON_HOST}{path}"

    payload = {
        "ItemIds":     asin_batch,
        "PartnerTag":  AMAZON_PARTNER_TAG,
        "PartnerType": "Associates",
        "Marketplace": "www.amazon.com",
        "Resources": [
            "Images.Primary.Large",
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Listings.Availability.Message",
            "Offers.Listings.DeliveryInfo.IsPrimeEligible",
        ]
    }
    body = json.dumps(payload)

    now        = datetime.datetime.utcnow()
    amz_date   = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{AMAZON_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash   = hashlib.sha256(body.encode("utf-8")).hexdigest()

    canonical_request = "\n".join([
        "POST", path, "",
        canonical_headers, signed_headers, payload_hash,
    ])

    credential_scope = f"{date_stamp}/{AMAZON_REGION}/{service}/aws4_request"
    string_to_sign   = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    signing_key   = get_aws_signing_key(AMAZON_SECRET_KEY, date_stamp, AMAZON_REGION, service)
    signature     = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={AMAZON_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        "content-encoding": "amz-1.0",
        "content-type":     "application/json; charset=utf-8",
        "host":             AMAZON_HOST,
        "x-amz-date":       amz_date,
        "x-amz-target":     "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems",
        "Authorization":    authorization,
    }

    try:
        r = requests.post(endpoint, headers=headers, data=body, timeout=15)
        r.raise_for_status()
        items  = r.json().get("ItemsResult", {}).get("Items", [])
        result = {}
        for item in items:
            asin      = item.get("ASIN")
            listing   = (item.get("Offers", {}).get("Listings") or [{}])[0]
            price_obj = listing.get("Price", {})
            img_obj   = item.get("Images", {}).get("Primary", {}).get("Large", {})
            title     = item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", "")
            prime     = listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False)
            result[asin] = {
                "price_display": price_obj.get("DisplayAmount", ""),
                "image":         img_obj.get("URL", ""),
                "title":         title,
                "prime":         prime,
            }
        print(f"  [Amazon PA API] Got live data for {len(result)} products")
        return result
    except Exception as e:
        print(f"  [Amazon PA API] ERROR: {e}")
        return {}

# ─── STEP 3: MERGE AND BUILD deals.json ───────────────────────────────────────

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop deal fetch...\n")

    all_asins, coupon_set = fetch_keepa_asins()
    keepa_products        = fetch_keepa_product_details(all_asins[:MAX_DEALS + 20])

    # Extract deal data from Keepa products
    keepa_deals = {}
    for p in keepa_products:
        try:
            asin    = p.get("asin", "")
            stats   = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90",   [])

            def to_d(v): return v / 100.0 if v and v > 0 else None

            current = to_d(cur_raw[0] if cur_raw and cur_raw[0] and cur_raw[0] > 0
                           else (cur_raw[1] if len(cur_raw) > 1 else None))
            avg90   = to_d(avg_raw[0] if avg_raw and avg_raw[0] and avg_raw[0] > 0 else None)
            coupon  = parse_coupon(p)

            pct = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue

            keepa_deals[asin] = {
                "asin":     asin,
                "category": get_category(p),
                "pct":      pct,
                "coupon":   coupon,
                "title_fallback": (p.get("title") or "")[:80],
            }
        except Exception as e:
            print(f"  Skipping product: {e}")

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n  {len(qualifying_asins)} qualifying deals after Keepa filtering")

    # Fetch live prices from Amazon PA API (10 ASINs per request)
    amazon_data = {}
    for i in range(0, len(qualifying_asins), 10):
        batch  = qualifying_asins[i:i+10]
        result = fetch_amazon_live_data(batch)
        amazon_data.update(result)
        time.sleep(1)

    # Merge everything
    formatted = []
    deal_id   = 1

    for asin in qualifying_asins:
        try:
            k = keepa_deals[asin]
            a = amazon_data.get(asin, {})

            title  = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue

            price   = a.get("price_display", "")
            image   = a.get("image",         "")
            prime   = a.get("prime",         False)
            coupon  = k["coupon"]
            pct     = k["pct"]
            cat     = k["category"]

            effective_pct = pct
            if coupon:
                if coupon["kind"] == "percent":
                    effective_pct = min(99, pct + coupon["value"])

            parts = []
            if pct >= MIN_DISCOUNT_PCT:
                parts.append(f"{pct}% off recent price")
            if coupon:
                parts.append(coupon["display"])
            if prime:
                parts.append("Prime eligible")
            desc = " · ".join(parts)

            formatted.append({
                "id":            deal_id,
                "asin":          asin,
                "cat":           cat,
                "emoji":         CATEGORY_EMOJI.get(cat, "🛒"),
                "title":         title[:80] + ("..." if len(title) > 80 else ""),
                "desc":          desc,
                "price":         price,
                "was":           "",
                "hasLivePrice":  bool(price),
                "pct":           pct,
                "effectivePct":  effective_pct,
                "hot":           effective_pct >= HOT_DEAL_PCT,
                "discount":      f"{pct}% off",
                "hasCoupon":     coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image":         image,
                "prime":         prime,
                "link":          f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt":     datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1
        except Exception as e:
            print(f"  Skipping {asin}: {e}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"]))
    formatted = formatted[:MAX_DEALS]

    hot_count    = sum(1 for d in formatted if d["hot"])
    coupon_count = sum(1 for d in formatted if d["hasCoupon"])

    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  len(formatted),
        "hotDeals":    hot_count,
        "couponDeals": coupon_count,
        "deals":       formatted,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals:    {hot_count}")
    print(f"  Coupon deals: {coupon_count}")
    print(f"  Updated:      {output['updatedAt']}")


if __name__ == "__main__":
    build_deals_json()
