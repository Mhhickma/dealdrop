"""
DealDrop — fetch_deals.py
--------------------------
Two-API pipeline:
  1. Keepa API     — finds deals using 90-day price history + coupon detection
  2. Amazon PA API — fetches live prices, images, titles (TOS compliant to display)

Requirements:
    pip install requests
"""

import json
import os
import time
import hmac
import hashlib
import datetime
import requests

# ─── CONFIG ───────────────────────────────────────────────────────────────────

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

def keepa_deal_request(deal_params):
    """
    Call Keepa deal endpoint — POST with JSON body.
    priceTypes must be an array e.g. [0] not 0.
    """
    url     = f"{KEEPA_BASE}/deal"
    params  = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    print(f"    POST {url}")
    print(f"    Body: {json.dumps(deal_params)}")

    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    print(f"    Status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Response: {r.text[:500]}")
    r.raise_for_status()
    return r.json()

def keepa_product_request(asins):
    """Call Keepa product endpoint — GET with query params."""
    url    = f"{KEEPA_BASE}/product"
    params = {
        "key":      KEEPA_API_KEY,
        "asin":     ",".join(asins),
        "domainId": 1,
        "stats":    90,
        "history":  0,
    }
    r = requests.get(url, params=params, timeout=60)
    print(f"    Product request status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Response: {r.text[:300]}")
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
    if any(w in title for w in ["laptop","phone","tablet","camera","headphone","speaker","monitor","tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt","shoe","dress","jacket","pants","bag","watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender","vacuum","mattress","pillow","cookware","kitchen"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein","vitamin","supplement","fitness","yoga"]):
        return "Health & Household"
    if any(w in title for w in ["toy","game","lego","puzzle","kids"]):
        return "Toys & Games"
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
    """
    Use Keepa Deal finder with correct format.
    priceTypes = array, domainId = integer, all other fields as per Keepa docs.
    """
    print("\n  [Keepa] Fetching deals...")

    # Try progressively simpler requests until one works
    attempts = [
        # Full request with priceTypes as array
        {
            "domainId":     1,
            "priceTypes":   [0],
            "deltaPercent": MIN_DISCOUNT_PCT,
            "interval":     10080,
            "page":         0,
        },
        # Without interval
        {
            "domainId":     1,
            "priceTypes":   [0],
            "deltaPercent": MIN_DISCOUNT_PCT,
            "page":         0,
        },
        # Absolute minimum
        {
            "domainId": 1,
            "page":     0,
        },
    ]

    deal_asins = []
    for i, body in enumerate(attempts):
        try:
            print(f"    Attempt {i+1}...")
            data       = keepa_deal_request(body)
            deals_raw  = data.get("deals", {}).get("dr", [])
            deal_asins = [d.get("asin") for d in deals_raw if d.get("asin")]
            print(f"  [Keepa] Got {len(deal_asins)} candidates")
            if deal_asins or i == len(attempts) - 1:
                break
        except Exception as e:
            print(f"    Attempt {i+1} failed: {e}")
            time.sleep(2)

    all_asins = list(dict.fromkeys(deal_asins))
    print(f"  [Keepa] {len(all_asins)} unique ASINs")
    return all_asins

def fetch_keepa_product_details(asins):
    """Get full product details from Keepa."""
    if not asins:
        return []
    print(f"  [Keepa] Fetching product details ({len(asins)} ASINs)...")
    all_products = []
    for i in range(0, len(asins), 20):
        batch = asins[i:i+20]
        try:
            data     = keepa_product_request(batch)
            products = data.get("products", [])
            all_products.extend(products)
            print(f"    Batch {i//20 + 1}: {len(products)} products")
            time.sleep(1)
        except Exception as e:
            print(f"  [Keepa] Batch error: {e}")
    print(f"  [Keepa] Total: {len(all_products)} products")
    return all_products

# ─── STEP 2: AMAZON PA API — LIVE PRICES ─────────────────────────────────────

def sign_aws(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_aws_signing_key(secret, date_stamp, region, service):
    k = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k = sign_aws(k, region)
    k = sign_aws(k, service)
    k = sign_aws(k, "aws4_request")
    return k

def fetch_amazon_live_data(asin_batch):
    """Fetch live prices and images from Amazon PA API."""
    if not AMAZON_ACCESS_KEY:
        print("  [Amazon PA API] Not configured — skipping.")
        return {}
    service  = "ProductAdvertisingAPI"
    path     = "/paapi5/getitems"
    endpoint = f"https://{AMAZON_HOST}{path}"
    payload  = {
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
    signed_headers    = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash      = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_request = "\n".join(["POST", path, "", canonical_headers, signed_headers, payload_hash])
    credential_scope  = f"{date_stamp}/{AMAZON_REGION}/{service}/aws4_request"
    string_to_sign    = "\n".join([
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
        print(f"  [Amazon PA API] Got data for {len(result)} products")
        return result
    except Exception as e:
        print(f"  [Amazon PA API] ERROR: {e}")
        return {}

# ─── STEP 3: BUILD deals.json ─────────────────────────────────────────────────

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop deal fetch...\n")

    all_asins = fetch_keepa_asins()

    if not all_asins:
        print("\n  No ASINs returned. Saving empty deals.json.")
        output = {
            "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
            "totalDeals":  0, "hotDeals": 0, "couponDeals": 0, "deals": [],
        }
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, indent=2)
        return

    keepa_products = fetch_keepa_product_details(all_asins[:MAX_DEALS + 20])

    keepa_deals = {}
    for p in keepa_products:
        try:
            asin    = p.get("asin", "")
            stats   = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90",   [])
            def to_d(v): return v / 100.0 if v and v > 0 else None
            current = to_d(cur_raw[0] if cur_raw and cur_raw[0] and cur_raw[0] > 0 else None)
            avg90   = to_d(avg_raw[0] if avg_raw and avg_raw[0] and avg_raw[0] > 0 else None)
            coupon  = parse_coupon(p)
            pct     = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)
            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue
            keepa_deals[asin] = {
                "asin": asin, "category": get_category(p),
                "pct": pct, "coupon": coupon,
                "title_fallback": (p.get("title") or "")[:80],
            }
        except Exception as e:
            print(f"  Skipping: {e}")

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n  {len(qualifying_asins)} qualifying deals")

    amazon_data = {}
    for i in range(0, len(qualifying_asins), 10):
        result = fetch_amazon_live_data(qualifying_asins[i:i+10])
        amazon_data.update(result)
        time.sleep(1)

    formatted = []
    deal_id   = 1
    for asin in qualifying_asins:
        try:
            k = keepa_deals[asin]
            a = amazon_data.get(asin, {})
            title = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue
            price  = a.get("price_display", "")
            image  = a.get("image", "")
            prime  = a.get("prime", False)
            coupon = k["coupon"]
            pct    = k["pct"]
            cat    = k["category"]
            effective_pct = pct
            if coupon and coupon["kind"] == "percent":
                effective_pct = min(99, pct + coupon["value"])
            parts = []
            if pct >= MIN_DISCOUNT_PCT: parts.append(f"{pct}% off recent price")
            if coupon:                  parts.append(coupon["display"])
            if prime:                   parts.append("Prime eligible")
            formatted.append({
                "id": deal_id, "asin": asin, "cat": cat,
                "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
                "title": title[:80] + ("..." if len(title) > 80 else ""),
                "desc": " · ".join(parts),
                "price": price, "was": "",
                "hasLivePrice": bool(price),
                "pct": pct, "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off",
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image, "prime": prime,
                "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1
        except Exception as e:
            print(f"  Skipping {asin}: {e}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"]))
    formatted = formatted[:MAX_DEALS]

    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  len(formatted),
        "hotDeals":    sum(1 for d in formatted if d["hot"]),
        "couponDeals": sum(1 for d in formatted if d["hasCoupon"]),
        "deals":       formatted,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals:    {output['hotDeals']}")
    print(f"  Coupon deals: {output['couponDeals']}")
    print(f"  Updated:      {output['updatedAt']}")


if __name__ == "__main__":
    build_deals_json()
