"""
DealDrop — fetch_deals.py
"""

import json
import os
import time
import hmac
import hashlib
import datetime
from collections import Counter

import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_ACCESS_KEY  = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY  = os.environ.get("AMAZON_SECRET_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
AMAZON_HOST        = "webservices.amazon.com"
AMAZON_REGION      = "us-east-1"

OUTPUT_FILE        = "deals.json"

MAX_DEALS          = 150
KEEPA_PAGES        = 3
PAGE_DELAY_SEC     = 1.2
PRODUCT_DELAY_SEC  = 0.35

MIN_DISCOUNT_PCT   = 10
HOT_DEAL_PCT       = 30
MIN_COUPON_VALUE   = 3
MIN_COUPON_PCT     = 5

EXCLUDED_CATEGORIES = {
    "Books",
}

CATEGORY_LIMITS = {
    "Clothing, Shoes & Jewelry": 6,
    "Home & Kitchen": 10,
    "Electronics": 10,
    "Tools & Home Improvement": 14,
}

DEFAULT_CATEGORY_LIMIT = 8

KEEPA_BASE = "https://api.keepa.com"

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


def keepa_deal_request(deal_params):
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    if r.status_code != 200:
        print(f"    Deal request failed: {r.status_code}")
        print(f"    Response: {r.text[:500]}")
    r.raise_for_status()
    return r.json()


def keepa_product_request(asins):
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "domain": 1,
        "asin": ",".join(asins),
    }

    try:
        r = requests.get(url, params=params, timeout=60)
        print(f"    Product status: {r.status_code}")
        if r.status_code != 200:
            print(f"    Response: {r.text[:500]}")
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"    Product request error: {e}")
        return {"products": []}


def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]

    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    title = (product.get("title") or "").lower()

    if any(w in title for w in ["laptop", "phone", "tablet", "camera", "headphone", "speaker", "monitor", "tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt", "shoe", "dress", "jacket", "pants", "bag", "watch", "bra", "sandal", "sneaker", "slipper", "mule"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender", "vacuum", "mattress", "pillow", "cookware", "kitchen", "rug", "ottoman", "tumbler"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein", "vitamin", "supplement", "fitness", "yoga"]):
        return "Health & Household"
    if any(w in title for w in ["toy", "game", "lego", "puzzle", "kids"]):
        return "Toys & Games"

    return "Electronics"


def parse_coupon(product):
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None

    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns = coupon_history[idx + 2]

        for val, ctype in [(one_time, "clip"), (sns, "sns")]:
            if val and val != 0:
                if val > 0 and val >= MIN_COUPON_PCT:
                    return {
                        "type": ctype,
                        "kind": "percent",
                        "value": val,
                        "display": f"{val}% off coupon",
                    }
                if val < 0:
                    dollars = abs(val) / 100.0
                    if dollars >= MIN_COUPON_VALUE:
                        return {
                            "type": ctype,
                            "kind": "dollars",
                            "value": dollars,
                            "display": f"${dollars:.0f} off coupon",
                        }
        idx -= 3

    return None


def fetch_keepa_candidates():
    print("\n  [Keepa] Fetching deals across multiple pages...")

    candidates = {}
    seen = set()

    for page in range(KEEPA_PAGES):
        body = {
            "domainId": 1,
            "priceTypes": [0],
            "deltaPercent": MIN_DISCOUNT_PCT,
            "interval": 10080,
            "page": page,
        }

        try:
            data = keepa_deal_request(body)
            deals_raw = data.get("deals", {}).get("dr", [])

            new_count = 0
            for d in deals_raw:
                asin = d.get("asin")
                if not asin:
                    continue
                if asin not in seen:
                    seen.add(asin)
                    new_count += 1

                # Keepa deal endpoint already matched these as deals.
                # We do not require product stats later to prove that again.
                delta = d.get("deltaPercent")
                if isinstance(delta, (int, float)):
                    pct = max(MIN_DISCOUNT_PCT, int(delta))
                else:
                    pct = MIN_DISCOUNT_PCT

                existing = candidates.get(asin)
                if not existing or pct > existing["pct"]:
                    candidates[asin] = {
                        "asin": asin,
                        "pct": pct,
                    }

            print(f"  [Keepa] Page {page}: {len(deals_raw)} candidates, {new_count} new unique")
        except Exception as e:
            print(f"  [Keepa] Page {page} failed: {e}")

        time.sleep(PAGE_DELAY_SEC)

    print(f"  [Keepa] {len(candidates)} unique ASINs across {KEEPA_PAGES} pages")
    return candidates


def fetch_keepa_product_details(asins):
    if not asins:
        return {}

    print(f"  [Keepa] Fetching product details for {len(asins)} ASINs...")
    products_by_asin = {}

    for i, asin in enumerate(asins, start=1):
        try:
            data = keepa_product_request([asin])
            products = data.get("products", [])

            if products:
                products_by_asin[asin] = products[0]

            if i % 10 == 0 or i == len(asins):
                print(f"    Progress: {i}/{len(asins)} ({len(products_by_asin)} successful)")

            time.sleep(PRODUCT_DELAY_SEC)

        except Exception as e:
            print(f"  [Keepa] Error on {asin}: {e}")

        if len(products_by_asin) >= MAX_DEALS + 75:
            break

    print(f"  [Keepa] Total products returned: {len(products_by_asin)}")
    return products_by_asin


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
        print("  [Amazon PA API] Not configured — skipping.")
        return {}

    service = "ProductAdvertisingAPI"
    path = "/paapi5/getitems"
    endpoint = f"https://{AMAZON_HOST}{path}"

    payload = {
        "ItemIds": asin_batch,
        "PartnerTag": AMAZON_PARTNER_TAG,
        "PartnerType": "Associates",
        "Marketplace": "www.amazon.com",
        "Resources": [
            "Images.Primary.Large",
            "ItemInfo.Title",
            "Offers.Listings.Price",
            "Offers.Listings.Availability.Message",
            "Offers.Listings.DeliveryInfo.IsPrimeEligible",
        ],
    }

    body = json.dumps(payload)
    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    canonical_headers = (
        f"content-encoding:amz-1.0\n"
        f"content-type:application/json; charset=utf-8\n"
        f"host:{AMAZON_HOST}\n"
        f"x-amz-date:{amz_date}\n"
        f"x-amz-target:com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems\n"
    )
    signed_headers = "content-encoding;content-type;host;x-amz-date;x-amz-target"
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_request = "\n".join(["POST", path, "", canonical_headers, signed_headers, payload_hash])
    credential_scope = f"{date_stamp}/{AMAZON_REGION}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signing_key = get_aws_signing_key(AMAZON_SECRET_KEY, date_stamp, AMAZON_REGION, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={AMAZON_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    headers = {
        "content-encoding": "amz-1.0",
        "content-type": "application/json; charset=utf-8",
        "host": AMAZON_HOST,
        "x-amz-date": amz_date,
        "x-amz-target": "com.amazon.paapi5.v1.ProductAdvertisingAPIv1.GetItems",
        "Authorization": authorization,
    }

    try:
        r = requests.post(endpoint, headers=headers, data=body, timeout=15)
        r.raise_for_status()

        items = r.json().get("ItemsResult", {}).get("Items", [])
        result = {}

        for item in items:
            asin = item.get("ASIN")
            listing = (item.get("Offers", {}).get("Listings") or [{}])[0]
            price_obj = listing.get("Price", {})
            img_obj = item.get("Images", {}).get("Primary", {}).get("Large", {})
            title = item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", "")
            prime = listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False)

            result[asin] = {
                "price_display": price_obj.get("DisplayAmount", ""),
                "image": img_obj.get("URL", ""),
                "title": title,
                "prime": prime,
            }

        print(f"  [Amazon PA API] Got data for {len(result)} products")
        return result

    except Exception as e:
        print(f"  [Amazon PA API] ERROR: {e}")
        return {}


def category_limit_for(category):
    return CATEGORY_LIMITS.get(category, DEFAULT_CATEGORY_LIMIT)


def compute_sort_score(deal):
    score = 0
    effective_pct = deal.get("effectivePct", 0)
    pct = deal.get("pct", 0)

    score += effective_pct * 10
    score += pct * 4

    if deal.get("hot"):
        score += 250
    if deal.get("hasCoupon"):
        score += 60
    if deal.get("prime"):
        score += 12
    if deal.get("hasLivePrice"):
        score += 20

    cat = deal.get("cat", "")
    if cat == "Clothing, Shoes & Jewelry":
        score -= 90
    elif cat == "Home & Kitchen":
        score -= 10

    return score


def apply_variety_limits(deals):
    sorted_deals = sorted(
        deals,
        key=lambda d: (-compute_sort_score(d), d.get("cat", ""), d.get("title", ""))
    )

    selected = []
    counts = Counter()

    for deal in sorted_deals:
        cat = deal.get("cat", "Other")
        if cat in EXCLUDED_CATEGORIES:
            continue
        if counts[cat] >= category_limit_for(cat):
            continue

        selected.append(deal)
        counts[cat] += 1

        if len(selected) >= MAX_DEALS:
            break

    if len(selected) < MAX_DEALS:
        selected_asins = {d.get("asin") for d in selected}
        for deal in sorted_deals:
            asin = deal.get("asin")
            if asin in selected_asins:
                continue
            if deal.get("cat") in EXCLUDED_CATEGORIES:
                continue

            selected.append(deal)
            selected_asins.add(asin)
            if len(selected) >= MAX_DEALS:
                break

    return selected


def build_deals_json():
    print("RUNNING NEW FETCH_DEALS VERSION")
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop deal fetch...\n")

    candidate_map = fetch_keepa_candidates()

    if not candidate_map:
        print("\n  No ASINs returned. Saving empty deals.json.")
        output = {
            "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            "totalDeals": 0,
            "hotDeals": 0,
            "couponDeals": 0,
            "deals": [],
        }
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, indent=2)
        return

    candidate_asins = list(candidate_map.keys())
    keepa_products = fetch_keepa_product_details(candidate_asins)

    qualifying = {}
    for asin, base in candidate_map.items():
        product = keepa_products.get(asin)
        if not product:
            continue

        category = get_category(product)
        if category in EXCLUDED_CATEGORIES:
            continue

        coupon = parse_coupon(product)

        qualifying[asin] = {
            "asin": asin,
            "category": category,
            "pct": max(MIN_DISCOUNT_PCT, int(base.get("pct", MIN_DISCOUNT_PCT))),
            "coupon": coupon,
            "title_fallback": (product.get("title") or "")[:120],
        }

    print(f"\n  {len(qualifying)} qualifying deals after Keepa filtering")

    amazon_data = {}
    qualifying_asins = list(qualifying.keys())
    for i in range(0, len(qualifying_asins), 10):
        batch = qualifying_asins[i:i+10]
        result = fetch_amazon_live_data(batch)
        amazon_data.update(result)
        time.sleep(1)

    formatted = []
    deal_id = 1

    for asin in qualifying_asins:
        try:
            k = qualifying[asin]
            a = amazon_data.get(asin, {})

            title = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue

            price = a.get("price_display", "")
            image = a.get("image", "")
            prime = a.get("prime", False)
            coupon = k["coupon"]
            pct = k["pct"]
            cat = k["category"]

            effective_pct = pct
            if coupon and coupon["kind"] == "percent":
                effective_pct = min(99, pct + coupon["value"])

            parts = []
            if pct >= MIN_DISCOUNT_PCT:
                parts.append(f"{pct}% off recent price")
            if coupon:
                parts.append(coupon["display"])
            if prime:
                parts.append("Prime eligible")

            formatted.append({
                "id": deal_id,
                "asin": asin,
                "cat": cat,
                "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
                "title": title[:90] + ("..." if len(title) > 90 else ""),
                "desc": " · ".join(parts),
                "price": price,
                "was": "",
                "hasLivePrice": bool(price),
                "pct": pct,
                "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off",
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image,
                "prime": prime,
                "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1

        except Exception as e:
            print(f"  Skipping formatted deal {asin}: {e}")

    final_deals = apply_variety_limits(formatted)

    for idx, deal in enumerate(final_deals, start=1):
        deal["id"] = idx

    output = {
        "updatedAt": datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals": len(final_deals),
        "hotDeals": sum(1 for d in final_deals if d["hot"]),
        "couponDeals": sum(1 for d in final_deals if d["hasCoupon"]),
        "deals": final_deals,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(final_deals)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals:    {output['hotDeals']}")
    print(f"  Coupon deals: {output['couponDeals']}")
    print(f"  Updated:      {output['updatedAt']}")

    by_cat = Counter(d["cat"] for d in final_deals)
    print("  Category mix:")
    for cat, count in by_cat.most_common():
        print(f"    {cat}: {count}")


if __name__ == "__main__":
    build_deals_json()
