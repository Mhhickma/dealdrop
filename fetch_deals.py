import os
import json
import time
import math
import hmac
import hashlib
import datetime as dt
from typing import Dict, List, Optional, Any

import requests


# =========================
# CONFIG
# =========================

KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
AMAZON_ACCESS_KEY = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY = os.environ.get("AMAZON_SECRET_KEY", "")

AMAZON_HOST = "webservices.amazon.com"
AMAZON_REGION = "us-east-1"

OUTPUT_FILE = "deals.json"
MEMORY_FILE = "deals_memory.json"

# How many Keepa candidates to try to process total
MAX_DEALS = 200

# How many deals to actually save to deals.json
DEALS_TO_SHOW = 200

# Filters
MIN_DISCOUNT_PCT = 10
HOT_DEAL_PCT = 50
MIN_COUPON_DOLLARS = 3
MIN_COUPON_PERCENT = 5

# Memory / freshness
DEAL_TTL_HOURS = 24

# Keepa
KEEPA_BASE = "https://api.keepa.com"
KEEPA_DEAL_DOMAIN_ID = 1

# Amazon PA batch size
PA_API_BATCH_SIZE = 10

# Request pacing
REQUEST_SLEEP_SECONDS = 0.35


# =========================
# CATEGORY HELPERS
# =========================

CATEGORY_NAMES = {
    281052: "Electronics",
    1055398: "Home & Kitchen",
    7141123011: "Clothing, Shoes & Jewelry",
    3760901: "Luggage & Travel",
    3375251: "Sports & Outdoors",
    165793011: "Toys & Games",
    2619525011: "Tools & Home Improvement",
    51574011: "Pet Supplies",
    165796011: "Baby",
    172282: "Electronics",
    1064954: "Health & Household",
    3760911: "Beauty & Personal Care",
    2238192011: "Musical Instruments",
    979455011: "Garden & Outdoor",
    1285128: "Office Products",
    468642: "Video Games",
    283155: "Books",
    16310101: "Grocery & Gourmet Food",
    9482648011: "Kitchen & Dining",
}

CATEGORY_EMOJI = {
    "Electronics": "💻",
    "Home & Kitchen": "🏠",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care": "💄",
    "Health & Household": "💊",
    "Toys & Games": "🧸",
    "Sports & Outdoors": "⚽",
    "Automotive": "🚗",
    "Pet Supplies": "🐾",
    "Baby": "🍼",
    "Garden & Outdoor": "🌱",
    "Office Products": "📎",
    "Tools & Home Improvement": "🔧",
    "Kitchen & Dining": "🍳",
    "Video Games": "🎮",
    "Books": "📚",
    "Musical Instruments": "🎸",
    "Grocery & Gourmet Food": "🛒",
    "Luggage & Travel": "🧳",
}


def get_category(product: Dict[str, Any]) -> str:
    root = product.get("rootCategory")
    if root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]

    for cat_id in product.get("categories", []) or []:
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    title = (product.get("title") or "").lower()
    if any(w in title for w in ["drill", "saw", "router", "sander", "clamp", "tool", "bit", "blade"]):
        return "Tools & Home Improvement"
    if any(w in title for w in ["laptop", "phone", "tablet", "camera", "headphone", "speaker", "monitor", "tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt", "shoe", "dress", "jacket", "pants", "watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender", "vacuum", "pillow", "cookware", "kitchen"]):
        return "Home & Kitchen"

    return "Tools & Home Improvement"


# =========================
# FILE HELPERS
# =========================

def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def load_json_file(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_memory() -> Dict[str, Any]:
    data = load_json_file(MEMORY_FILE, {})
    return data if isinstance(data, dict) else {}


def save_memory(memory: Dict[str, Any]) -> None:
    save_json_file(MEMORY_FILE, memory)


def prune_memory(memory: Dict[str, Any], ttl_hours: int) -> Dict[str, Any]:
    cutoff = utc_now() - dt.timedelta(hours=ttl_hours)
    pruned = {}

    for asin, meta in memory.items():
        first_seen = meta.get("firstSeen")
        if not first_seen:
            continue
        try:
            seen_dt = dt.datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            if seen_dt >= cutoff:
                pruned[asin] = meta
        except ValueError:
            continue

    return pruned


# =========================
# KEEPA HELPERS
# =========================

def keepa_deal_request(body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    resp = requests.post(url, params=params, json=body, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def keepa_product_request(asins: List[str]) -> Dict[str, Any]:
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "asin": ",".join(asins),
        "stats": 1,
        "history": 1,
        "days": 2,
    }

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def is_standard_asin(value: Any) -> bool:
    value = str(value).strip().upper()
    return len(value) == 10 and value[0].isalpha() and value.isalnum()


def parse_coupon(product: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    coupon_history = product.get("coupon")
    if not coupon_history or len(coupon_history) < 3:
        return None

    idx = len(coupon_history) - 3
    while idx >= 0:
        one_time = coupon_history[idx + 1]
        sns = coupon_history[idx + 2]

        for val, coupon_type in [(one_time, "clip"), (sns, "sns")]:
            if not val:
                continue

            if val > 0 and val >= MIN_COUPON_PERCENT:
                return {
                    "type": coupon_type,
                    "kind": "percent",
                    "value": int(val),
                    "display": f"{int(val)}% off coupon",
                }

            if val < 0:
                dollars = abs(val) / 100.0
                if dollars >= MIN_COUPON_DOLLARS:
                    return {
                        "type": coupon_type,
                        "kind": "dollars",
                        "value": dollars,
                        "display": f"${dollars:.0f} off coupon",
                    }

        idx -= 3

    return None


def cents_to_dollars(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)) and value > 0:
        return float(value) / 100.0
    return None


def fetch_keepa_asins() -> List[str]:
    print("\n[Keepa] Fetching deal ASINs...")

    body = {
        "domainId": KEEPA_DEAL_DOMAIN_ID,
        "priceTypes": [0],
        "deltaPercent": MIN_DISCOUNT_PCT,
        "interval": 10080,
        "page": 0,
    }

    data = keepa_deal_request(body)
    raw_deals = data.get("deals", {}).get("dr", []) or []

    asins = []
    for item in raw_deals:
        asin = item.get("asin")
        if asin:
            asins.append(str(asin).strip().upper())

    unique_asins = list(dict.fromkeys(asins))
    print(f"[Keepa] Got {len(unique_asins)} unique ASINs")
    return unique_asins[:MAX_DEALS]


def fetch_keepa_product_details(asins: List[str]) -> List[Dict[str, Any]]:
    if not asins:
        return []

    cleaned_asins = [asin for asin in asins if is_standard_asin(asin)]

    print(f"\n[Keepa] Raw ASIN candidates: {len(asins)}")
    print(f"[Keepa] Valid standard ASINs: {len(cleaned_asins)}")
    print(f"[Keepa] Fetching {len(cleaned_asins)} product details...")

    products: List[Dict[str, Any]] = []

    for i in range(0, len(cleaned_asins), 10):
        batch = cleaned_asins[i:i + 10]
        try:
            data = keepa_product_request(batch)
            batch_products = data.get("products", []) or []
            products.extend(batch_products)
            print(f"Got {len(batch_products)} products")
        except requests.HTTPError as e:
            print(f"[Keepa] Batch failed: {batch}")
            print(f"[Keepa] Error: {e}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"Total products fetched: {len(products)}")
    return products


# =========================
# AMAZON PA API HELPERS
# =========================

def sign_aws(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def get_aws_signing_key(secret: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = sign_aws(k_date, region)
    k_service = sign_aws(k_region, service)
    k_signing = sign_aws(k_service, "aws4_request")
    return k_signing


def fetch_amazon_live_data(asin_batch: List[str]) -> Dict[str, Dict[str, Any]]:
    if not AMAZON_ACCESS_KEY or not AMAZON_SECRET_KEY or not AMAZON_PARTNER_TAG:
        print("[Amazon PA API] Missing credentials or partner tag. Skipping.")
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
    now = dt.datetime.utcnow()
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

    canonical_request = "\n".join([
        "POST",
        path,
        "",
        canonical_headers,
        signed_headers,
        payload_hash,
    ])

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

    resp = requests.post(endpoint, headers=headers, data=body, timeout=30)
    resp.raise_for_status()

    result: Dict[str, Dict[str, Any]] = {}
    items = resp.json().get("ItemsResult", {}).get("Items", []) or []

    for item in items:
        asin = item.get("ASIN")
        if not asin:
            continue

        listing = (item.get("Offers", {}).get("Listings") or [{}])[0]
        price_obj = listing.get("Price", {}) or {}
        image_obj = item.get("Images", {}).get("Primary", {}).get("Large", {}) or {}
        title_obj = item.get("ItemInfo", {}).get("Title", {}) or {}

        result[asin] = {
            "title": title_obj.get("DisplayValue", ""),
            "image": image_obj.get("URL", ""),
            "price_display": price_obj.get("DisplayAmount", ""),
            "price_amount": price_obj.get("Amount"),
            "currency": price_obj.get("Currency"),
            "prime": listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False),
        }

    return result


# =========================
# DEAL BUILDING
# =========================

def build_keepa_deal_map(products: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    keepa_deals: Dict[str, Dict[str, Any]] = {}

    for p in products:
        asin = str(p.get("asin", "")).strip().upper()
        if not asin or not is_standard_asin(asin):
            continue

        stats = p.get("stats", {}) or {}
        current = cents_to_dollars((stats.get("current") or [None])[0])
        avg90 = cents_to_dollars((stats.get("avg90") or [None])[0])

        pct = 0
        if current and avg90 and avg90 > 0 and current < avg90:
            pct = round((1 - current / avg90) * 100)

        coupon = parse_coupon(p)

        if pct < MIN_DISCOUNT_PCT and coupon is None:
            continue

        keepa_deals[asin] = {
            "asin": asin,
            "category": get_category(p),
            "pct": pct,
            "coupon": coupon,
            "title_fallback": (p.get("title") or "")[:200],
            "current_price_estimate": current,
            "avg90_price": avg90,
        }

    return keepa_deals


def update_memory(memory: Dict[str, Any], formatted: List[Dict[str, Any]]) -> Dict[str, Any]:
    now_iso = iso_now()

    for deal in formatted:
        asin = deal["asin"]
        existing = memory.get(asin, {})
        memory[asin] = {
            "id": existing.get("id", deal["id"]),
            "asin": asin,
            "cat": deal["cat"],
            "emoji": deal["emoji"],
            "title": deal["title"],
            "desc": deal["desc"],
            "price": deal["price"],
            "was": deal["was"],
            "hasLivePrice": deal["hasLivePrice"],
            "pct": deal["pct"],
            "effectivePct": deal["effectivePct"],
            "hot": deal["hot"],
            "discount": deal["discount"],
            "hasCoupon": deal["hasCoupon"],
            "couponDisplay": deal["couponDisplay"],
            "image": deal["image"],
            "prime": deal["prime"],
            "link": deal["link"],
            "updatedAt": now_iso,
            "firstSeen": existing.get("firstSeen", now_iso),
        }

    return memory


def build_formatted_deals(
    keepa_deals: Dict[str, Dict[str, Any]],
    amazon_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    formatted: List[Dict[str, Any]] = []
    deal_id = 1
    skipped_no_title = 0

    for asin, k in keepa_deals.items():
        a = amazon_data.get(asin, {})

        title = a.get("title") or k.get("title_fallback") or ""
        if not title.strip():
            title = f"Amazon Deal {asin}"

        if len(title.strip()) < 5:
            skipped_no_title += 1
            continue

        pct = int(k.get("pct", 0))
        coupon = k.get("coupon")
        effective_pct = pct

        if coupon and coupon["kind"] == "percent":
            effective_pct = min(99, pct + int(coupon["value"]))

        parts = []
        if pct >= MIN_DISCOUNT_PCT:
            parts.append(f"{pct}% off recent price")
        if coupon:
            parts.append(coupon["display"])
        if a.get("prime"):
            parts.append("Prime eligible")

        avg90 = k.get("avg90_price")
        was_display = f"${avg90:.2f}" if avg90 else ""

        cat = k["category"]

        formatted.append({
            "id": deal_id,
            "asin": asin,
            "cat": cat,
            "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
            "title": title[:120] + ("..." if len(title) > 120 else ""),
            "desc": " · ".join(parts),
            "price": a.get("price_display", ""),
            "was": was_display,
            "hasLivePrice": bool(a.get("price_display")),
            "pct": pct,
            "effectivePct": effective_pct,
            "hot": effective_pct >= HOT_DEAL_PCT,
            "discount": f"{pct}% off" if pct > 0 else (coupon["display"] if coupon else "Deal"),
            "hasCoupon": coupon is not None,
            "couponDisplay": coupon["display"] if coupon else None,
            "image": a.get("image", ""),
            "prime": bool(a.get("prime")),
            "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
            "updatedAt": iso_now(),
        })
        deal_id += 1

    print(f"build_formatted_deals returned: {len(formatted)}")
    print(f"Skipped for missing/short title: {skipped_no_title}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"], d["title"]))
    return formatted


# =========================
# MAIN
# =========================

def build_deals_json() -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] Starting deal fetch...")

    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")

    memory = load_memory()
    memory = prune_memory(memory, DEAL_TTL_HOURS)

    asins = fetch_keepa_asins()
    if not asins:
        output = {
            "updatedAt": iso_now(),
            "totalDeals": 0,
            "hotDeals": 0,
            "couponDeals": 0,
            "deals": [],
        }
        save_json_file(OUTPUT_FILE, output)
        save_memory(memory)
        print("No ASINs returned. Saved empty deals.json.")
        return

    keepa_products = fetch_keepa_product_details(asins)
    keepa_deals = build_keepa_deal_map(keepa_products)

    filtered_keepa_deals = {}
    new_count = 0
    for asin, info in keepa_deals.items():
        existing = memory.get(asin)
        if existing:
            filtered_keepa_deals[asin] = info
        else:
            filtered_keepa_deals[asin] = info
            new_count += 1

    print(f"\n{new_count} new deals found this run")
    print(f"keepa_deals count: {len(keepa_deals)}")
    print(f"filtered_keepa_deals count: {len(filtered_keepa_deals)}")
    print("\nFetching live prices from Amazon PA API...")

    amazon_data: Dict[str, Dict[str, Any]] = {}
    qualifying_asins = list(filtered_keepa_deals.keys())

    for i in range(0, len(qualifying_asins), PA_API_BATCH_SIZE):
        batch = qualifying_asins[i:i + PA_API_BATCH_SIZE]
        result = fetch_amazon_live_data(batch)
        amazon_data.update(result)
        print(f"Fetched live data for batch {math.floor(i / PA_API_BATCH_SIZE) + 1}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"amazon_data count: {len(amazon_data)}")

    formatted = build_formatted_deals(filtered_keepa_deals, amazon_data)

    print(f"\nFinal qualifying deals before cap: {len(formatted)}")

    if DEALS_TO_SHOW > 0:
        formatted = formatted[:DEALS_TO_SHOW]

    print(f"Final deals after cap: {len(formatted)}")

    memory = update_memory(memory, formatted)
    memory = prune_memory(memory, DEAL_TTL_HOURS)
    save_memory(memory)

    output = {
        "updatedAt": iso_now(),
        "totalDeals": len(formatted),
        "hotDeals": sum(1 for d in formatted if d["hot"]),
        "couponDeals": sum(1 for d in formatted if d["hasCoupon"]),
        "deals": formatted,
    }

    save_json_file(OUTPUT_FILE, output)

    print(f"\nSaved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"Hot deals: {output['hotDeals']}")
    print(f"Coupon deals: {output['couponDeals']}")
    print(f"Updated: {output['updatedAt']}")


if __name__ == "__main__":
    build_deals_json()
