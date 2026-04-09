"""
DealDrop — fetch_deals.py
Creators API transition version

- Keepa finds candidate deals
- Amazon enrichment can use Creators API or PA API
- Default provider is Creators API
"""

import json
import os
import time
import hmac
import hashlib
import datetime
import requests

# Creators API SDK
from creatorsapi_python_sdk.api_client import ApiClient
from creatorsapi_python_sdk.api.default_api import DefaultApi
from creatorsapi_python_sdk.models.get_items_request_content import GetItemsRequestContent

# ─── CONFIG ───────────────────────────────────────────────────────────────────

KEEPA_API_KEY       = os.environ.get("KEEPA_API_KEY", "")
AMAZON_ACCESS_KEY   = os.environ.get("AMAZON_ACCESS_KEY", "")
AMAZON_SECRET_KEY   = os.environ.get("AMAZON_SECRET_KEY", "")
AMAZON_PARTNER_TAG  = os.environ.get("AFFILIATE_TAG", "")
AMAZON_HOST         = "webservices.amazon.com"
AMAZON_REGION       = "us-east-1"

# Creators API credentials
CREATORS_CREDENTIAL_ID      = os.environ.get("CREATORS_CREDENTIAL_ID", "")
CREATORS_CREDENTIAL_SECRET  = os.environ.get("CREATORS_CREDENTIAL_SECRET", "")
CREATORS_CREDENTIAL_VERSION = os.environ.get("CREATORS_CREDENTIAL_VERSION", "")
CREATORS_MARKETPLACE        = os.environ.get("CREATORS_MARKETPLACE", "www.amazon.com")

# creators = preferred now
# paapi = fallback if needed
AMAZON_PROVIDER = os.environ.get("AMAZON_PROVIDER", "creators").lower()

OUTPUT_FILE        = "deals.json"
MEMORY_FILE        = "deals_memory.json"

MAX_DEALS          = 60
DEALS_TO_SHOW      = 60
MIN_DISCOUNT_PCT   = 10
HOT_DEAL_PCT       = 50
MIN_COUPON_VALUE   = 3
MIN_COUPON_PCT     = 5
DEAL_TTL_HOURS     = 24

KEEPA_BASE         = "https://api.keepa.com"

# Keepa test-safe settings
KEEPA_DEAL_DELTA_PERCENT  = 12
KEEPA_DEAL_INTERVAL       = 4320
KEEPA_DEAL_PAGES          = 1
KEEPA_MAX_CANDIDATE_ASINS = 70
KEEPA_BATCH_SIZE          = 10
KEEPA_BATCH_SLEEP_SEC     = 2.5
AMAZON_BATCH_SLEEP_SEC    = 1.0

EXCLUDED_CATEGORY_NAMES = {
    "Books",
}

EXCLUDED_TITLE_TERMS = [
    " magazine",
    " magazines",
    " paperback",
    " hardcover",
    " audiobook",
    " kindle",
    " issue ",
    " vol.",
    " volume ",
    " journal",
    " workbook",
    " textbook",
    " study guide",
    " comic",
    " comics",
    " manga",
    " novel",
]

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

# ─── MEMORY HELPERS ───────────────────────────────────────────────────────────

def load_memory():
    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_memory(memory):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(memory, f, indent=2)

def prune_memory(memory):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=DEAL_TTL_HOURS)
    pruned = {}
    for asin, item in memory.items():
        first_seen = item.get("firstSeen")
        if not first_seen:
            continue
        try:
            seen_dt = datetime.datetime.fromisoformat(first_seen.replace("Z", ""))
            if seen_dt >= cutoff:
                pruned[asin] = item
        except Exception:
            continue
    return pruned

# ─── KEEPA HELPERS ────────────────────────────────────────────────────────────

def keepa_deal_request(deal_params):
    url = f"{KEEPA_BASE}/deal"
    params = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, params=params, json=deal_params, headers=headers, timeout=60)
    print(f"    Deal status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Deal response: {r.text[:500]}")
    r.raise_for_status()
    return r.json()

def keepa_product_request(asins):
    url = f"{KEEPA_BASE}/product"
    params = {
        "key": KEEPA_API_KEY,
        "domain": 1,
        "asin": ",".join(asins),
        "history": 1,
        "rating": 0,
        "stats": 90,
    }

    r = requests.get(url, params=params, timeout=60)
    print(f"    Product status: {r.status_code}")
    if r.status_code != 200:
        print(f"    Product response: {r.text[:500]}")
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
    if any(w in title for w in ["laptop", "phone", "tablet", "camera", "headphone", "speaker", "monitor", "tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt", "shoe", "dress", "jacket", "pants", "bag", "watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title for w in ["blender", "vacuum", "mattress", "pillow", "cookware", "kitchen"]):
        return "Home & Kitchen"
    if any(w in title for w in ["protein", "vitamin", "supplement", "fitness", "yoga"]):
        return "Health & Household"
    if any(w in title for w in ["toy", "game", "lego", "puzzle", "kids"]):
        return "Toys & Games"
    if any(w in title for w in ["tool", "drill", "saw", "router", "sander", "clamp", "blade", "bit"]):
        return "Tools & Home Improvement"

    return "Electronics"

def is_excluded_product(product, category_name):
    title = f" {(product.get('title') or '').lower()} "

    if category_name in EXCLUDED_CATEGORY_NAMES:
        return True

    for term in EXCLUDED_TITLE_TERMS:
        if term in title:
            return True

    return False

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
                elif val < 0:
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

def fetch_keepa_asins():
    print("\n[Keepa] Fetching deal ASINs...")
    all_candidates = []

    for page in range(KEEPA_DEAL_PAGES):
        body = {
            "domainId": 1,
            "priceTypes": [0],
            "deltaPercent": KEEPA_DEAL_DELTA_PERCENT,
            "interval": KEEPA_DEAL_INTERVAL,
            "page": page,
        }

        try:
            data = keepa_deal_request(body)
            deals_raw = data.get("deals", {}).get("dr", [])
            page_asins = [str(d.get("asin")).strip().upper() for d in deals_raw if d.get("asin")]
            print(f"[Keepa] Page {page}: {len(page_asins)} candidates")
            all_candidates.extend(page_asins)
            time.sleep(0.5)
        except Exception as e:
            print(f"[Keepa] Deal request failed on page {page}: {e}")

    all_asins = list(dict.fromkeys(all_candidates))
    print(f"[Keepa] Got {len(all_candidates)} total candidates across pages")
    print(f"[Keepa] {len(all_asins)} unique ASINs")
    print(f"[Keepa] Limiting to first {KEEPA_MAX_CANDIDATE_ASINS} ASINs for testing")
    return all_asins[:KEEPA_MAX_CANDIDATE_ASINS]

def fetch_keepa_product_details(asins):
    if not asins:
        return []

    print(f"\n[Keepa] Fetching product details ({len(asins)} ASINs)...")
    all_products = []

    for i in range(0, len(asins), KEEPA_BATCH_SIZE):
        batch = asins[i:i+KEEPA_BATCH_SIZE]
        try:
            data = keepa_product_request(batch)
            products = data.get("products", [])
            all_products.extend(products)
            print(f"    Progress: {min(i+KEEPA_BATCH_SIZE, len(asins))}/{len(asins)} ({len(all_products)} successful)")
            time.sleep(KEEPA_BATCH_SLEEP_SEC)
        except Exception as e:
            msg = str(e)
            print(f"[Keepa] Error on batch {batch}: {msg}")
            if "429" in msg:
                print("[Keepa] Hit rate limit. Stopping product fetch early to save tokens.")
                break

        if len(all_products) >= MAX_DEALS:
            break

    print(f"[Keepa] Total: {len(all_products)} products")
    return all_products

# ─── AMAZON PROVIDER INTERFACE ────────────────────────────────────────────────

def normalize_amazon_item(
    *,
    asin: str,
    title: str = "",
    image: str = "",
    price_display: str = "",
    price_amount=None,
    currency: str = "",
    prime: bool = False,
):
    return {
        "asin": asin,
        "title": title or "",
        "image": image or "",
        "price_display": price_display or "",
        "price_amount": price_amount,
        "currency": currency or "",
        "prime": bool(prime),
    }

def fetch_amazon_live_data(asin_batch):
    provider = AMAZON_PROVIDER
    print(f"[Amazon] Provider: {provider}")

    if provider == "creators":
        return fetch_amazon_live_data_creators(asin_batch)

    return fetch_amazon_live_data_paapi(asin_batch)

# ─── CREATORS API IMPLEMENTATION ──────────────────────────────────────────────

def fetch_amazon_live_data_creators(asin_batch):
    if not CREATORS_CREDENTIAL_ID or not CREATORS_CREDENTIAL_SECRET or not CREATORS_CREDENTIAL_VERSION:
        print("[Amazon Creators API] Missing credentials — skipping.")
        return {}

    try:
        api_client = ApiClient(
            credential_id=CREATORS_CREDENTIAL_ID,
            credential_secret=CREATORS_CREDENTIAL_SECRET,
            version=CREATORS_CREDENTIAL_VERSION,
        )
        api = DefaultApi(api_client)

        resources = [
            "images.primary.medium",
            "itemInfo.title",
            "offersV2.listings.price",
            "offersV2.listings.availability",
            "offersV2.listings.condition",
            "offersV2.listings.merchantInfo",
        ]

        request_body = GetItemsRequestContent(
            partner_tag=AMAZON_PARTNER_TAG,
            item_ids=asin_batch,
            resources=resources,
        )

        response = api.get_items(
            x_marketplace=CREATORS_MARKETPLACE,
            get_items_request_content=request_body,
        )

        response_dict = response.to_dict() if hasattr(response, "to_dict") else {}
        items = ((response_dict.get("itemsResult") or {}).get("items")) or []

        result = {}

        for item in items:
            asin = item.get("asin") or ""
            if not asin:
                continue

            title = (((item.get("itemInfo") or {}).get("title") or {}).get("displayValue")) or ""

            image = ""
            images = item.get("images") or {}
            primary = images.get("primary") or {}
            medium = primary.get("medium") or {}
            large = primary.get("large") or {}
            image = medium.get("url") or large.get("url") or ""

            offers_v2 = item.get("offersV2") or {}
            listings = offers_v2.get("listings") or []

            price_display = ""
            price_amount = None
            currency = ""
            prime = False

            if listings:
                listing = listings[0] or {}
                price_obj = listing.get("price") or {}

                price_display = price_obj.get("displayAmount") or ""
                price_amount = price_obj.get("amount")
                currency = price_obj.get("currency") or ""

                availability = listing.get("availability") or {}
                availability_type = availability.get("type") or ""
                prime = "prime" in str(availability_type).lower()

            result[asin] = normalize_amazon_item(
                asin=asin,
                title=title,
                image=image,
                price_display=price_display,
                price_amount=price_amount,
                currency=currency,
                prime=prime,
            )

        print(f"[Amazon Creators API] Got data for {len(result)} products")
        return result

    except Exception as e:
        print(f"[Amazon Creators API] ERROR: {e}")
        return {}

# ─── PA API FALLBACK ──────────────────────────────────────────────────────────

def sign_aws(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_aws_signing_key(secret, date_stamp, region, service):
    k = sign_aws(("AWS4" + secret).encode("utf-8"), date_stamp)
    k = sign_aws(k, region)
    k = sign_aws(k, service)
    k = sign_aws(k, "aws4_request")
    return k

def fetch_amazon_live_data_paapi(asin_batch):
    if not AMAZON_ACCESS_KEY:
        print("[Amazon PA API] Not configured — skipping.")
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
            "Offers.Summaries.LowestPrice",
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
        r = requests.post(endpoint, headers=headers, data=body, timeout=20)
        r.raise_for_status()
        items = r.json().get("ItemsResult", {}).get("Items", [])
        result = {}

        for item in items:
            asin = item.get("ASIN")
            listing = (item.get("Offers", {}).get("Listings") or [{}])[0]
            price_obj = listing.get("Price", {}) or {}
            summaries = item.get("Offers", {}).get("Summaries") or []
            lowest_price_obj = summaries[0].get("LowestPrice", {}) if summaries else {}

            result[asin] = normalize_amazon_item(
                asin=asin,
                title=item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue", ""),
                image=item.get("Images", {}).get("Primary", {}).get("Large", {}).get("URL", ""),
                price_display=price_obj.get("DisplayAmount") or lowest_price_obj.get("DisplayAmount", ""),
                price_amount=price_obj.get("Amount") or lowest_price_obj.get("Amount"),
                currency=price_obj.get("Currency") or lowest_price_obj.get("Currency"),
                prime=listing.get("DeliveryInfo", {}).get("IsPrimeEligible", False),
            )

        print(f"[Amazon PA API] Got data for {len(result)} products")
        return result

    except Exception as e:
        print(f"[Amazon PA API] ERROR: {e}")
        return {}

# ─── BUILD deals.json ─────────────────────────────────────────────────────────

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop deal fetch...\n")

    if not KEEPA_API_KEY:
        raise RuntimeError("Missing KEEPA_API_KEY")

    memory = prune_memory(load_memory())

    all_asins = fetch_keepa_asins()
    if not all_asins:
        print("No ASINs returned. Keeping existing deals.json.")
        return

    keepa_products = fetch_keepa_product_details(all_asins)

    keepa_deals = {}
    for p in keepa_products:
        try:
            asin = p.get("asin", "")
            stats = p.get("stats", {})
            cur_raw = stats.get("current", [])
            avg_raw = stats.get("avg90", [])

            def to_d(v):
                return v / 100.0 if v and v > 0 else None

            current = to_d(cur_raw[0] if cur_raw and len(cur_raw) > 0 else None)
            avg90 = to_d(avg_raw[0] if avg_raw and len(avg_raw) > 0 else None)
            coupon = parse_coupon(p)

            pct = 0
            if current and avg90 and avg90 > 0 and current < avg90:
                pct = round((1 - current / avg90) * 100)

            if pct < MIN_DISCOUNT_PCT and coupon is None:
                continue

            category_name = get_category(p)
            if is_excluded_product(p, category_name):
                continue

            keepa_deals[asin] = {
                "asin": asin,
                "category": category_name,
                "pct": pct,
                "coupon": coupon,
                "title_fallback": (p.get("title") or "")[:120],
                "avg90_price": avg90,
            }
        except Exception as e:
            print(f"Skipping product: {e}")

    qualifying_asins = list(keepa_deals.keys())
    print(f"\n{len(qualifying_asins)} qualifying deals")

    amazon_data = {}
    for i in range(0, len(qualifying_asins), 10):
        batch = qualifying_asins[i:i+10]
        amazon_data.update(fetch_amazon_live_data(batch))
        time.sleep(AMAZON_BATCH_SLEEP_SEC)

    formatted = []
    deal_id = 1
    now_iso = datetime.datetime.utcnow().isoformat() + "Z"

    for asin in qualifying_asins:
        try:
            k = keepa_deals[asin]
            a = amazon_data.get(asin, {})

            title = a.get("title") or k["title_fallback"]
            if not title or len(title) < 5:
                continue

            price = (a.get("price_display") or "").strip()
            price_amount = a.get("price_amount")
            currency = a.get("currency")

            if not price and price_amount is not None:
                try:
                    if currency == "USD":
                        price = f"${float(price_amount):.2f}"
                    else:
                        price = f"{float(price_amount):.2f} {currency}" if currency else f"{float(price_amount):.2f}"
                except Exception:
                    pass

            has_live_price = bool((a.get("price_display") or "").strip() or price_amount is not None)

            if not price:
                price = "See price on Amazon"

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

            was_display = f"${k['avg90_price']:.2f}" if k.get("avg90_price") else ""

            deal = {
                "id": deal_id,
                "asin": asin,
                "cat": cat,
                "emoji": CATEGORY_EMOJI.get(cat, "🛒"),
                "title": title[:120] + ("..." if len(title) > 120 else ""),
                "desc": " · ".join(parts),
                "price": price,
                "was": was_display,
                "hasLivePrice": has_live_price,
                "pct": pct,
                "effectivePct": effective_pct,
                "hot": effective_pct >= HOT_DEAL_PCT,
                "discount": f"{pct}% off" if pct > 0 else (coupon["display"] if coupon else "Deal"),
                "hasCoupon": coupon is not None,
                "couponDisplay": coupon["display"] if coupon else None,
                "image": image,
                "prime": prime,
                "link": f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt": now_iso,
            }

            formatted.append(deal)

            existing = memory.get(asin, {})
            memory[asin] = {
                **deal,
                "firstSeen": existing.get("firstSeen", now_iso),
            }

            deal_id += 1

        except Exception as e:
            print(f"Skipping formatted deal {asin}: {e}")

    formatted.sort(key=lambda d: (not d["hot"], -d["effectivePct"]))
    formatted = formatted[:DEALS_TO_SHOW]

    print(f"\nFinal qualifying deals before save: {len(formatted)}")

    if len(formatted) == 0:
        print("No formatted deals found. Keeping existing deals.json and not overwriting.")
        return

    save_memory(prune_memory(memory))

    output = {
        "updatedAt": now_iso,
        "totalDeals": len(formatted),
        "hotDeals": sum(1 for d in formatted if d["hot"]),
        "couponDeals": sum(1 for d in formatted if d["hasCoupon"]),
        "deals": formatted,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"Hot deals:    {output['hotDeals']}")
    print(f"Coupon deals: {output['couponDeals']}")
    print(f"Updated:      {output['updatedAt']}")

if __name__ == "__main__":
    build_deals_json()
