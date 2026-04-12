import json
import os
import time
import requests

# =========================
# ENV VARS
# =========================
CLIENT_ID = os.getenv("CREATORS_CREDENTIAL_ID")
CLIENT_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
CREDENTIAL_VERSION = os.getenv("CREATORS_CREDENTIAL_VERSION", "2.1")
MARKETPLACE = os.getenv("CREATORS_MARKETPLACE", "www.amazon.com")
CREATOR_URL = os.getenv("CREATOR_API_URL", "https://creators-api-na.amazon.com").rstrip("/")

AFFILIATE_TAG = os.getenv("AFFILIATE_TAG")
KEEPA_API_KEY = os.getenv("KEEPA_API_KEY")

TOKEN_URL = "https://creatorsapi.auth.us-east-1.amazoncognito.com/oauth2/token"
GET_ITEMS_PATH = "/getitems"

OUTPUT_FILE = "deals.json"

MAX_DEALS = 150
MIN_DISCOUNT = 10
HOT_DEAL_PCT = 30


# =========================
# AUTH
# =========================
def get_access_token():
    print("[Auth] Getting token...")
    print(f"[Auth] Client ID present: {bool(CLIENT_ID)}")
    print(f"[Auth] Client Secret present: {bool(CLIENT_SECRET)}")

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID or "",
        "client_secret": CLIENT_SECRET or "",
        "scope": "creators::api",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    r = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=30)

    print(f"[Auth] Status: {r.status_code}")
    print(f"[Auth] Response: {r.text}")

    r.raise_for_status()

    data = r.json()
    token = data["access_token"]
    print("[Auth] Token received")
    return token


# =========================
# KEEPA DEAL FETCH
# =========================
def get_keepa_deals():
    print("[Keepa] Fetching deals...")

    url = "https://api.keepa.com/deal"
    params = {"key": KEEPA_API_KEY}

    body = {
        "domainId": 1,
        "priceTypes": [0],
        "deltaPercent": MIN_DISCOUNT,
    }

    r = requests.post(url, params=params, json=body, timeout=60)
    r.raise_for_status()

    raw = r.json().get("deals", {}).get("dr", [])

    deals = []
    seen = set()

    for d in raw:
        asin = d.get("asin")
        if not asin or asin in seen:
            continue

        seen.add(asin)

        delta = d.get("deltaPercent")
        pct = int(delta) if isinstance(delta, (int, float)) else MIN_DISCOUNT
        pct = max(MIN_DISCOUNT, pct)

        deals.append({
            "asin": asin,
            "pct": pct
        })

        if len(deals) >= MAX_DEALS:
            break

    print(f"[Keepa] Found {len(deals)} ASINs")
    return deals


# =========================
# CREATORS API
# =========================
def fetch_creator_data(asins, token):
    print("[Creator API] Fetching product data...")

    headers = {
        "Authorization": f"Bearer {token}, Version {CREDENTIAL_VERSION}",
        "Content-Type": "application/json",
        "x-marketplace": MARKETPLACE,
    }

    payload = {
        "itemIds": asins,
        "itemIdType": "ASIN",
        "languagesOfPreference": ["en_US"],
        "marketplace": MARKETPLACE,
        "partnerTag": AFFILIATE_TAG,
        "resources": [
            "images.primary.large",
            "itemInfo.title",
            "offersV2.listings.price",
            "offersV2.listings.availability"
        ]
    }

    url = f"{CREATOR_URL}{GET_ITEMS_PATH}"
    print(f"[Creator API] URL: {url}")

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    print(f"[Creator API] Status: {r.status_code}")
    if r.status_code != 200:
        print(f"[Creator API] Response: {r.text}")

    r.raise_for_status()

    data = r.json()
    items = data.get("itemResults", {}).get("items", [])

    results = {}

    for item in items:
        asin = item.get("asin")
        if not asin:
            continue

        title = (
            item.get("itemInfo", {})
                .get("title", {})
                .get("displayValue", "")
        )

        image = (
            item.get("images", {})
                .get("primary", {})
                .get("large", {})
                .get("url", "")
        )

        detail_page_url = item.get("detailPageURL", "")

        price = ""
        availability = ""

        listings = item.get("offersV2", {}).get("listings", [])
        if listings:
            first = listings[0]
            price = first.get("price", {}).get("displayAmount", "")
            availability = first.get("availability", {}).get("message", "")

        results[asin] = {
            "title": title,
            "image": image,
            "price": price,
            "availability": availability,
            "detailPageURL": detail_page_url,
        }

    priced = sum(1 for v in results.values() if v.get("price"))
    print(f"[Creator API] Got {len(results)} items, {priced} with prices")
    return results


# =========================
# BUILD DEALS
# =========================
def build_deals():
    token = get_access_token()
    keepa_deals = get_keepa_deals()

    asin_list = [d["asin"] for d in keepa_deals]
    creator_data = {}

    for i in range(0, len(asin_list), 10):
        batch = asin_list[i:i+10]
        batch_data = fetch_creator_data(batch, token)
        creator_data.update(batch_data)
        time.sleep(0.5)

    deals = []

    for d in keepa_deals:
        asin = d["asin"]
        pct = d["pct"]

        data = creator_data.get(asin)
        if not data:
            continue

        price = data.get("price", "").strip()
        if not price:
            continue

        title = data.get("title", "").strip()
        image = data.get("image", "").strip()
        detail_url = data.get("detailPageURL", "").strip()

        if not title:
            continue

        if not detail_url:
            detail_url = f"https://www.amazon.com/dp/{asin}?tag={AFFILIATE_TAG}"

        desc_parts = [f"{pct}% off recent price"]
        if data.get("availability"):
            desc_parts.append(data["availability"])

        deals.append({
            "asin": asin,
            "title": title,
            "image": image,
            "price": price,
            "link": detail_url,
            "pct": pct,
            "hot": pct >= HOT_DEAL_PCT,
            "discount": f"{pct}% off",
            "desc": " · ".join(desc_parts)
        })

        if len(deals) >= MAX_DEALS:
            break

    return deals


# =========================
# MAIN
# =========================
def main():
    print("Starting DealDrop fetch...")

    deals = build_deals()

    output = {
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totalDeals": len(deals),
        "hotDeals": len([d for d in deals if d.get("hot")]),
        "couponDeals": 0,
        "deals": deals
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Saved {len(deals)} deals to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
