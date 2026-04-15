"""
Keepa + Amazon Creators API — Deal Price Scraper
-------------------------------------------------
Pulls price drop deals from Keepa, enriches them with
real-time pricing from the Amazon Creators API,
and saves the results to deals.json for your website.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
import numpy as np
import keepa
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

# ─────────────────────────────────────────────
# CREDENTIALS — read from environment variables
# ─────────────────────────────────────────────
KEEPA_API_KEY     = os.getenv("KEEPA_API_KEY")
CREDENTIAL_ID     = os.getenv("CREATORS_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
PARTNER_TAG       = os.getenv("AFFILIATE_TAG", "simplewoodsho-20")

if not KEEPA_API_KEY:
    raise RuntimeError("Missing KEEPA_API_KEY")
if not CREDENTIAL_ID or not CREDENTIAL_SECRET:
    raise RuntimeError("Missing CREATORS_CREDENTIAL_ID or CREATORS_CREDENTIAL_SECRET")

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
OUTPUT_FILE       = "deals.json"
MEMORY_FILE       = "deals_memory.json"
FETCH_ASINS       = 280
MAX_DISPLAY       = 1000
DEAL_TTL_HOURS    = 24
AMAZON_BATCH_SIZE = 10
MIN_DISCOUNT_PCT  = 10

# Only pull from these Keepa category IDs
INCLUDED_CATEGORIES = [
    2619525011,   # Appliances
    2617941011,   # Arts, Crafts & Sewing
    15684181,     # Automotive
    165796011,    # Baby Products
    2335752011,   # Cell Phones & Accessories
    172282,       # Electronics
    10272111,     # Everything Else
    11260432011,  # Handmade Products
    3760901,      # Health & Household
    1055398,      # Home & Kitchen
    16310091,     # Industrial & Scientific
    11091801,     # Musical Instruments
    1064954,      # Office Products
    2972638011,   # Patio, Lawn & Garden
    2619533011,   # Pet Supplies
    3375251,      # Sports & Outdoors
    228013,       # Tools & Home Improvement
    165793011,    # Toys & Games
]

# Explicitly exclude these Keepa category IDs
EXCLUDED_CATEGORIES = [
    283155,       # Books
]

# Still filter out at Amazon category name level as safety net
EXCLUDED_CATEGORY_NAMES = [
    "apparel", "clothing", "shoes", "shoe", "jewelry", "jewellery",
    "luggage", "handbag", "wallet", "fashion", "dress", "shirt",
    "pants", "jeans", "sneaker", "boot", "sandal",
    "book", "books", "textbook", "novel", "literature",
]


# ─────────────────────────────────────────────
# MEMORY: Load and save deal history
# ─────────────────────────────────────────────
def load_memory():
    if not os.path.exists(MEMORY_FILE):
        return {}
    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_memory(memory):
    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(memory, f, indent=2)


def purge_expired(memory):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=DEAL_TTL_HOURS)
    now_str = datetime.now(timezone.utc).isoformat()
    before = len(memory)
    memory = {
        asin: deal for asin, deal in memory.items()
        if datetime.fromisoformat(deal.get("seen_at", now_str)) > cutoff
    }
    purged = before - len(memory)
    if purged:
        print(f"    Purged {purged} expired deals (older than {DEAL_TTL_HOURS}h).")
    return memory


# ─────────────────────────────────────────────
# STEP 1: Pull price drop ASINs + price history
# ─────────────────────────────────────────────
def get_keepa_deals(api_key, fetch_asins):
    print("\n[1/3] Fetching price drops from Keepa...")
    api = keepa.Keepa(api_key)
    print(f"    Keepa tokens available: {api.tokens_left}")

    product_params = {
    "sort":                        [["deltaPercent7_AMAZON", "asc"]],
    "productType":                 [0],
    "deltaPercent7_AMAZON_lte":    -10,
    "current_AMAZON_gte":          1,
    "current_COUNT_REVIEWS_gte":   15,
    "current_RATING_gte":          40,
    "categories_include":          INCLUDED_CATEGORIES,
    "categories_exclude":          EXCLUDED_CATEGORIES,
    "availabilityAmazon":          [0],
}

    try:
        asins = api.product_finder(product_params, n_products=fetch_asins)
        asins = list(asins[:fetch_asins])
        print(f"    Found {len(asins)} price drop ASINs.")
    except Exception as e:
        print(f"    product_finder failed: {e}")
        asins = []

    if not asins:
        try:
            print("    Trying deal finder fallback...")
            deal_response = api.deals({"page": 0, "domainId": 1})
            asins = list(deal_response.get("asinList", []))[:fetch_asins]
            print(f"    Found {len(asins)} deal ASINs.")
        except Exception as e:
            print(f"    Deal finder also failed: {e}")
            return [], {}

    # Pull Keepa price history in batches of 10
    print(f"    Fetching Keepa price history for {len(asins)} ASINs...")
    keepa_prices = {}

    for i in range(0, len(asins), 10):
        batch = asins[i:i + 10]
        try:
            products = api.query(batch, stats=90, history=False)
            for product in products:
                asin = product.get("asin")
                if not asin:
                    continue

                stats = product.get("stats", {})

                current_raw = stats.get("current", [None] * 10)
                current_price = None
                if isinstance(current_raw, list) and len(current_raw) > 0:
                    val = current_raw[0]
                    if val and val > 0:
                        current_price = val / 100.0

                high_raw = stats.get("max90", [None] * 10)
                high_price = None
                if isinstance(high_raw, list) and len(high_raw) > 0:
                    val = high_raw[0]
                    if val and val > 0:
                        high_price = val / 100.0

                avg_raw = stats.get("avg90", [None] * 10)
                avg_price = None
                if isinstance(avg_raw, list) and len(avg_raw) > 0:
                    val = avg_raw[0]
                    if val and val > 0:
                        avg_price = val / 100.0

                keepa_prices[asin] = {
                    "current":  current_price,
                    "high_90d": high_price,
                    "avg_90d":  avg_price,
                }
        except Exception as e:
            print(f"    Warning: Keepa history batch failed — {e}")
        time.sleep(0.5)

    print(f"    Got price history for {len(keepa_prices)} ASINs.")
    return asins, keepa_prices


# ─────────────────────────────────────────────
# STEP 2: Pull pricing from Amazon Creators API
# ─────────────────────────────────────────────
def get_amazon_pricing(asins, credential_id, credential_secret, partner_tag):
    print("\n[2/3] Fetching pricing from Amazon Creators API...")

    amazon = AmazonCreatorsApi(
        credential_id=credential_id,
        credential_secret=credential_secret,
        version="3.1",
        tag=partner_tag,
        country=Country.US,
    )

    resources = [
        GetItemsResource.ITEM_INFO_DOT_TITLE,
        GetItemsResource.ITEM_INFO_DOT_BY_LINE_INFO,
        GetItemsResource.ITEM_INFO_DOT_CLASSIFICATIONS,
        GetItemsResource.IMAGES_DOT_PRIMARY_DOT_LARGE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_PRICE,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_AVAILABILITY,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_CONDITION,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_IS_BUY_BOX_WINNER,
        GetItemsResource.OFFERS_V2_DOT_LISTINGS_DOT_DEAL_DETAILS,
    ]

    all_items = {}
    for i in range(0, len(asins), AMAZON_BATCH_SIZE):
        batch = asins[i:i + AMAZON_BATCH_SIZE]
        print(f"    Fetching batch {i // AMAZON_BATCH_SIZE + 1} ({len(batch)} items)...")
        try:
            items = amazon.get_items(batch, resources=resources)
            for item in items:
                all_items[item.asin] = item
        except Exception as e:
            print(f"    Warning: batch failed — {e}")
        time.sleep(1)

    print(f"    Retrieved pricing for {len(all_items)} items.")
    return all_items


# ─────────────────────────────────────────────
# HELPER: Check if a category should be excluded
# ─────────────────────────────────────────────
def is_excluded_category(category):
    if not category:
        return False
    cat_lower = category.lower()
    return any(word in cat_lower for word in EXCLUDED_CATEGORY_NAMES)


# ─────────────────────────────────────────────
# HELPER: Calculate % off and was price
# ─────────────────────────────────────────────
def calculate_discount(current_price, keepa_data):
    if not current_price or not keepa_data:
        return None, None, ""

    was_price = keepa_data.get("high_90d") or keepa_data.get("avg_90d")

    if not was_price or was_price <= current_price:
        return None, None, ""

    pct_off = round(((was_price - current_price) / was_price) * 100)

    if pct_off < MIN_DISCOUNT_PCT:
        return None, None, ""

    was_display    = f"${was_price:.2f}"
    discount_label = f"-{pct_off}%"

    return was_display, pct_off, discount_label


# ─────────────────────────────────────────────
# STEP 3: Build deals and merge with memory
# ─────────────────────────────────────────────
def build_and_merge(asins, amazon_items, keepa_prices, memory):
    print("\n[3/3] Building and merging deals...")
    now = datetime.now(timezone.utc).isoformat()
    new_count = 0

    for asin in asins:
        item = amazon_items.get(asin)
        if not item:
            continue

        try:
            title = item.item_info.title.display_value
        except:
            title = None

        try:
            brand = item.item_info.by_line_info.brand.display_value
        except:
            brand = None

        try:
            category = item.item_info.classifications.product_group.display_value
        except:
            category = None

        if is_excluded_category(category):
            print(f"    Skipping {asin} — excluded category: {category}")
            continue

        try:
            image = item.images.primary.large.url
        except:
            image = None

        try:
            listing       = item.offers_v2.listings[0]
            price_amount  = listing.price.money.amount
            price_display = listing.price.money.display_amount
            currency      = listing.price.money.currency
        except:
            listing       = None
            price_amount  = None
            price_display = None
            currency      = None

        # Skip used items
        try:
            condition = listing.condition.value
            if condition and condition.lower() != "new":
                print(f"    Skipping {asin} — condition: {condition}")
                continue
        except:
            pass

        try:
            availability = listing.availability.type
        except:
            availability = None

        try:
            deal_type = listing.deal_details.access_type
        except:
            deal_type = "PRICE_DROP"

        try:
            url = item.detail_page_url
        except:
            url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

        keepa_data = keepa_prices.get(asin, {})
        was_display, pct_off, discount_label = calculate_discount(
            price_amount, keepa_data
        )

        if not pct_off:
            try:
                pct_off        = listing.price.savings.percentage
                was_display    = None
                discount_label = f"-{pct_off}%" if pct_off else ""
            except:
                pct_off        = None
                discount_label = ""

        is_hot = bool(pct_off and pct_off >= 30)

        deal = {
            "asin":          asin,
            "title":         title,
            "brand":         brand,
            "cat":           category,
            "image":         image,
            "price":         price_display,
            "price_amount":  price_amount,
            "currency":      currency,
            "was":           was_display,
            "savings":       was_display,
            "pct":           pct_off,
            "discount":      discount_label,
            "deal_type":     deal_type,
            "availability":  availability,
            "link":          url,
            "hot":           is_hot,
            "hasCoupon":     False,
            "couponDisplay": "",
            "desc":          brand or "",
            "seen_at":       memory.get(asin, {}).get("seen_at", now),
            "updated_at":    now,
        }

        if asin not in memory:
            new_count += 1

        memory[asin] = deal

    print(f"    {new_count} new deals added to memory.")
    return memory


def main():
    print("=" * 55)
    print("  Keepa + Amazon Creators API — Deal Price Scraper")
    print("=" * 55)

    memory = load_memory()
    print(f"\n    Memory: {len(memory)} deals before purge.")
    memory = purge_expired(memory)
    print(f"    Memory: {len(memory)} deals after purge.")

    asins, keepa_prices = get_keepa_deals(KEEPA_API_KEY, FETCH_ASINS)

    if not asins:
        print("No ASINs found from Keepa.")
    else:
        new_asins = [a for a in asins if a not in memory]
        print(f"\n    {len(new_asins)} new ASINs to price-check "
              f"({len(asins) - len(new_asins)} already cached).")

        if new_asins:
            amazon_items = get_amazon_pricing(
                new_asins, CREDENTIAL_ID, CREDENTIAL_SECRET, PARTNER_TAG
            )
            memory = build_and_merge(
                new_asins, amazon_items, keepa_prices, memory
            )
        else:
            print("    All ASINs already in memory — skipping Amazon API calls.")

    save_memory(memory)

    all_deals = sorted(
        memory.values(),
        key=lambda d: d.get("seen_at", ""),
        reverse=True
    )[:MAX_DISPLAY]

    output = {
        "deals":       all_deals,
        "count":       len(all_deals),
        "totalDeals":  len(all_deals),
        "hotDeals":    sum(1 for d in all_deals if d.get("hot")),
        "couponDeals": 0,
        "updatedAt":   datetime.now(timezone.utc).isoformat(),
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Saved {len(all_deals)} deals to {OUTPUT_FILE}")
    print("Done.")


if __name__ == "__main__":
    main()
