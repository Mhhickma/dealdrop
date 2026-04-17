"""
Keepa + Amazon Creators API — Deal Price Scraper
-------------------------------------------------
A deal = current price is at least 10% below the 30-day average price.
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
import keepa
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

# ─────────────────────────────────────────────
# CREDENTIALS — read from environment variables
# ─────────────────────────────────────────────
KEEPA_API_KEY     = os.getenv("KEEPA_API_KEY")
CREDENTIAL_ID     = os.getenv("CREATORS_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("CREATORS_CREDENTIAL_SECRET")
PARTNER_TAG       = os.getenv("AFFILIATE_TAG", "sawdustsavings-20")

if not KEEPA_API_KEY:
    raise RuntimeError("Missing KEEPA_API_KEY")
if not CREDENTIAL_ID or not CREDENTIAL_SECRET:
    raise RuntimeError("Missing CREATORS_CREDENTIAL_ID or CREATORS_CREDENTIAL_SECRET")

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
OUTPUT_FILE         = "deals.json"
MEMORY_FILE         = "deals_memory.json"
MAX_NEW_ASINS       = 50     # max new ASINs per run (fits within 5 tokens/min plan)
MIN_KEEPA_TOKENS    = 20     # stop mid-run if tokens drop below this
MAX_DISPLAY         = 1000
DEAL_TTL_HOURS      = 24
AMAZON_BATCH_SIZE   = 10
MIN_DISCOUNT_PCT    = 10     # current price must be at least this % below 30d avg

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
    1036592,      # Clothing, Shoes & Jewelry
    7141123011,   # Clothing & Fashion
    679337011,    # Shoes
    2476901011,   # Luggage & Travel Gear
    1040660,      # Apparel (top-level)
    2015765011,   # Boys' Clothing
    2015766011,   # Girls' Clothing
    1044694,      # Men's Clothing
    1045024,      # Women's Clothing
    9056981011,   # Baby & Toddler Clothing
    2209219011,   # Novelty & More Clothing
    7147440011,   # School Uniforms
    3880881,      # Costumes & Accessories
]

# Filter out at Amazon category name level as safety net
EXCLUDED_CATEGORY_NAMES = [
    "apparel", "clothing", "shoes", "shoe", "jewelry", "jewellery",
    "luggage", "handbag", "wallet", "fashion", "dress", "shirt",
    "pants", "jeans", "sneaker", "boot", "sandal",
    "book", "books", "textbook", "novel", "literature",
]

# Hardcoded ASIN blacklist
BLACKLISTED_ASINS = {
    "B0CNSFQ988",
    "B0CNSDDJ1C",
    "B0CNSDNT27",
    "B0CNSCN4KW",
    "B0CNSCZQ1W",
    "B0CNSBX4ZK",
}

# ─────────────────────────────────────────────
# CATEGORY NORMALIZATION
# ─────────────────────────────────────────────
CATEGORY_MAP = {
    "health and beauty":            "Health & Household",
    "health & beauty":              "Health & Household",
    "beauty":                       "Health & Household",
    "personal care":                "Health & Household",
    "drugstore":                    "Health & Household",
    "grocery":                      "Health & Household",
    "vitamins":                     "Health & Household",
    "supplement":                   "Health & Household",
    "medical":                      "Health & Household",
    "health":                       "Health & Household",
    "personal computers":           "Electronics",
    "camera & photo":               "Electronics",
    "cameras & photo":              "Electronics",
    "consumer electronics":         "Electronics",
    "computers":                    "Electronics",
    "computer":                     "Electronics",
    "television":                   "Electronics",
    "tv":                           "Electronics",
    "audio":                        "Electronics",
    "headphone":                    "Electronics",
    "speaker":                      "Electronics",
    "wearable":                     "Electronics",
    "tablet":                       "Electronics",
    "laptop":                       "Electronics",
    "printer":                      "Electronics",
    "monitor":                      "Electronics",
    "projector":                    "Electronics",
    "wireless":                     "Cell Phones & Accessories",
    "cell phone":                   "Cell Phones & Accessories",
    "mobile phone":                 "Cell Phones & Accessories",
    "smartphone":                   "Cell Phones & Accessories",
    "kitchen":                      "Home & Kitchen",
    "home":                         "Home & Kitchen",
    "bedding":                      "Home & Kitchen",
    "bath":                         "Home & Kitchen",
    "furniture":                    "Home & Kitchen",
    "lighting":                     "Home & Kitchen",
    "storage":                      "Home & Kitchen",
    "vacuum":                       "Home & Kitchen",
    "appliance":                    "Home & Kitchen",
    "cookware":                     "Home & Kitchen",
    "dining":                       "Home & Kitchen",
    "garden & outdoor":             "Patio, Lawn & Garden",
    "outdoor living":               "Patio, Lawn & Garden",
    "patio":                        "Patio, Lawn & Garden",
    "lawn":                         "Patio, Lawn & Garden",
    "garden":                       "Patio, Lawn & Garden",
    "outdoor":                      "Patio, Lawn & Garden",
    "toy":                          "Toys & Games",
    "game":                         "Toys & Games",
    "puzzle":                       "Toys & Games",
    "kids":                         "Toys & Games",
    "children":                     "Toys & Games",
    "sport":                        "Sports & Outdoors",
    "outdoor recreation":           "Sports & Outdoors",
    "exercise":                     "Sports & Outdoors",
    "fitness":                      "Sports & Outdoors",
    "cycling":                      "Sports & Outdoors",
    "hiking":                       "Sports & Outdoors",
    "camping":                      "Sports & Outdoors",
    "hunting":                      "Sports & Outdoors",
    "fishing":                      "Sports & Outdoors",
    "golf":                         "Sports & Outdoors",
    "automotive parts":             "Automotive",
    "vehicle":                      "Automotive",
    "car":                          "Automotive",
    "truck":                        "Automotive",
    "motorcycle":                   "Automotive",
    "auto":                         "Automotive",
    "office":                       "Office Products",
    "stationery":                   "Office Products",
    "school supplies":              "Office Products",
    "baby":                         "Baby Products",
    "infant":                       "Baby Products",
    "toddler":                      "Baby Products",
    "musical":                      "Musical Instruments",
    "instrument":                   "Musical Instruments",
    "guitar":                       "Musical Instruments",
    "piano":                        "Musical Instruments",
    "drum":                         "Musical Instruments",
    "pet":                          "Pet Supplies",
    "dog":                          "Pet Supplies",
    "cat supplies":                 "Pet Supplies",
    "aquarium":                     "Pet Supplies",
    "bird":                         "Pet Supplies",
    "tool":                         "Tools & Home Improvement",
    "hardware":                     "Tools & Home Improvement",
    "power tool":                   "Tools & Home Improvement",
    "hand tool":                    "Tools & Home Improvement",
    "home improvement":             "Tools & Home Improvement",
    "building":                     "Tools & Home Improvement",
    "paint":                        "Tools & Home Improvement",
    "plumbing":                     "Tools & Home Improvement",
    "electrical":                   "Tools & Home Improvement",
    "craft":                        "Arts, Crafts & Sewing",
    "sewing":                       "Arts, Crafts & Sewing",
    "art supply":                   "Arts, Crafts & Sewing",
    "drawing":                      "Arts, Crafts & Sewing",
    "knitting":                     "Arts, Crafts & Sewing",
    "scrapbook":                    "Arts, Crafts & Sewing",
    "industrial":                   "Industrial & Scientific",
    "scientific":                   "Industrial & Scientific",
    "laboratory":                   "Industrial & Scientific",
    "janitorial":                   "Industrial & Scientific",
    "safety":                       "Industrial & Scientific",
    "handmade":                     "Handmade Products",
    "large appliance":              "Appliances",
    "small appliance":              "Appliances",
    "washer":                       "Appliances",
    "dryer":                        "Appliances",
    "refrigerator":                 "Appliances",
    "dishwasher":                   "Appliances",
    "microwave":                    "Appliances",
    "air conditioner":              "Appliances",
}

KNOWN_CATEGORIES = {
    "appliances", "arts, crafts & sewing", "automotive",
    "baby products", "cell phones & accessories", "electronics",
    "everything else", "handmade products", "health & household",
    "home & kitchen", "industrial & scientific", "musical instruments",
    "office products", "patio, lawn & garden", "pet supplies",
    "sports & outdoors", "tools & home improvement", "toys & games",
}


def normalize_category(raw_cat):
    if not raw_cat:
        return "Everything Else"
    if raw_cat.lower() in KNOWN_CATEGORIES:
        return raw_cat
    lower = raw_cat.lower()
    for key, mapped in CATEGORY_MAP.items():
        if key in lower:
            return mapped
    print(f"    [UNMAPPED CAT] '{raw_cat}' -> Everything Else")
    return "Everything Else"


# ─────────────────────────────────────────────
# MEMORY
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
# STEP 1: Pull ASINs from Keepa
# ─────────────────────────────────────────────
def get_keepa_deals(api_key, cached_asins):
    print("\n[1/3] Fetching price drops from Keepa...")
    api = keepa.Keepa(api_key)
    tokens_before = api.tokens_left
    print(f"    Keepa tokens available: {tokens_before}")

    base_params = {
        "productType":               [0],
        "deltaPercent7_AMAZON_lte":  -10,
        "current_AMAZON_gte":        1,
        "current_COUNT_REVIEWS_gte": 15,
        "current_RATING_gte":        40,
        "categories_include":        INCLUDED_CATEGORIES,
        "categories_exclude":        EXCLUDED_CATEGORIES,
        "availabilityAmazon":        [0],
    }

    sort_strategies = [
        ("deltaPercent7_AMAZON",  "asc",  "7-day price drop"),
        ("deltaPercent30_AMAZON", "asc",  "30-day price drop"),
        ("current_COUNT_REVIEWS", "desc", "most reviewed"),
        ("current_RATING",        "desc", "highest rated"),
    ]

    seen = set(BLACKLISTED_ASINS)
    asins = []

    for sort_field, sort_dir, label in sort_strategies:
        params = {**base_params, "sort": [[sort_field, sort_dir]]}
        try:
            batch = api.product_finder(params, n_products=50)
            batch = [a for a in batch if a not in seen]
            seen.update(batch)
            asins.extend(batch)
            print(f"    [{label}] -> {len(batch)} unique ASINs")
        except Exception as e:
            print(f"    product_finder failed ({label}): {e}")
        time.sleep(1)

    print(f"    Found {len(asins)} total unique ASINs.")

    if not asins:
        print("    No ASINs found — skipping deal fetch.")
        return [], {}

    # Only fetch history for NEW ASINs not already in memory
    new_asins = [a for a in asins if a not in cached_asins]
    cached_count = len(asins) - len(new_asins)
    print(f"    {len(new_asins)} new ASINs found ({cached_count} already cached).")

    # Cap to stay within token budget
    if len(new_asins) > MAX_NEW_ASINS:
        print(f"    Capping at {MAX_NEW_ASINS} to stay within token budget.")
        new_asins = new_asins[:MAX_NEW_ASINS]

    keepa_prices = {}

    if new_asins:
        print(f"    Fetching Keepa price history for {len(new_asins)} ASINs...")
        for i in range(0, len(new_asins), 10):

            # Stop mid-run if tokens drop too low
            if api.tokens_left < MIN_KEEPA_TOKENS:
                print(f"    Token balance low ({api.tokens_left}) — stopping early.")
                break

            batch = new_asins[i:i + 10]
            try:
                products = api.query(batch, stats=30, history=False)
                for product in products:
                    asin = product.get("asin")
                    if not asin:
                        continue

                    stats = product.get("stats", {})

                    # Current Amazon price
                    current_price = None
                    current_raw = stats.get("current", [])
                    if isinstance(current_raw, list) and len(current_raw) > 0:
                        val = current_raw[0]
                        if val and val > 0:
                            current_price = val / 100.0

                    # 30-day average Amazon price
                    avg_30d = None
                    avg_raw = stats.get("avg30", [])
                    if isinstance(avg_raw, list) and len(avg_raw) > 0:
                        val = avg_raw[0]
                        if val and val > 0:
                            avg_30d = val / 100.0

                    keepa_prices[asin] = {
                        "current": current_price,
                        "avg_30d": avg_30d,
                    }
            except Exception as e:
                print(f"    Warning: Keepa history batch failed - {e}")
                time.sleep(5)
            time.sleep(0.5)

        print(f"    Got price history for {len(keepa_prices)} ASINs.")

    tokens_after = api.tokens_left
    print(f"    Tokens used: {tokens_before - tokens_after} (remaining: {tokens_after})")

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
            print(f"    Warning: batch failed - {e}")
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
# STEP 3: Build deals and merge with memory
# ─────────────────────────────────────────────
def build_and_merge(asins, amazon_items, keepa_prices, memory):
    print("\n[3/3] Building and merging deals...")
    now = datetime.now(timezone.utc).isoformat()
    new_count = 0
    skip_count = 0

    for asin in asins:
        item = amazon_items.get(asin)
        if not item:
            continue

        # ── Title ──
        try:
            title = item.item_info.title.display_value
        except:
            title = None

        # ── Brand ──
        try:
            brand = item.item_info.by_line_info.brand.display_value
        except:
            brand = None

        # ── Category ──
        try:
            raw_category = item.item_info.classifications.product_group.display_value
        except:
            raw_category = None

        if is_excluded_category(raw_category):
            print(f"    Skipping {asin} - excluded category: {raw_category}")
            skip_count += 1
            continue

        category = normalize_category(raw_category)

        # ── Image ──
        try:
            image = item.images.primary.large.url
        except:
            image = None

        # ── Price ──
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
                print(f"    Skipping {asin} - condition: {condition}")
                skip_count += 1
                continue
        except:
            pass

        # ── Availability ──
        try:
            availability = listing.availability.type
        except:
            availability = None

        # ── Deal type ──
        try:
            deal_type = listing.deal_details.access_type
        except:
            deal_type = "PRICE_DROP"

        # ── URL ──
        try:
            url = item.detail_page_url
        except:
            url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

        # ──────────────────────────────────────────────────────
        # CORE DEAL CHECK:
        # Current price must be at least 10% below 30-day average
        # ──────────────────────────────────────────────────────
        keepa_data = keepa_prices.get(asin, {})
        avg_30d    = keepa_data.get("avg_30d")

        if not avg_30d:
            print(f"    Skipping {asin} - no 30-day average available")
            skip_count += 1
            continue

        if not price_amount:
            print(f"    Skipping {asin} - no current price available")
            skip_count += 1
            continue

        # Skip if current price is more than 2x the 30d average (price spike junk)
        if price_amount > avg_30d * 2:
            print(f"    Skipping {asin} - price ${price_amount:.2f} is a spike vs "
                  f"30d avg ${avg_30d:.2f}")
            skip_count += 1
            continue

        if price_amount >= avg_30d * (1 - MIN_DISCOUNT_PCT / 100):
            print(f"    Skipping {asin} - price ${price_amount:.2f} not {MIN_DISCOUNT_PCT}% "
                  f"below 30d avg ${avg_30d:.2f}")
            skip_count += 1
            continue

        # Calculate discount label
        pct_off        = round(((avg_30d - price_amount) / avg_30d) * 100)
        was_display    = f"${avg_30d:.2f}"
        discount_label = f"-{pct_off}%"
        is_hot         = pct_off >= 30

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
    print(f"    {skip_count} items skipped.")
    return memory


def main():
    print("=" * 55)
    print("  Keepa + Amazon Creators API — Deal Price Scraper")
    print("=" * 55)

    memory = load_memory()
    print(f"\n    Memory: {len(memory)} deals before purge.")
    memory = purge_expired(memory)
    print(f"    Memory: {len(memory)} deals after purge.")

    cached_asins = set(memory.keys())

    asins, keepa_prices = get_keepa_deals(KEEPA_API_KEY, cached_asins)

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
            print("    All ASINs already in memory - skipping Amazon API calls.")

    save_memory(memory)

    # Sort by updated_at so freshest deals appear first
    all_deals = sorted(
        memory.values(),
        key=lambda d: d.get("updated_at", d.get("seen_at", "")),
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

    print(f"\nSaved {len(all_deals)} deals to {OUTPUT_FILE}")
    print("Done.")


if __name__ == "__main__":
    main()
