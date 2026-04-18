"""
Keepa Deals Endpoint + Amazon Creators API — Deal Scraper
----------------------------------------------------------
Uses Keepa's deals endpoint across all 7 price types (Buy Box, Amazon,
New, FBA, FBM, Prime, Lightning) then validates pricing via Amazon PA API.
"""

import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from amazon_creatorsapi import AmazonCreatorsApi, Country
from amazon_creatorsapi.models import GetItemsResource

# ─────────────────────────────────────────────
# CREDENTIALS
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
OUTPUT_FILE       = "deals.json"
MEMORY_FILE       = "deals_memory.json"
MAX_DISPLAY       = 1000
DEAL_TTL_HOURS    = 24
AMAZON_BATCH_SIZE = 10
MIN_DISCOUNT_PCT  = 5
KEEPA_DEALS_URL   = "https://api.keepa.com/deal"

PRICE_TYPES = [7, 0, 1, 10, 2, 13, 3]

EXCLUDED_CATEGORIES = [
    283155,       # Books
    5174,         # CDs & Vinyl
    133140011,    # Kindle Store
    2625373011,   # Movies & TV
    7141123011,   # Clothing
    163856011,    # Digital Music
    18145289011,  # Audible
    2350149011,   # Apps & Games
    2238192011,   # Gift Cards
    4991425011,   # Collectibles & Fine Art
    229534,       # Software
    18981045011,  # Amazon Luxury
    11260432011,  # Handmade Products
    16310091,     # Industrial & Scientific
]

BAD_KEYWORDS = [
    "sex", "doll", "erotic", "fetish", "penis", "vagina",
    "dildo", "vibrator", "nude", "naked", "porn", "xxx",
    "bdsm", "bondage",
    "abrasive", "torque", "fiber optic", "qsfp", "sfp",
    "evaporator", "flame retardant", "safety vest", "hard hat",
    "bearing", "set screw", "end mill", "clamp", "permaculture",
    "grass paint", "field line", "marking paint", "hydraulic",
    "pneumatic", "actuator", "splice", "scotchcast", "schuko",
    "waffle polish", "roller refill", "dental", "vapor-tight",
    "jute", "bohemian", "hinge", "barrel hinge", "mortise",
    "water pump", "latex glove", "circuit breaker",
    "conduit", "junction box", "wire connector",
]

BLACKLISTED_ASINS = {
    "B0CNSFQ988", "B0CNSDDJ1C", "B0CNSDNT27",
    "B0CNSCN4KW", "B0CNSCZQ1W", "B0CNSBX4ZK",
}

# ─────────────────────────────────────────────
# TITLE DECODER
# Keepa returns titles as int arrays in deals endpoint
# ─────────────────────────────────────────────
def decode_title(raw):
    """Keepa deal titles can be a list of ints (char codes) or a plain string."""
    if isinstance(raw, list):
        try:
            return "".join(chr(c) for c in raw if isinstance(c, int))
        except:
            return ""
    if isinstance(raw, str):
        return raw
    return ""


def is_bad_title(title):
    if not title or len(title) < 3:
        return True
    # Block foreign language titles (check first 10 chars)
    try:
        if not all(ord(c) < 128 for c in title[:10]):
            return True
    except:
        return True
    title_lower = title.lower()
    if any(w in title_lower for w in BAD_KEYWORDS):
        return True
    return False


# ─────────────────────────────────────────────
# CATEGORY NORMALIZATION
# ─────────────────────────────────────────────
CATEGORY_MAP = {
    "health": "Health & Household",
    "beauty": "Health & Household",
    "personal care": "Health & Household",
    "grocery": "Health & Household",
    "electronics": "Electronics",
    "computer": "Electronics",
    "camera": "Electronics",
    "television": "Electronics",
    "audio": "Electronics",
    "headphone": "Electronics",
    "speaker": "Electronics",
    "tablet": "Electronics",
    "laptop": "Electronics",
    "cell phone": "Cell Phones & Accessories",
    "smartphone": "Cell Phones & Accessories",
    "wireless": "Cell Phones & Accessories",
    "kitchen": "Home & Kitchen",
    "home": "Home & Kitchen",
    "bedding": "Home & Kitchen",
    "furniture": "Home & Kitchen",
    "lighting": "Home & Kitchen",
    "vacuum": "Home & Kitchen",
    "appliance": "Home & Kitchen",
    "cookware": "Home & Kitchen",
    "patio": "Patio, Lawn & Garden",
    "lawn": "Patio, Lawn & Garden",
    "garden": "Patio, Lawn & Garden",
    "outdoor": "Patio, Lawn & Garden",
    "toy": "Toys & Games",
    "game": "Toys & Games",
    "kids": "Toys & Games",
    "sport": "Sports & Outdoors",
    "fitness": "Sports & Outdoors",
    "camping": "Sports & Outdoors",
    "automotive": "Automotive",
    "vehicle": "Automotive",
    "car": "Automotive",
    "office": "Office Products",
    "baby": "Baby Products",
    "pet": "Pet Supplies",
    "dog": "Pet Supplies",
    "tool": "Tools & Home Improvement",
    "hardware": "Tools & Home Improvement",
    "home improvement": "Tools & Home Improvement",
    "craft": "Arts, Crafts & Sewing",
    "sewing": "Arts, Crafts & Sewing",
    "musical": "Musical Instruments",
}

KNOWN_CATEGORIES = {
    "appliances", "arts, crafts & sewing", "automotive",
    "baby products", "cell phones & accessories", "electronics",
    "everything else", "health & household", "home & kitchen",
    "musical instruments", "office products", "patio, lawn & garden",
    "pet supplies", "sports & outdoors", "tools & home improvement",
    "toys & games",
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
# STEP 1: Pull ASINs from Keepa Deals Endpoint
# ─────────────────────────────────────────────
def get_keepa_deals(api_key, cached_asins):
    print("\n[1/3] Fetching deals from Keepa deals endpoint...")

    all_deals = []

    for pt in PRICE_TYPES:
        payload = {
            "domainId":          1,
            "priceTypes":        [pt],
            "dateRange":         4,
            "sortType":          4,
            "page":              0,
            "filterErotic":      True,
            "hasReviews":        True,
            "minRating":         40,
            "deltaPercentRange": [-100, -MIN_DISCOUNT_PCT],
            "excludeCategories": EXCLUDED_CATEGORIES,
        }
        try:
            r = requests.post(
                KEEPA_DEALS_URL,
                params={"key": api_key, "domain": 1},
                json=payload,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            dr = data.get("deals", {}).get("dr", [])
            all_deals.extend(dr)
            print(f"    priceType {pt} -> {len(dr)} deals | tokens left: {data.get('tokensLeft', '?')}")
        except Exception as e:
            print(f"    priceType {pt} failed: {e}")
        time.sleep(1)

    # Deduplicate and filter
    seen = set(BLACKLISTED_ASINS)
    unique_asins = []

    for item in all_deals:
        asin = item.get("asin", "")
        if not asin or asin in seen:
            continue

        # Decode title (may be int array or string)
        raw_title = item.get("title", "")
        title = decode_title(raw_title)

        if is_bad_title(title):
            continue

        # Min price $10 — current is a list, index varies by price type
        prices = [x for x in item.get("current", []) if isinstance(x, (int, float)) and x > 0]
        if not prices or min(prices) < 1000:
            continue

        seen.add(asin)
        unique_asins.append(asin)

    print(f"    {len(unique_asins)} unique clean ASINs after filtering.")

    new_asins = [a for a in unique_asins if a not in cached_asins]
    print(f"    {len(new_asins)} new ASINs to fetch from Amazon "
          f"({len(unique_asins) - len(new_asins)} already cached).")

    return new_asins


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
        print(f"    Batch {i // AMAZON_BATCH_SIZE + 1} ({len(batch)} items)...")
        try:
            items = amazon.get_items(batch, resources=resources)
            for item in items:
                all_items[item.asin] = item
        except Exception as e:
            print(f"    Warning: batch failed - {e}")
        time.sleep(1)

    print(f"    Retrieved {len(all_items)} items from Amazon.")
    return all_items


# ─────────────────────────────────────────────
# STEP 3: Build deals and merge with memory
# ─────────────────────────────────────────────
def build_and_merge(asins, amazon_items, memory):
    print("\n[3/3] Building and merging deals...")
    now = datetime.now(timezone.utc).isoformat()
    new_count = 0
    skip_count = 0

    for asin in asins:
        item = amazon_items.get(asin)
        if not item:
            continue

        # Title
        try:
            title = item.item_info.title.display_value
        except:
            title = None
        if not title:
            skip_count += 1
            continue

        # Brand
        try:
            brand = item.item_info.by_line_info.brand.display_value
        except:
            brand = None

        # Category
        try:
            raw_category = item.item_info.classifications.product_group.display_value
        except:
            raw_category = None
        category = normalize_category(raw_category)

        # Image
        try:
            image = item.images.primary.large.url
        except:
            image = None

        # Price
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

        if not price_amount:
            print(f"    Skipping {asin} - no price available")
            skip_count += 1
            continue

        # Skip used items
        try:
            condition = listing.condition.value
            if condition and condition.lower() != "new":
                skip_count += 1
                continue
        except:
            pass

        # Availability
        try:
            availability = listing.availability.type
        except:
            availability = None

        # Deal type
        try:
            deal_type = listing.deal_details.access_type
        except:
            deal_type = "PRICE_DROP"

        # URL
        try:
            url = item.detail_page_url
        except:
            url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

        # Discount
        pct_off        = MIN_DISCOUNT_PCT
        was_display    = None
        discount_label = f"-{pct_off}%+"
        is_hot         = False

        try:
            savings = listing.price.savings
            if savings:
                pct_off        = round(savings.percentage)
                was_display    = f"${round(price_amount + savings.money.amount, 2)}"
                discount_label = f"-{pct_off}%"
                is_hot         = pct_off >= 30
        except:
            pass

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

    print(f"    {new_count} new deals added.")
    print(f"    {skip_count} items skipped.")
    return memory


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Keepa Deals + Amazon Creators API — DealDrop")
    print("=" * 55)

    memory = load_memory()
    print(f"\n    Memory: {len(memory)} deals before purge.")
    memory = purge_expired(memory)
    print(f"    Memory: {len(memory)} deals after purge.")

    cached_asins = set(memory.keys())

    new_asins = get_keepa_deals(KEEPA_API_KEY, cached_asins)

    if not new_asins:
        print("No new ASINs to process.")
    else:
        amazon_items = get_amazon_pricing(
            new_asins, CREDENTIAL_ID, CREDENTIAL_SECRET, PARTNER_TAG
        )
        memory = build_and_merge(new_asins, amazon_items, memory)

    save_memory(memory)

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
