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
    raise RuntimeError("Missing AMAZON_CREATOR_CREDENTIAL_ID or AMAZON_CREATOR_CREDENTIAL_SECRET")

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────
OUTPUT_FILE       = "deals.json"   # saves to repo root for GitHub Pages
MAX_ASINS         = 10
AMAZON_BATCH_SIZE = 10


# ─────────────────────────────────────────────
# STEP 1: Pull price drop ASINs from Keepa
# ─────────────────────────────────────────────
def get_keepa_deals(api_key, max_asins):
    print("\n[1/3] Fetching price drops from Keepa...")
    api = keepa.Keepa(api_key)

    product_params = {
        "sort":        [["delta_percent", "asc"]],
        "productType": [0],
    }

    try:
        asins = api.product_finder(product_params)
        asins = list(asins[:max_asins])
        print(f"    Found {len(asins)} price drop ASINs.")
        return asins
    except Exception as e:
        print(f"    product_finder failed: {e}")

    # Fallback: deal finder
    try:
        print("    Trying deal finder fallback...")
        deal_response = api.deals({"page": 0, "domainId": 1})
        asins = list(deal_response.get("asinList", []))[:max_asins]
        print(f"    Found {len(asins)} deal ASINs.")
        return asins
    except Exception as e:
        print(f"    Deal finder also failed: {e}")
        return []


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
# STEP 3: Build and save JSON output
# ─────────────────────────────────────────────
def build_output(asins, amazon_items):
    print("\n[3/3] Building JSON output...")
    deals = []

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

        try:
            availability = listing.availability.type
        except:
            availability = None

        try:
            deal_type = listing.deal_details.access_type
        except:
            deal_type = "PRICE_DROP"

        try:
            savings_amount = listing.price.savings.money.display_amount
            savings_pct    = listing.price.savings.percentage
        except:
            savings_amount = None
            savings_pct    = None

        try:
            url = item.detail_page_url
        except:
            url = f"https://www.amazon.com/dp/{asin}?tag={PARTNER_TAG}"

        deals.append({
            "asin":          asin,
            "title":         title,
            "brand":         brand,
            "category":      category,
            "image":         image,
            "price":         price_display,
            "price_amount":  price_amount,
            "currency":      currency,
            "savings":       savings_amount,
            "savings_pct":   savings_pct,
            "deal_type":     deal_type,
            "availability":  availability,
            "affiliate_url": url,
        })

    return deals


def main():
    print("=" * 55)
    print("  Keepa + Amazon Creators API — Deal Price Scraper")
    print("=" * 55)

    asins = get_keepa_deals(KEEPA_API_KEY, MAX_ASINS)
    if not asins:
        print("No ASINs found. Exiting.")
        return

    amazon_items = get_amazon_pricing(
        asins, CREDENTIAL_ID, CREDENTIAL_SECRET, PARTNER_TAG
    )

    deals = build_output(asins, amazon_items)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"deals": deals, "count": len(deals)}, f, indent=2)

    print(f"\n✅ Saved {len(deals)} deals to {OUTPUT_FILE}")
    print("Done.")


if __name__ == "__main__":
    main()
