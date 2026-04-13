import os
from amazon_creatorsapi import AmazonCreatorsApi, Country

CREDENTIAL_ID = os.getenv("AMAZON_CREATOR_CREDENTIAL_ID")
CREDENTIAL_SECRET = os.getenv("AMAZON_CREATOR_CREDENTIAL_SECRET")
PARTNER_TAG = os.getenv("AFFILIATE_TAG", "simplewoodsho-20")

if not CREDENTIAL_ID or not CREDENTIAL_SECRET:
    raise RuntimeError("Missing AMAZON_CREATOR_CREDENTIAL_ID or AMAZON_CREATOR_CREDENTIAL_SECRET")

api = AmazonCreatorsApi(
    credential_id=CREDENTIAL_ID,
    credential_secret=CREDENTIAL_SECRET,
    version="3.1",
    tag=PARTNER_TAG,
    country=Country.US,
)

results = api.search_items(keywords="Woodworking Sander")

if not results.items:
    print("No results found.")
else:
    for i, item in enumerate(results.items[:10], start=1):
        title = getattr(getattr(getattr(item, "item_info", None), "title", None), "display_value", "No title")
        url = getattr(item, "detail_page_url", "")
        price = "Price not available"

        try:
            listings = getattr(getattr(item, "offers_v2", None), "listings", [])
            if listings:
                money = getattr(getattr(listings[0], "price", None), "money", None)
                price = getattr(money, "display_amount", "Price not available")
        except Exception:
            pass

        print(f"[{i}] {title}")
        print(f"     Price : {price}")
        print(f"     Link  : {url}")
        print()
