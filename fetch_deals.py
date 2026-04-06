"""
DealDrop — fetch_deals.py
Uses same Keepa API call format as your working Google Sheets script.
"""

import json
import os
import time
import datetime
import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 50
MIN_DISCOUNT_PCT   = 5
HOT_DEAL_PCT       = 50
DOMAIN_ID          = "1"

CATEGORY_NAMES = {
    281052:        "Electronics",
    1055398:       "Home & Kitchen",
    7141123011:    "Clothing, Shoes & Jewelry",
    3760901:       "Luggage & Travel",
    3375251:       "Sports & Outdoors",
    165793011:     "Toys & Games",
    2619525011:    "Tools & Home Improvement",
    51574011:      "Pet Supplies",
    165796011:     "Baby",
    172282:        "Electronics",
    1064954:       "Health & Household",
    3760911:       "Beauty & Personal Care",
    2238192011:    "Musical Instruments",
    979455011:     "Garden & Outdoor",
    1285128:       "Office Products",
    468642:        "Video Games",
    283155:        "Books",
    16310101:      "Grocery & Gourmet Food",
    9482648011:    "Kitchen & Dining",
    228013:        "Industrial & Scientific",
    491244:        "Automotive",
    2619533011:    "Automotive",
    1064012:       "Sports & Outdoors",
    3737671:       "Baby",
    2350149011:    "Musical Instruments",
    11091801:      "Grocery & Gourmet Food",
    130:           "Computers",
    541966:        "Electronics",
}

CATEGORY_EMOJI = {
    "Electronics":               "💻",
    "Computers":                 "💻",
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
    "Industrial & Scientific":   "🔩",
}

# Keyword-based category fallback
KEYWORD_CATEGORIES = [
    (["hydraulic","press","lathe","drill press","bandsaw","grinder","welder","welding","compressor","generator","chain saw","chainsaw","circular saw","table saw","miter saw","jigsaw","router","sander","planer","jointer","nailer","nail gun","staple gun","impact driver","impact wrench","torque wrench","socket set","wrench set","tool set","tool box","toolbox","workbench","clamp","vise","anvil","forge","metalwork","industrial","shop press"], "Tools & Home Improvement"),
    (["car","truck","vehicle","auto","motorcycle","tire","brake","oil filter","wiper blade","floor mat","seat cover","dash cam","jump starter","battery charger","tow"], "Automotive"),
    (["laptop","computer","pc","desktop","monitor","keyboard","mouse","hard drive","ssd","ram","cpu","gpu","motherboard","router","modem","printer","scanner","webcam","microphone","headset","earbuds","airpods","speaker","tablet","ipad","kindle"], "Electronics"),
    (["phone","smartphone","iphone","android","charger","cable","power bank","case","screen protector"], "Electronics"),
    (["tv","television","projector","streaming","fire stick","roku","chromecast","remote","hdmi"], "Electronics"),
    (["camera","lens","tripod","drone","gopro","photography","video camera"], "Electronics"),
    (["shirt","pants","dress","jacket","coat","sweater","hoodie","shorts","jeans","leggings","skirt","shoes","boots","sneakers","sandals","heels","handbag","purse","wallet","belt","hat","cap","gloves","scarf","sock","underwear","bra","swimsuit","jewelry","necklace","bracelet","ring","earring","watch"], "Clothing, Shoes & Jewelry"),
    (["sofa","couch","bed","mattress","pillow","blanket","sheet","curtain","rug","lamp","chair","table","desk","shelf","bookcase","dresser","nightstand","mirror","frame","vase","candle"], "Home & Kitchen"),
    (["blender","mixer","toaster","coffee maker","keurig","air fryer","instant pot","slow cooker","rice cooker","microwave","juicer","food processor","stand mixer","waffle","griddle","pan","pot","knife","cutting board","bakeware","cookware"], "Kitchen & Dining"),
    (["vacuum","mop","broom","cleaning","detergent","laundry","dishwasher","trash","storage","organizer","hanger"], "Home & Kitchen"),
    (["vitamin","supplement","protein","probiotic","fish oil","collagen","melatonin","zinc","magnesium","medicine","first aid","bandage","thermometer","blood pressure","glucose","hearing aid","contact lens","toothbrush","dental","razor","shaver"], "Health & Household"),
    (["shampoo","conditioner","moisturizer","serum","foundation","mascara","lipstick","perfume","cologne","nail polish","hair dryer","straightener","curling iron","makeup","skincare","sunscreen","lotion","face wash"], "Beauty & Personal Care"),
    (["lego","action figure","doll","board game","puzzle","play","toy","remote control car","rc car","nerf","pokemon","hot wheels","barbie","playset"], "Toys & Games"),
    (["dog","cat","fish","bird","hamster","pet","collar","leash","crate","aquarium","bird cage","pet food","treat","litter","flea"], "Pet Supplies"),
    (["diaper","baby","infant","toddler","stroller","car seat","crib","pacifier","bottle","formula","baby monitor","high chair"], "Baby"),
    (["guitar","piano","keyboard","drum","violin","ukulele","bass","microphone","amp","amplifier","music stand","instrument"], "Musical Instruments"),
    (["tent","sleeping bag","hiking","camping","backpack","climbing","kayak","canoe","fishing","hunting","archery","golf","tennis","basketball","football","soccer","baseball","yoga mat","dumbbell","barbell","weight","treadmill","bike","bicycle","scooter","ski","snowboard"], "Sports & Outdoors"),
    (["seed","plant","soil","fertilizer","garden","hose","sprinkler","lawn mower","trimmer","hedge","rake","shovel","wheelbarrow","planter","pot","outdoor furniture","patio","grill","bbq"], "Garden & Outdoor"),
    (["notebook","pen","pencil","stapler","paper","folder","binder","desk organizer","calculator","whiteboard","printer ink","toner","office chair","filing"], "Office Products"),
    (["suitcase","luggage","travel bag","backpack","duffel","carry on","passport","travel pillow","packing cube"], "Luggage & Travel"),
    (["snack","coffee","tea","protein bar","candy","chocolate","nuts","cereal","pasta","sauce","spice","condiment","drink","juice","soda","water bottle","food"], "Grocery & Gourmet Food"),
    (["ps5","xbox","nintendo","switch","gaming","controller","headset","game"], "Video Games"),
]

def get_category(product):
    # First try Keepa's category IDs
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]

    # Fall back to keyword matching on title
    title = (product.get("title") or "").lower()
    for keywords, category in KEYWORD_CATEGORIES:
        if any(w in title for w in keywords):
            return category

    return "Home & Kitchen"

def get_price_at_time(history, minutes_ago):
    if not history or len(history) < 2:
        return -1
    last_time = history[-2]
    target_time = last_time - minutes_ago
    i = len(history) - 2
    while i >= 0:
        t = history[i]
        p = history[i + 1]
        if t <= target_time:
            return p
        i -= 2
    return history[1]

def save_empty():
    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  0, "hotDeals": 0, "couponDeals": 0, "deals": [],
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print("  Saved empty deals.json")

def fetch_products(asins):
    chunk_size = 20
    all_products = []
    for i in range(0, len(asins), chunk_size):
        chunk = asins[i:i+chunk_size]
        url = (
            f"https://api.keepa.com/product"
            f"?key={KEEPA_API_KEY}"
            f"&domain={DOMAIN_ID}"
            f"&asin={','.join(chunk)}"
            f"&stats=1"
            f"&history=1"
            f"&days=2"
        )
        print(f"    Fetching {len(chunk)} products...")
        try:
            r = requests.get(url, timeout=30)
            print(f"    Status: {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                products = data.get("products", [])
                all_products.extend(products)
                print(f"    Got {len(products)} products")
            else:
                print(f"    Error: {r.text[:200]}")
            time.sleep(1)
        except Exception as e:
            print(f"    Request failed: {e}")
    return all_products

def fetch_deal_asins():
    print("  Fetching deals from Keepa...")
    url     = "https://api.keepa.com/deal"
    params  = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}
    body    = {
        "domainId":     1,
        "priceTypes":   [0],
        "deltaPercent": MIN_DISCOUNT_PCT,
        "interval":     10080,
        "page":         0,
    }
    try:
        r = requests.post(url, params=params, json=body, headers=headers, timeout=30)
        print(f"    Status: {r.status_code}")
        if r.status_code == 200:
            data   = r.json()
            deals  = data.get("deals", {}).get("dr", [])
            asins  = [d.get("asin") for d in deals if d.get("asin")]
            print(f"    Got {len(asins)} deal ASINs")
            return asins
        else:
            print(f"    Error: {r.text[:200]}")
            return []
    except Exception as e:
        print(f"    Failed: {e}")
        return []

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop...\n")

    deal_asins = fetch_deal_asins()
    if not deal_asins:
        print("  No deal ASINs. Saving empty.")
        save_empty()
        return

    print(f"\n  Fetching details for {min(len(deal_asins), MAX_DEALS)} products...")
    products = fetch_products(deal_asins[:MAX_DEALS])
    print(f"  Total products fetched: {len(products)}")

    if not products:
        print("  No products returned. Saving empty.")
        save_empty()
        return

    formatted = []
    deal_id   = 1

    for p in products:
        try:
            asin  = p.get("asin", "")
            title = p.get("title", "")
            if not title or len(title) < 5:
                continue

            current_stats = p.get("stats", {}).get("current", [])
            current_price = -1
            price_type    = -1

            if len(current_stats) > 18 and current_stats[18] > 0:
                current_price = current_stats[18]; price_type = 18
            elif len(current_stats) > 1 and current_stats[1] > 0:
                current_price = current_stats[1];  price_type = 1
            elif len(current_stats) > 0 and current_stats[0] > 0:
                current_price = current_stats[0];  price_type = 0

            yesterday_price = -1
            csv_data        = p.get("csv", [])
            if price_type != -1 and csv_data and len(csv_data) > price_type and csv_data[price_type]:
                yesterday_price = get_price_at_time(csv_data[price_type], 24 * 60)
            if yesterday_price == -1:
                yesterday_price = current_price

            pct = 0
            if current_price > 0 and yesterday_price > 0:
                drop = (yesterday_price - current_price) / yesterday_price
                if drop > 0:
                    pct = round(drop * 100)

            if pct < MIN_DISCOUNT_PCT:
                continue

            image_url = ""
            if p.get("imagesCSV"):
                image_url = "https://images-na.ssl-images-amazon.com/images/I/" + p["imagesCSV"].split(",")[0]

            cat   = get_category(p)
            emoji = CATEGORY_EMOJI.get(cat, "🛒")

            price_display = f"${current_price/100:.2f}"   if current_price   > 0 else ""
            was_display   = f"${yesterday_price/100:.2f}" if yesterday_price > 0 else ""

            formatted.append({
                "id":            deal_id,
                "asin":          asin,
                "cat":           cat,
                "emoji":         emoji,
                "title":         title[:80] + ("..." if len(title) > 80 else ""),
                "desc":          f"{pct}% off yesterday's price",
                "price":         price_display,
                "was":           was_display,
                "hasLivePrice":  bool(price_display),
                "pct":           pct,
                "effectivePct":  pct,
                "hot":           pct >= HOT_DEAL_PCT,
                "discount":      f"{pct}% off",
                "hasCoupon":     False,
                "couponDisplay": None,
                "image":         image_url,
                "prime":         False,
                "link":          f"https://www.amazon.com/dp/{asin}?tag={AMAZON_PARTNER_TAG}",
                "updatedAt":     datetime.datetime.utcnow().isoformat() + "Z",
            })
            deal_id += 1

        except Exception as e:
            print(f"  Skipping {p.get('asin','?')}: {e}")

    formatted.sort(key=lambda d: -d["effectivePct"])
    formatted = formatted[:MAX_DEALS]

    output = {
        "updatedAt":   datetime.datetime.utcnow().isoformat() + "Z",
        "totalDeals":  len(formatted),
        "hotDeals":    sum(1 for d in formatted if d["hot"]),
        "couponDeals": 0,
        "deals":       formatted,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(formatted)} deals to {OUTPUT_FILE}")
    print(f"  Hot deals: {output['hotDeals']}")
    print(f"  Updated:   {output['updatedAt']}")

if __name__ == "__main__":
    build_deals_json()
