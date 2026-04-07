"""
DealDrop — fetch_deals.py
Hourly runs, 8 pages of deals, 300 products scanned per run.
"""

import json
import os
import time
import datetime
import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 300
MIN_DISCOUNT_PCT   = 10
HOT_DEAL_PCT       = 50
DOMAIN_ID          = "1"
DEALS_TO_SHOW      = 50

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
    228013:      "Industrial & Scientific",
    491244:      "Automotive",
    2619533011:  "Automotive",
    1064012:     "Sports & Outdoors",
    979455011:   "Patio, Lawn & Garden",
    1285128:     "Office Products",
    468642:      "Video Games",
    283155:      "Books",
    16310101:    "Grocery & Gourmet Food",
    9482648011:  "Kitchen & Dining",
    130:         "Computers",
    541966:      "Electronics",
    2625373011:  "Cell Phones & Accessories",
    1736172:     "Movies & TV",
    5174:        "Music",
    409488:      "Software",
    11091801:    "Grocery & Gourmet Food",
    2582543011:  "Arts, Crafts & Sewing",
    3760931:     "Handmade Products",
}

CATEGORY_EMOJI = {
    "Electronics":               "💻",
    "Computers":                 "🖥️",
    "Cell Phones & Accessories": "📱",
    "Home & Kitchen":            "🏠",
    "Kitchen & Dining":          "🍳",
    "Clothing, Shoes & Jewelry": "👗",
    "Beauty & Personal Care":    "💄",
    "Health & Household":        "💊",
    "Toys & Games":              "🧸",
    "Sports & Outdoors":         "⚽",
    "Automotive":                "🚗",
    "Pet Supplies":              "🐾",
    "Baby":                      "🍼",
    "Patio, Lawn & Garden":      "🌱",
    "Office Products":           "📎",
    "Tools & Home Improvement":  "🔧",
    "Video Games":               "🎮",
    "Books":                     "📚",
    "Musical Instruments":       "🎸",
    "Movies & TV":               "🎬",
    "Music":                     "🎵",
    "Software":                  "💿",
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
    "Industrial & Scientific":   "🔩",
    "Arts, Crafts & Sewing":     "🎨",
    "Handmade Products":         "🤝",
}

KEYWORD_CATEGORIES = [
    (["hydraulic","press","lathe","drill press","bandsaw","grinder","welder","welding","compressor","generator","chainsaw","circular saw","table saw","miter saw","jigsaw","router","sander","planer","nailer","nail gun","staple gun","impact driver","impact wrench","torque wrench","socket set","wrench set","tool set","tool box","toolbox","workbench","clamp","vise","anvil","forge","metalwork","industrial","shop press"], "Tools & Home Improvement"),
    (["iphone","android","smartphone","cell phone","mobile phone","phone case","screen protector","phone charger","phone mount","sim card","airpods","earbuds","bluetooth headset"], "Cell Phones & Accessories"),
    (["laptop","computer","pc","desktop","monitor","keyboard","mouse","hard drive","ssd","ram","cpu","gpu","motherboard","printer","scanner","webcam","usb hub","external drive"], "Computers"),
    (["router","modem","smart home","alexa","echo","fire tv","fire stick","roku","streaming","hdmi","cable","ethernet"], "Electronics"),
    (["tv","television","projector","camera","lens","tripod","drone","headphone","speaker","amplifier","receiver"], "Electronics"),
    (["car","truck","vehicle","auto","motorcycle","tire","brake","oil filter","wiper blade","floor mat","seat cover","dash cam","jump starter","battery charger","tow strap"], "Automotive"),
    (["shirt","pants","dress","jacket","coat","sweater","hoodie","shorts","jeans","leggings","skirt","shoes","boots","sneakers","sandals","heels","handbag","purse","wallet","belt","hat","cap","gloves","scarf","sock","underwear","bra","swimsuit","jewelry","necklace","bracelet","ring","earring","watch"], "Clothing, Shoes & Jewelry"),
    (["sofa","couch","bed","mattress","pillow","blanket","sheet","curtain","rug","lamp","chair","table","desk","shelf","bookcase","dresser","nightstand","mirror","frame","vase","candle","vacuum","mop","broom","cleaning","detergent","laundry","trash","storage","organizer"], "Home & Kitchen"),
    (["blender","mixer","toaster","coffee maker","keurig","air fryer","instant pot","slow cooker","rice cooker","microwave","juicer","food processor","stand mixer","waffle","griddle","pan","pot","knife","cutting board","bakeware","cookware"], "Kitchen & Dining"),
    (["vitamin","supplement","protein","probiotic","fish oil","collagen","melatonin","zinc","magnesium","medicine","first aid","bandage","thermometer","blood pressure","glucose","hearing aid","contact lens","toothbrush","dental","razor","shaver"], "Health & Household"),
    (["shampoo","conditioner","moisturizer","serum","foundation","mascara","lipstick","perfume","cologne","nail polish","hair dryer","straightener","curling iron","makeup","skincare","sunscreen","lotion","face wash"], "Beauty & Personal Care"),
    (["lego","action figure","doll","board game","puzzle","play","toy","remote control car","rc car","nerf","pokemon","hot wheels","barbie","playset"], "Toys & Games"),
    (["dog","cat","fish","bird","hamster","pet","collar","leash","crate","aquarium","bird cage","pet food","treat","litter","flea"], "Pet Supplies"),
    (["diaper","baby","infant","toddler","stroller","car seat","crib","pacifier","bottle","formula","baby monitor","high chair"], "Baby"),
    (["guitar","piano","keyboard instrument","drum","violin","ukulele","bass guitar","music stand","instrument"], "Musical Instruments"),
    (["tent","sleeping bag","hiking","camping","backpack","climbing","kayak","canoe","fishing","hunting","archery","golf","tennis","basketball","football","soccer","baseball","yoga mat","dumbbell","barbell","weight","treadmill","bike","bicycle","scooter","ski","snowboard"], "Sports & Outdoors"),
    (["seed","plant","soil","fertilizer","garden hose","sprinkler","lawn mower","trimmer","hedge","rake","shovel","wheelbarrow","planter","outdoor furniture","patio","grill","bbq","fire pit"], "Patio, Lawn & Garden"),
    (["notebook","pen","pencil","stapler","paper","folder","binder","desk organizer","calculator","whiteboard","printer ink","toner","office chair","filing cabinet"], "Office Products"),
    (["suitcase","luggage","travel bag","duffel","carry on","passport holder","travel pillow","packing cube"], "Luggage & Travel"),
    (["snack","coffee","tea","protein bar","candy","chocolate","nuts","cereal","pasta","sauce","spice","condiment","juice","soda","food","grocery"], "Grocery & Gourmet Food"),
    (["ps5","xbox","nintendo","switch","gaming","controller","game","video game"], "Video Games"),
    (["blu-ray","dvd","movie","film","tv show","television series"], "Movies & TV"),
    (["vinyl","cd album","music cd","record","mp3"], "Music"),
    (["software","antivirus","windows","microsoft office","adobe","operating system"], "Software"),
    (["paint","canvas","brush","craft","knitting","crochet","sewing","embroidery","yarn","fabric","scrapbook","art supply"], "Arts, Crafts & Sewing"),
    (["handmade","artisan","handcrafted","custom made"], "Handmade Products"),
]

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]
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
    print("  Fetching deals from
