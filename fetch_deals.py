"""
DealDrop — fetch_deals.py
24-hour deal memory — keeps deals for 24 hours then removes them.
Permanent category fix — 3-tier category detection system.
"""

import json
import os
import time
import datetime
import requests

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MEMORY_FILE        = "deals_memory.json"
MAX_DEALS          = 300
MIN_DISCOUNT_PCT   = 10
HOT_DEAL_PCT       = 50
DOMAIN_ID          = "1"
DEALS_TO_SHOW      = 50
DEAL_TTL_HOURS     = 24

CATEGORY_NAMES = {
    172282:       "Electronics",
    493964:       "Electronics",
    541966:       "Electronics",
    1266092011:   "Electronics",
    13896617011:  "Computers",
    2335752011:   "Cell Phones & Accessories",
    2625373011:   "Cell Phones & Accessories",
    7141123011:   "Clothing, Shoes & Jewelry",
    1036592:      "Clothing, Shoes & Jewelry",
    1055398:      "Home & Kitchen",
    284507:       "Home & Kitchen",
    9482648011:   "Kitchen & Dining",
    228013:       "Tools & Home Improvement",
    2619525011:   "Tools & Home Improvement",
    15684181:     "Automotive",
    491244:       "Automotive",
    2619533011:   "Automotive",
    10399642011:  "Automotive",
    3375251:      "Sports & Outdoors",
    1064012:      "Sports & Outdoors",
    165793011:    "Toys & Games",
    1249140011:   "Toys & Games",
    51574011:     "Pet Supplies",
    2619534011:   "Pet Supplies",
    165796011:    "Baby",
    2619535011:   "Baby",
    1064954:      "Health & Household",
    3760911:      "Beauty & Personal Care",
    11055981:     "Beauty & Personal Care",
    7730994011:   "Beauty & Personal Care",
    2972638011:   "Patio, Lawn & Garden",
    979455011:    "Patio, Lawn & Garden",
    1064278:      "Office Products",
    1285128:      "Office Products",
    283155:       "Books",
    468642:       "Video Games",
    2858778011:   "Movies & TV",
    5174:         "Music",
    11091801:     "Musical Instruments",
    2238192011:   "Musical Instruments",
    409488:       "Software",
    16310101:     "Grocery & Gourmet Food",
    3780361:      "Luggage & Travel",
    9479199011:   "Luggage & Travel",
    3760901:      "Luggage & Travel",
    2582543011:   "Arts, Crafts & Sewing",
    3760931:      "Handmade Products",
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

BAD_CATEGORY_WORDS = [
    "strut","shock absorber","suspension","brake pad","brake rotor","brake kit",
    "caliper","wheel bearing","control arm","tie rod","ball joint","cv axle",
    "muffler","exhaust","radiator","alternator","fuel pump","water pump",
    "wiper blade","floor mat","car seat cover","oil filter","spark plug",
    "lawn mower","string trimmer","leaf blower","hedge trimmer","chainsaw",
    "garden hose","sprinkler","fire pit","bbq grill","patio chair","hammock",
    "sofa","sectional","recliner","dresser","bookcase","curtain","area rug",
    "door mat","welcome mat","air purifier","humidifier","space heater",
    "shirt","pants","dress","jacket","hoodie","sneakers","boots","handbag",
    "vitamin","supplement","protein powder","first aid","thermometer",
    "shampoo","moisturizer","foundation","mascara","perfume","razor",
    "dog food","cat food","dog bed","cat tree","litter box","fish tank",
    "diaper","stroller","car seat","crib","baby monitor","pacifier",
    "guitar","piano","drum","violin","saxophone","trumpet","ukulele",
    "yoga mat","dumbbell","barbell","treadmill","kayak","fishing rod",
    "puzzle","board game","action figure","lego","nerf","stuffed animal",
    "notebook","stapler","binder","whiteboard","pencil","calculator",
    "suitcase","luggage","travel pillow","passport holder","packing cube",
    "coffee","tea","protein bar","nuts","cereal","pasta","olive oil",
    "acrylic paint","canvas","embroidery","knitting","crochet","sewing",
]

KEYWORD_CATEGORIES = [
    (["strut","shock absorber","suspension","brake pad","brake rotor","brake kit","caliper","wheel bearing","control arm","tie rod","ball joint","cv axle","cv joint","catalytic converter","muffler","exhaust","radiator","alternator","starter motor","fuel pump","water pump","timing belt","serpentine belt","wiper blade","floor mat car","car seat cover","dash cam","jump starter","tow strap","oil filter","air filter cabin","spark plug","lug nut","wheel spacer","trailer hitch","tonneau cover","running board","mud flap","car cover","tire inflator","tire gauge","wheel cleaner"], "Automotive"),
    (["hydraulic press","shop press","drill press","lathe","bandsaw","table saw","miter saw","circular saw","jigsaw","reciprocating saw","angle grinder","bench grinder","air compressor","pressure washer","welder","welding","soldering iron","torque wrench","socket set","wrench set","tool set","tool box","toolbox","workbench","pipe wrench","pliers set","screwdriver set","clamp set","vise","saw blade","router table","planer","jointer","brad nailer","framing nailer","staple gun","nail gun","heat gun","caulk gun","wire stripper","crimping tool","voltage tester","stud finder","tape measure"], "Tools & Home Improvement"),
    (["machine screw","hex bolt","hex nut","lock nut","flange nut","carriage bolt","lag screw","sheet metal screw","anchor bolt","rivet set","threaded rod","shaft coupling","ball bearing","sprocket","conveyor","industrial valve","pneumatic fitting","hydraulic fitting","wire loom","heat shrink tubing","terminal block","relay switch","contactor","industrial motor","centrifugal pump","air compressor tank"], "Industrial & Scientific"),
    (["iphone case","samsung case","phone case","screen protector","tempered glass","phone charger","wireless charger","car phone mount","phone stand","magsafe","lightning cable","usb-c cable","phone holder","pop socket","airpods case","wireless earbuds","bluetooth earphone","phone wallet case"], "Cell Phones & Accessories"),
    (["gaming laptop","notebook computer","desktop computer","all-in-one pc","computer monitor","curved monitor","gaming monitor","mechanical keyboard","gaming keyboard","wireless keyboard","gaming mouse","wireless mouse","mousepad","usb hub","external hard drive","solid state drive","nvme ssd","graphics card","gpu","cpu cooler","pc case","power supply unit","motherboard","cpu processor","webcam","network card","wifi adapter","ethernet switch","nas drive","ups battery backup"], "Computers"),
    (["smart tv","4k tv","oled tv","qled tv","projector","soundbar","home theater","stereo receiver","turntable","record player","bluetooth speaker","smart speaker","security camera","doorbell camera","action camera","mirrorless camera","dslr camera","camera lens","drone","vr headset","streaming stick","hdmi switch","surge protector","smart plug","smart bulb","led strip light"], "Electronics"),
    (["t-shirt","polo shirt","dress shirt","button down","flannel shirt","hoodie","zip hoodie","pullover","crewneck","cardigan","sweater","windbreaker","rain jacket","winter coat","puffer jacket","cargo pants","chino pants","sweatpants","jogger pants","leggings","yoga pants","athletic shorts","board shorts","swim trunks","bikini","sports bra","underwear","boxer briefs","compression shorts","maxi dress","mini dress","blouse","tunic","midi skirt","skinny jeans","bootcut jeans","sneakers","running shoes","walking shoes","dress shoes","loafers","oxford shoes","ankle boots","chelsea boots","cowboy boots","sandals","flip flops","high heels","wedges","tote bag","crossbody bag","backpack purse","leather wallet","money clip","leather belt","necklace","bracelet","earrings","engagement ring","watch band"], "Clothing, Shoes & Jewelry"),
    (["air fryer","instant pot","pressure cooker","slow cooker","rice cooker","bread maker","waffle maker","panini press","electric griddle","toaster oven","convection oven","keurig","nespresso","espresso machine","french press","pour over coffee","vitamix","ninja blender","food processor","stand mixer","hand mixer","juicer","mandoline slicer","food dehydrator","cast iron skillet","nonstick pan","stainless steel pan","dutch oven","carbon steel wok","saucepan","stockpot","baking sheet","cake pan","muffin tin","loaf pan","pie dish","casserole dish","mixing bowl set","cutting board set","knife set","chef knife","santoku knife","bread knife","kitchen shears","measuring cups","colander","strainer","spatula set","ladle","whisk","tongs","oven mitt","dish rack","pot holder"], "Kitchen & Dining"),
    (["sofa","sectional sofa","loveseat","recliner chair","accent chair","dining chair","bar stool","bed frame","headboard","nightstand","dresser","chest of drawers","wardrobe","bookcase","bookshelf","tv stand","entertainment center","coffee table","end table","console table","standing desk","bathroom vanity","shower curtain","bath mat","towel rack","curtain rod","blackout curtain","throw pillow","bed sheet set","comforter","duvet cover","mattress topper","area rug","runner rug","welcome mat","wall art","picture frame","wall mirror","floor lamp","table lamp","ceiling fan","air purifier","humidifier","space heater","tower fan","robot vacuum","storage bin","closet organizer","shoe rack","trash can","recycling bin"], "Home & Kitchen"),
    (["multivitamin","vitamin c","vitamin d","vitamin b12","zinc supplement","magnesium supplement","calcium supplement","fish oil","omega 3","probiotics","collagen peptides","whey protein","pre workout","creatine","bcaa","melatonin","elderberry","turmeric supplement","ashwagandha","first aid kit","bandage","gauze pad","thermometer","blood pressure monitor","pulse oximeter","glucose meter","heating pad","knee brace","back brace","wrist brace","ankle brace","pill organizer","contact lens solution","electric toothbrush","water flosser","whitening strips","safety razor","electric shaver","hair trimmer","body trimmer","nail clipper set","cotton swabs"], "Health & Household"),
    (["face moisturizer","eye cream","face serum","retinol cream","hyaluronic acid","vitamin c serum","spf sunscreen","liquid foundation","concealer","setting powder","blush palette","bronzer","eyeshadow palette","eyeliner pencil","mascara","lipstick","lip gloss","setting spray","face primer","facial toner","face cleanser","face exfoliator","clay mask","sheet mask","micellar water","makeup remover","dry shampoo","hair mask","hair serum","hair oil","hair spray","hair gel","pomade","hair dye","flat iron","curling wand","hair dryer diffuser","body lotion","body butter","body wash","bath bomb set","perfume","cologne","body spray","deodorant","nail polish","nail gel kit","lip balm"], "Beauty & Personal Care"),
    (["lego set","duplo","action figure","barbie doll","hot wheels","remote control car","rc truck","nerf gun","nerf blaster","water gun","play set","dollhouse","toy kitchen","play doh","kinetic sand","slime kit","science kit","board game","card game","jigsaw puzzle","3d puzzle","rubiks cube","stuffed animal","plush toy","teddy bear","pokemon card","trading card","collectible figure","baby toy","infant toy","teether","rattle","play tent","trampoline"], "Toys & Games"),
    (["dog food","cat food","dog treat","cat treat","dog toy","cat toy","dog bed","cat bed","dog crate","cat carrier","dog collar","cat collar","dog leash","retractable leash","dog harness","dog bowl","cat bowl","pet fountain","dog shampoo","flea treatment","litter box","cat litter","cat tree","cat scratcher","bird cage","bird feeder","fish tank","aquarium","reptile tank","hamster cage"], "Pet Supplies"),
    (["diaper","baby wipe","baby lotion","baby shampoo","baby monitor","baby swing","baby bouncer","baby carrier","baby wrap","jogging stroller","travel system stroller","infant car seat","convertible car seat","crib","bassinet","pack and play","changing table","nursing pillow","breast pump","bottle warmer","baby bottle","sippy cup","pacifier","baby food","baby formula","high chair","baby gate","baby bathtub"], "Baby"),
    (["acoustic guitar","electric guitar","bass guitar","guitar amp","guitar pedal","guitar string","guitar strap","ukulele","banjo","violin","viola","cello","keyboard piano","digital piano","midi keyboard","drum set","drum kit","cymbal","drum stick","drum pad","electronic drum","trumpet","trombone","saxophone","clarinet","flute","harmonica","accordion","music stand","metronome","tuner clip","audio interface","studio monitor","xlr cable"], "Musical Instruments"),
    (["yoga mat","yoga block","foam roller","resistance band","pull up bar","dumbbell set","barbell","weight plate","kettlebell","weight bench","squat rack","power rack","treadmill","elliptical machine","stationary bike","rowing machine","jump rope","medicine ball","ab wheel","gym bag","gym gloves","weightlifting belt","knee sleeve","hiking boot","hiking pole","hydration pack","camping tent","sleeping bag","sleeping pad","camp stove","headlamp lantern","fishing rod","fishing reel","kayak paddle","life jacket","snorkel set","surfboard","skateboard","bike helmet","cycling jersey","bike lock","golf club","tennis racket","basketball hoop","swimming goggle","ski goggle","ski helmet","snowboard binding"], "Sports & Outdoors"),
    (["lawn mower","riding mower","zero turn mower","push mower","string trimmer","weed eater","leaf blower","leaf vacuum","hedge trimmer","pole saw","pruning shear","loppers","garden hoe","garden rake","garden spade","garden trowel","wheelbarrow","garden cart","garden hose","soaker hose","drip irrigation","sprinkler head","hose reel","watering can","garden sprayer","fertilizer spreader","compost bin","raised garden bed","planter box","flower pot","garden edging","weed killer","bird feeder","bird bath","fire pit","chiminea","outdoor heater","bbq grill","charcoal grill","gas grill","pellet grill","smoker grill","griddle outdoor","grill cover","grill brush","patio chair","adirondack chair","patio table","patio umbrella","outdoor cushion","hammock","string light outdoor","solar pathway light","landscape light"], "Patio, Lawn & Garden"),
    (["office chair","ergonomic chair","monitor stand","monitor arm","laptop stand","desk organizer","pencil holder","paper tray","file organizer","binder","hanging folder","file cabinet","label maker","laminator","paper shredder","stapler","hole punch","tape dispenser","whiteboard","cork board","dry erase marker","highlighter set","ballpoint pen","gel pen","mechanical pencil","notebook spiral","legal pad","index card","planner","desk calendar","badge holder","lanyard"], "Office Products"),
    (["carry on luggage","checked luggage","hardside luggage","spinner luggage","rolling luggage","duffel bag","weekender bag","travel backpack","packing cube","toiletry bag","dopp kit","passport holder","travel wallet","luggage lock","luggage tag","luggage strap","travel pillow","neck pillow","eye mask travel","travel blanket","travel adapter","portable charger travel","travel umbrella","money belt","hidden wallet"], "Luggage & Travel"),
    (["ground coffee","coffee bean","instant coffee","coffee pod","k cup","loose leaf tea","green tea matcha","protein bar","granola bar","trail mix","mixed nuts","beef jerky","protein shake","meal replacement shake","electrolyte drink","kombucha","apple cider vinegar","extra virgin olive oil","coconut oil","avocado oil","hot sauce","soy sauce","pasta sauce","salsa jar","hummus","peanut butter","almond butter","raw honey","maple syrup","dark chocolate","baking powder","baking soda","all purpose flour","rolled oats","granola","breakfast cereal","instant oatmeal","white rice","quinoa","dried lentil","canned chickpea","canned tomato","coconut milk","unsweetened almond milk","rice cake","microwave popcorn"], "Grocery & Gourmet Food"),
    (["ps5 controller","ps4 controller","xbox series controller","nintendo switch game","switch lite","steam deck","gaming headset","pro controller","joy con","game cartridge","capture card","streaming deck","elgato","razer gaming","corsair gaming","logitech gaming","steelseries","hyperx"], "Video Games"),
    (["blu ray disc","4k blu ray","dvd movie","complete tv series dvd","criterion collection","anime dvd","documentary blu ray"], "Movies & TV"),
    (["vinyl record","lp album","music cd","greatest hits cd","box set music","cassette tape","record cleaner","turntable stylus","record storage"], "Music"),
    (["windows 11 key","microsoft office","office 365","adobe creative cloud","photoshop license","antivirus software","norton security","mcafee","vpn subscription","quickbooks","turbotax","tax software","autocad","video editing software","photo editing software"], "Software"),
    (["acrylic paint set","oil paint set","watercolor set","stretched canvas","canvas board","paint brush set","easel","sketchbook","drawing pad","charcoal pencil","pastel set","colored pencil set","copic marker","alcohol marker","calligraphy pen","brush pen","stamp pad","scrapbook kit","washi tape set","die cut machine","cricut maker","heat press machine","sublimation paper","embroidery hoop","embroidery thread","cross stitch kit","knitting needle set","crochet hook set","yarn skein","sewing machine","serger machine","sewing thread","fabric bolt","felt sheet","foam sheet craft","mod podge","resin kit","epoxy resin","silicone mold","air dry clay","polymer clay","sculpting tool","diamond painting kit","paint by number","string art kit"], "Arts, Crafts & Sewing"),
    (["handmade","hand crafted","artisan made","hand poured candle","hand stamped","hand painted","hand sewn","hand knit","hand woven","custom engraved","personalized gift","monogrammed","made to order","small batch","cottage industry","folk art"], "Handmade Products"),
]

def get_category(product):
    """
    3-tier category detection:
    1. Keepa category ID lookup with sanity check
    2. Keyword matching on product title
    3. Default fallback
    """
    title = (product.get("title") or "").lower()

    # Tier 1: rootCategory ID with sanity check
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        cat = CATEGORY_NAMES[root]
        # Sanity check — if Keepa says Computers/Electronics but title
        # clearly describes something else, fall through to keyword matching
        if cat in ("Computers", "Electronics") and any(w in title for w in BAD_CATEGORY_WORDS):
            pass
        else:
            return cat

    # Tier 2: categories list with sanity check
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            cat = CATEGORY_NAMES[cat_id]
            if cat in ("Computers", "Electronics") and any(w in title for w in BAD_CATEGORY_WORDS):
                pass
            else:
                return cat

    # Tier 3: keyword matching
    for keywords, category in KEYWORD_CATEGORIES:
        if any(w in title for w in keywords):
            return category

    # Tier 4: default
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

# ─── 24-HOUR DEAL MEMORY ─────────────────────────────────────────────────────

def load_memory():
    try:
        with open(MEMORY_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_memory(memory):
    with open(MEMORY_FILE, "w") as f:
        json.dump(memory, f, indent=2)

def is_expired(first_seen_str):
    try:
        first_seen = datetime.datetime.fromisoformat(first_seen_str.replace("Z", ""))
        age_hours = (datetime.datetime.utcnow() - first_seen).total_seconds() / 3600
        return age_hours >= DEAL_TTL_HOURS
    except Exception:
        return True

def merge_with_memory(new_deals):
    memory = load_memory()
    now    = datetime.datetime.utcnow().isoformat() + "Z"

    expired_count = 0
    for asin in list(memory.keys()):
        if is_expired(memory[asin].get("firstSeen", now)):
            del memory[asin]
            expired_count += 1

    if expired_count > 0:
        print(f"  Removed {expired_count} expired deals from memory")

    new_count = 0
    for deal in new_deals:
        asin = deal["asin"]
        if asin not in memory:
            deal["firstSeen"] = now
            memory[asin] = deal
            new_count += 1
        else:
            first_seen = memory[asin]["firstSeen"]
            memory[asin] = deal
            memory[asin]["firstSeen"] = first_seen

    print(f"  Added {new_count} new deals to memory")
    print(f"  Total deals in memory: {len(memory)}")

    save_memory(memory)
    return list(memory.values())

# ─── KEEPA DEAL FINDER ───────────────────────────────────────────────────────

def fetch_deal_asins():
    print("  Fetching deals from Keepa — 8 pages...")
    url     = "https://api.keepa.com/deal"
    params  = {"key": KEEPA_API_KEY}
    headers = {"Content-Type": "application/json"}
    all_asins = []

    for page in range(8):
        body = {
            "domainId":     1,
            "priceTypes":   [0],
            "deltaPercent": MIN_DISCOUNT_PCT,
            "interval":     10080,
            "page":         page,
        }
        try:
            r = requests.post(url, params=params, json=body, headers=headers, timeout=30)
            print(f"    Page {page} status: {r.status_code}")
            if r.status_code == 200:
                data  = r.json()
                deals = data.get("deals", {}).get("dr", [])
                asins = [d.get("asin") for d in deals if d.get("asin")]
                print(f"    Page {page}: {len(asins)} ASINs")
                all_asins.extend(asins)
                if len(asins) < 100:
                    print(f"    Only {len(asins)} results — no more pages")
                    break
            else:
                print(f"    Error: {r.text[:200]}")
                break
        except Exception as e:
            print(f"    Page {page} failed: {e}")
            break
        time.sleep(1)

    seen   = set()
    unique = []
    for a in all_asins:
        if a not in seen:
            seen.add(a)
            unique.append(a)

    print(f"  Total unique ASINs: {len(unique)}")
    return unique

def fetch_products(asins):
    chunk_size   = 20
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
                data     = r.json()
                products = data.get("products", [])
                all_products.extend(products)
                print(f"    Got {len(products)} products")
            else:
                print(f"    Error: {r.text[:200]}")
            time.sleep(1)
        except Exception as e:
            print(f"    Request failed: {e}")
    return all_products

# ─── BUILD deals.json ─────────────────────────────────────────────────────────

def build_deals_json():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting DealDrop...\n")

    deal_asins = fetch_deal_asins()
    if not deal_asins:
        print("  No deal ASINs.")

    fetch_count = min(len(deal_asins), MAX_DEALS)
    products    = []
    if fetch_count > 0:
        print(f"\n  Fetching details for {fetch_count} products...")
        products = fetch_products(deal_asins[:fetch_count])
        print(f"  Total products fetched: {len(products)}")

    new_deals = []
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
            was_display   = f"${yesterday_price/100:.2f}" if ye
