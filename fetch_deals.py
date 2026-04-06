"""
DealDrop — fetch_deals.py
"""

import json
import os
import time
import datetime

KEEPA_API_KEY      = os.environ.get("KEEPA_API_KEY", "")
AMAZON_PARTNER_TAG = os.environ.get("AFFILIATE_TAG", "")
OUTPUT_FILE        = "deals.json"
MAX_DEALS          = 50
MIN_DISCOUNT_PCT   = 20
HOT_DEAL_PCT       = 50

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
    "Grocery & Gourmet Food":    "🛒",
    "Luggage & Travel":          "🧳",
}

def get_category(product):
    root = product.get("rootCategory")
    if root and root in CATEGORY_NAMES:
        return CATEGORY_NAMES[root]
    for cat_id in (product.get("categories") or []):
        if cat_id in CATEGORY_NAMES:
            return CATEGORY_NAMES[cat_id]
    title = (product.get("title") or "").lower()
    if any(w in title for w in ["laptop","phone","tablet","camera","headphone","speaker","monitor","tv"]):
        return "Electronics"
    if any(w in title for w in ["shirt","shoe","dress","jacket","pants","bag","watch"]):
        return "Clothing, Shoes & Jewelry"
    if any(w in title f
