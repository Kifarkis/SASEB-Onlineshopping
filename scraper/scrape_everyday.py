"""SAS EuroBonus everyday/in-person shopping tracker."""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "everyday"

API_URL = "https://eb-member-portal-api.loyaltfacts.com/stores"

COUNTRY_MAP = {
    1: "dk",
    2: "se",
    3: "no",
    4: "fo",
    5: "fi",
}

CATEGORY_IDS = list(range(1, 13))

CARD_NETWORK_KEYS = [
    ("merchant_mc_cls.status", "MC"),
    ("merchant_visa_vop.status", "VISA"),
    ("merchant_visa_vlps.status", "VISA"),
    ("merchant_amex_sop.status", "AMEX"),
    ("merchant_npc.status", "NPC"),
]

USER_AGENT = "Mozilla/5.0 (compatible; eb-tracker/1.0; +https://eurobonus.chiq.se)"

REQUEST_TIMEOUT = 30
PAGE_DELAY = 0.5


def fetch_page(offset: int) -> dict:
    params = [
        ("autoComplete", "0"),
        ("hideComingSoon", "0"),
        ("webShops", "0"),
        ("specialCampaign", "0"),
        ("redemptionOnly", "0"),
        ("sortBy", "name_common"),
        ("sortDirection", "asc"),
        ("offset", str(offset)),
    ]
    for cat_id in CATEGORY_IDS:
        params.append(("categories[]", str(cat_id)))
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    resp = requests.get(API_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_all_shops() -> list[dict]:
    print("Fetching page 1...", flush=True)
    first = fetch_page(0)
    pages = first.get("pages", 1)
    shops = list(first.get("shops", []))
    print(f"  total pages: {pages}, shops on page 1: {len(shops)}", flush=True)
    for offset in range(1, pages):
        time.sleep(PAGE_DELAY)
        print(f"Fetching page {offset + 1}/{pages}...", flush=True)
        page = fetch_page(offset)
        page_shops = page.get("shops", [])
        shops.extend(page_shops)
        print(f"  shops on page: {len(page_shops)} (running total: {len(shops)})", flush=True)
    return shops


if __name__ == "__main__":
    shops = fetch_all_shops()
    print(f"\nFetched {len(shops)} shops total.")
    if shops:
        print("\nFirst shop sample:")
        print(json.dumps(shops[0], indent=2, ensure_ascii=False)[:1500])
