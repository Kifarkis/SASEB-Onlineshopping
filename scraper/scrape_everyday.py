"""SAS EuroBonus everyday/in-person shopping tracker."""

from __future__ import annotations

import json
import re
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

CARD_NETWORKS = [
    ("merchant_mc_cls.status", "MC"),
    ("merchant_visa_vop.status", "VISA"),
    ("merchant_visa_vlps.status", "VISA"),
    ("merchant_amex_sop.status", "AMEX"),
]

USER_AGENT = "Mozilla/5.0 (compatible; eb-tracker/1.0; +https://eurobonus.chiq.se)"

REQUEST_TIMEOUT = 30
PAGE_DELAY = 0.5

DESC_DANGER_TAGS = "(?:script|iframe|object|embed|style|form|input|button|link|meta)"
DESC_DANGER_BLOCK = re.compile(rf"<{DESC_DANGER_TAGS}\b[^>]*>.*?</[^>]+>", re.IGNORECASE | re.DOTALL)
DESC_DANGER_VOID = re.compile(rf"<{DESC_DANGER_TAGS}\b[^>]*/?>", re.IGNORECASE)
DESC_ON_HANDLER = re.compile(r'\son\w+\s*=\s*("[^"]*"|\'[^\']*\'|[^\s>]+)', re.IGNORECASE)
DESC_JS_HREF = re.compile(r'href\s*=\s*("javascript:[^"]*"|\'javascript:[^\']*\')', re.IGNORECASE)

POINTS_PREFIX_PATTERNS = [
    # English: "Earn N points per N currency." optionally followed by
    # "when paying with a linked card." or similar boilerplate sentence.
    re.compile(
        r"^Earn\s+\d+\s+(?:EuroBonus\s+)?(?:Bonus\s+)?points?\s+(?:pr|per)\.?\s+\d+\s+\w+\s*\.?"
        r"(?:\s*(?:when\s+(?:paying|entering)|by\s+paying|if\s+you\s+pay)[^.<]*?\.)?"
        r"\s*(?:<br\s*/?>\s*)*",
        re.IGNORECASE,
    ),
    # Tail-only fragment: description starts directly with "when paying..."
    # because the source data has lost the leading "Earn N points..." part.
    re.compile(
        r"^when\s+paying\s+with\s+a\s+linked\s+(?:payment\s+)?card\.?\s*(?:<br\s*/?>\s*)*",
        re.IGNORECASE,
    ),
    # Swedish: "Tjäna N poäng per N kr." optionally followed by tail clause.
    re.compile(
        r"^Tjäna\s+\d+\s+(?:EuroBonus\s+)?(?:Bonus)?[Pp]oäng\s+per\s+\d+\s+\w+\s*\.?"
        r"(?:\s*(?:när\s+du\s+betalar|när\s+du\s+anger)[^.<]*?\.)?"
        r"\s*(?:<br\s*/?>\s*)*",
        re.IGNORECASE,
    ),
]


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
    print(f"\nFetched {len(shops)} shops total.\n", flush=True)
    return shops


def sanitize_html(text: str | None) -> str:
    if not text:
        return ""
    text = DESC_DANGER_BLOCK.sub("", text)
    text = DESC_DANGER_VOID.sub("", text)
    text = DESC_ON_HANDLER.sub("", text)
    text = DESC_JS_HREF.sub("", text)
    text = text.replace("\u0001", "").replace("\u0002", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def strip_points_prefix(description: str) -> str:
    for pattern in POINTS_PREFIX_PATTERNS:
        description = pattern.sub("", description, count=1)
    return description.strip()


def extract_cards(raw: dict) -> list[str]:
    cards = []
    seen = set()
    for key, label in CARD_NETWORKS:
        if raw.get(key) == "active" and label not in seen:
            cards.append(label)
            seen.add(label)
    return cards


def fix_postcode_city_swap(postcode: str, city: str) -> tuple[str, str]:
    if not postcode or not city:
        return postcode, city
    postcode_has_letters = bool(re.search(r"[A-Za-zÅÄÖØÆÉÜåäöøæéü]", postcode))
    city_is_numeric = bool(re.fullmatch(r"\d+", city))
    if postcode_has_letters and city_is_numeric:
        return city, postcode
    return postcode, city


def is_online_only(raw: dict) -> bool:
    lat = raw.get("latitude")
    lng = raw.get("longitude")
    if lat is None or lng is None:
        return True
    if isinstance(lat, str) and not lat.strip():
        return True
    if isinstance(lng, str) and not lng.strip():
        return True
    return False


def parse_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+|www\.[^)\s]+)\)")


def clean_website(value: str | None) -> str:
    """Unwrap markdown-style links like [www.foo.com](https://www.foo.com).

    Some merchants register their website as a markdown link. Prefer the URL
    inside the parentheses; fall back to the raw value otherwise.
    """
    if not value:
        return ""
    text = value.strip()
    match = MARKDOWN_LINK_RE.search(text)
    if match:
        return match.group(2).strip()
    return text


def transform_shop(raw: dict) -> dict | None:
    _w = raw.get("website")
    if _w and "[" in str(_w):
        m = MARKDOWN_LINK_RE.search(str(_w).strip())
        print(f"DEBUG {raw.get('name_slug','?')} raw={_w!r} match={bool(m)} groups={m.groups() if m else None}", flush=True)

    country_id = raw.get("country_id")
    country_code = COUNTRY_MAP.get(country_id)
    if not country_code:
        return None

    postcode = (raw.get("postcode") or "").strip()
    city = (raw.get("city") or "").strip()
    postcode, city = fix_postcode_city_swap(postcode, city)

    address = (raw.get("address") or "").strip()
    online_only = is_online_only(raw)

    reward_rate = parse_float(raw.get("purchase_reward_rate")) or 0.0
    points_per_100 = round(reward_rate * 100)

    description = sanitize_html(raw.get("merchant_sas.description"))
    description = strip_points_prefix(description)

    has_campaign = bool(raw.get("merchant_campaigns.promote_campaign"))

    return {
        "uuid": raw.get("name_slug"),
        "name": raw.get("name_common"),
        "country": country_code,
        "city": city,
        "postcode": postcode,
        "address": address,
        "lat": parse_float(raw.get("latitude")),
        "lng": parse_float(raw.get("longitude")),
        "mode": "online" if online_only else "onsite",
        "category_id": raw.get("primary_category_id"),
        "points_per_100": points_per_100,
        "currency": raw.get("currencies.code"),
        "website": clean_website(raw.get("website")),
        "phone": (raw.get("phone") or "").strip(),
        "email": (raw.get("email") or "").strip(),
        "description": description,
        "cards_accepted": extract_cards(raw),
        "has_fixed_reward": bool(raw.get("merchant_sas.has_fixed_reward")),
        "coming_soon": bool(raw.get("merchant_sas.coming_soon")),
        "has_campaign": has_campaign,
        "campaign_title": raw.get("merchant_campaigns.campaign_title") if has_campaign else None,
        "campaign_description": sanitize_html(raw.get("merchant_campaigns.description")) if has_campaign else None,
        "created_at": raw.get("created_at"),
    }


def merge_with_existing(country_code: str, new_shops: list[dict]) -> list[dict]:
    """Merge fresh API data with persisted state.

    - Preserve `first_seen` from existing data when shop is still present.
    - Mark shops absent from the latest API as `status: gone`.
    - Add `first_seen` for newly observed shops.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    out_path = DATA_DIR / country_code / "shops.json"
    existing_by_uuid: dict[str, dict] = {}
    if out_path.exists():
        try:
            existing_data = json.loads(out_path.read_text(encoding="utf-8"))
            for s in existing_data.get("shops", []):
                if s.get("uuid"):
                    existing_by_uuid[s["uuid"]] = s
        except (json.JSONDecodeError, OSError) as e:
            print(f"  warning: could not read existing {country_code} data ({e}), starting fresh", flush=True)

    new_uuids = set()
    merged: list[dict] = []
    for shop in new_shops:
        uuid = shop.get("uuid")
        if not uuid:
            continue
        new_uuids.add(uuid)
        prev = existing_by_uuid.get(uuid)
        shop["first_seen"] = prev["first_seen"] if prev and prev.get("first_seen") else today
        shop["status"] = "active"
        merged.append(shop)

    for uuid, prev in existing_by_uuid.items():
        if uuid in new_uuids:
            continue
        prev["status"] = "gone"
        if not prev.get("gone_since"):
            prev["gone_since"] = today
        merged.append(prev)

    return merged


def write_country_file(country_code: str, shops: list[dict]) -> Path:
    out_dir = DATA_DIR / country_code
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "shops.json"
    payload = {
        "country": country_code,
        "updated": datetime.now(timezone.utc).isoformat(),
        "shop_count": sum(1 for s in shops if s.get("status") == "active"),
        "shops": shops,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    raw_shops = fetch_all_shops()

    transformed_by_country: dict[str, list[dict]] = {}
    skipped = 0
    for raw in raw_shops:
        shop = transform_shop(raw)
        if shop is None:
            skipped += 1
            continue
        transformed_by_country.setdefault(shop["country"], []).append(shop)

    if skipped:
        print(f"Skipped {skipped} shops with unknown country_id", flush=True)

    for country_code in sorted(COUNTRY_MAP.values()):
        country_shops = transformed_by_country.get(country_code, [])
        merged = merge_with_existing(country_code, country_shops)
        path = write_country_file(country_code, merged)
        active = sum(1 for s in merged if s.get("status") == "active")
        gone = sum(1 for s in merged if s.get("status") == "gone")
        online = sum(1 for s in merged if s.get("mode") == "online" and s.get("status") == "active")
        print(f"{country_code}: {active} active ({online} online-only), {gone} gone -> {path.relative_to(REPO_ROOT)}", flush=True)


if __name__ == "__main__":
    main()
