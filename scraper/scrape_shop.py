"""SAS EuroBonus Shop tracker — gift cards and Deals of the Month.

Scrapes public listing pages at saseurobonusshop.com. Logged-out data only:
brand, name, points price, cash price, and the accrual figure where shown.

Gift cards list one tile per brand; the denomination selector is behind a
login, so per-denomination earn rates are not available. Their accrual reads
"up to N points" (the maximum across denominations) and is stored as
max_earn_points, not as a rate.

Deals of the Month are single SKUs, so their accrual is exact. The earn rate
there is a flat 100 points per 100 currency across the whole category, which
makes it useless for ranking — ore_per_point (cash price / points price) is
the metric that actually varies.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "shop"

BASE_URL = "https://www.saseurobonusshop.com"

# Region code -> (url segment, currency). "en" is the EUR catalogue.
REGIONS = {
    "se": ("se", "SEK"),
    "dk": ("dk", "DKK"),
    "no": ("no", "NOK"),
    "en": ("en", "EUR"),
}

COLLECTIONS = {
    "giftcards": "gift-cards-vouchers",
    "deals": "deals-of-the-month",
}

USER_AGENT = "Mozilla/5.0 (compatible; eb-tracker/1.0; +https://eurobonus.chiq.se)"

REQUEST_TIMEOUT = 30
PAGE_DELAY = 1.0
MAX_PAGES = 40

# Class names carry a per-build hash suffix (styles_name__d5455ad3), so match
# on the stable prefix only. A CSS rebuild changes the hash, not the prefix.
TILE_SELECTOR = 'div[class^="ProductList_item__"]'
PAGER_LINK_SELECTOR = 'a[class^="PaginatedProductList_paginationButton__"]'

NBSP = "\u00a0"
NNBSP = "\u202f"

# "1 610 poäng" / "5 000 points" — thousands separated by various space chars.
POINTS_RE = re.compile(
    r"([\d][\d\s" + NBSP + NNBSP + r"]*)\s*(?:poäng|poeng|poin?ts?|pt)",
    re.IGNORECASE,
)
# "50 kr" / "6 570 kr" / "167,56 €"
CASH_RE = re.compile(r"([\d][\d\s" + NBSP + NNBSP + r"]*(?:[.,]\d+)?)\s*(kr|€|dkk|nok|sek|eur)", re.IGNORECASE)
# "upp till" (sv), "op til" (da), "opp til" (nb), "up to" (en)
UP_TO_RE = re.compile(r"\b(?:upp\s+till|op\s+til|opp\s+til|up\s+to)\b", re.IGNORECASE)


def _text(node) -> str:
    """Collapse a node's text, normalising the various space characters."""
    if node is None:
        return ""
    raw = node.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", raw.replace(NBSP, " ").replace(NNBSP, " ")).strip()


def _select_prefixed(tile, prefix: str):
    """First descendant whose class starts with the given prefix."""
    return tile.select_one(f'[class^="{prefix}"]')


def parse_number(text: str) -> float | None:
    """Parse '1 610' or '167,56' into a float. Returns None if unparseable."""
    if not text:
        return None
    cleaned = text.replace(NBSP, "").replace(NNBSP, "").replace(" ", "")
    # Decimal comma is the Nordic convention; there are no thousands separators
    # left at this point, so a comma can only be a decimal point.
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_points(text: str) -> int | None:
    m = POINTS_RE.search(text or "")
    if not m:
        return None
    value = parse_number(m.group(1))
    return int(value) if value is not None else None


def parse_cash(text: str) -> tuple[float | None, str | None]:
    m = CASH_RE.search(text or "")
    if not m:
        return None, None
    return parse_number(m.group(1)), m.group(2).lower()


def parse_tile(tile, region: str, collection: str) -> dict | None:
    link = tile.find("a", href=True)
    if link is None:
        return None
    href = link["href"].strip()
    slug = href.rstrip("/").rsplit("/", 1)[-1]
    if not slug:
        return None

    name = _text(_select_prefixed(tile, "styles_name__"))
    brand = _text(_select_prefixed(tile, "styles_brand__"))
    accrual_text = _text(_select_prefixed(tile, "styles_accrual__"))
    price_text = _text(_select_prefixed(tile, "styles_price__"))

    img = _select_prefixed(tile, "styles_image__")
    image = img.get("src", "").strip() if img else ""

    points_price = parse_points(price_text)
    cash_price, cash_unit = parse_cash(price_text)

    accrual_points = parse_points(accrual_text)
    accrual_is_max = bool(accrual_text and UP_TO_RE.search(accrual_text))

    # Cash per point, expressed in minor units (ore/oyre/cents) per point.
    # Only meaningful when both sides are present and the item is a single SKU.
    ore_per_point = None
    if points_price and cash_price:
        ore_per_point = round(cash_price * 100 / points_price, 3)

    # Exact accrual on a single-SKU item gives a real rate. For gift cards the
    # accrual is a maximum over unknown denominations, so no rate is derivable.
    points_per_100 = None
    if accrual_points and cash_price and not accrual_is_max:
        points_per_100 = round(accrual_points * 100 / cash_price)

    return {
        "uuid": f"{region}:{slug}",
        "slug": slug,
        "name": name,
        "brand": brand,
        "collection": collection,
        "region": region,
        "url": BASE_URL + href if href.startswith("/") else href,
        "image": image,
        "points_price": points_price,
        "cash_price": cash_price,
        "cash_unit": cash_unit,
        "accrual_points": accrual_points,
        "accrual_is_max": accrual_is_max,
        "max_earn_points": accrual_points if accrual_is_max else None,
        "points_per_100": points_per_100,
        "ore_per_point": ore_per_point,
    }


# Product-count header, e.g. "Visar 150 produkter i PRESENTKORT" (sv),
# "Viser 37 produkter" (da/nb), "Showing 142 products" (en).
PRODUCT_COUNT_RE = re.compile(
    r"(?:visar|viser|showing)\s+(\d+)\s+(?:produkter|products)",
    re.IGNORECASE,
)

# Marker shown when a region has no gift cards and only a region picker is
# rendered, e.g. the EUR storefront: "Looking for gift cards?" /
# "Currently we do not offer Gift Cards in your region."
NOT_AVAILABLE_RE = re.compile(
    r"do not offer|looking for gift cards|erbjuder (?:vi )?inte|tilbyder vi ikke",
    re.IGNORECASE,
)


def parse_listing(html: str, region: str, collection: str) -> tuple[list[dict], int, dict]:
    """Return (items, last_page, meta) for one listing page.

    meta carries signals that let the caller tell an empty-by-design page
    (a region with no products) apart from a parsing failure:
      - product_count: int|None  — the "Visar N produkter" header value
      - not_available: bool       — a "we don't offer this here" marker
    """
    soup = BeautifulSoup(html, "html.parser")

    items = []
    for tile in soup.select(TILE_SELECTOR):
        item = parse_tile(tile, region, collection)
        if item:
            items.append(item)

    last_page = 1
    for a in soup.select(PAGER_LINK_SELECTOR):
        label = _text(a)
        if label.isdigit():
            last_page = max(last_page, int(label))

    page_text = soup.get_text(" ", strip=True)
    count_match = PRODUCT_COUNT_RE.search(page_text)
    meta = {
        "product_count": int(count_match.group(1)) if count_match else None,
        "not_available": bool(NOT_AVAILABLE_RE.search(page_text)),
    }

    return items, last_page, meta


def fetch(url: str, session: requests.Session) -> str:
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def scrape_collection(region: str, collection: str, session: requests.Session) -> tuple[list[dict], bool]:
    """Scrape one collection. Returns (items, empty_by_design).

    empty_by_design is True when the first page legitimately has no products
    (a region that doesn't carry this collection), so the caller can treat
    it as success rather than a parse failure.
    """
    segment, _currency = REGIONS[region]
    path = COLLECTIONS[collection]
    base = f"{BASE_URL}/{segment}/{path}"

    print(f"  {region}/{collection}: page 1", flush=True)
    html = fetch(base, session)
    items, last_page, meta = parse_listing(html, region, collection)
    last_page = min(last_page, MAX_PAGES)

    # No tiles on page 1 — decide whether that's legitimate or a break.
    if not items:
        empty_by_design = meta["not_available"] or meta["product_count"] == 0
        if empty_by_design:
            print(f"  {region}/{collection}: none offered in this region", flush=True)
        return [], empty_by_design

    for page in range(2, last_page + 1):
        time.sleep(PAGE_DELAY)
        print(f"  {region}/{collection}: page {page}/{last_page}", flush=True)
        page_html = fetch(f"{base}?page={page}", session)
        page_items, _, _ = parse_listing(page_html, region, collection)
        if not page_items:
            break
        items.extend(page_items)

    # Same product can appear twice if the catalogue shifts mid-crawl.
    seen = set()
    deduped = []
    for item in items:
        if item["uuid"] in seen:
            continue
        seen.add(item["uuid"])
        deduped.append(item)

    print(f"  {region}/{collection}: {len(deduped)} items", flush=True)
    return deduped, False


def merge_with_existing(region: str, collection: str, fresh: list[dict]) -> list[dict]:
    """Preserve first_seen, mark absent items gone. Mirrors the other scrapers."""
    today = datetime.now(timezone.utc).date().isoformat()
    out_path = DATA_DIR / region / f"{collection}.json"

    existing: dict[str, dict] = {}
    if out_path.exists():
        try:
            data = json.loads(out_path.read_text(encoding="utf-8"))
            for item in data.get("items", []):
                if item.get("uuid"):
                    existing[item["uuid"]] = item
        except (json.JSONDecodeError, OSError) as e:
            print(f"  warning: could not read existing {region}/{collection} ({e})", flush=True)

    fresh_uuids = set()
    merged: list[dict] = []
    for item in fresh:
        uuid = item["uuid"]
        fresh_uuids.add(uuid)
        prev = existing.get(uuid)
        item["first_seen"] = (prev or {}).get("first_seen") or today
        item["status"] = "active"

        # Track the best (lowest) points price ever seen, so a price drop is
        # visible rather than silently overwritten.
        prev_low = (prev or {}).get("lowest_points_price")
        current = item.get("points_price")
        if current and (not prev_low or current < prev_low):
            item["lowest_points_price"] = current
            item["lowest_points_date"] = today
        else:
            item["lowest_points_price"] = prev_low
            item["lowest_points_date"] = (prev or {}).get("lowest_points_date")

        merged.append(item)

    for uuid, prev in existing.items():
        if uuid in fresh_uuids:
            continue
        prev["status"] = "gone"
        prev.setdefault("gone_since", today)
        merged.append(prev)

    return merged


def write_file(region: str, collection: str, items: list[dict]) -> Path:
    out_dir = DATA_DIR / region
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{collection}.json"
    active = [i for i in items if i.get("status") == "active"]
    payload = {
        "region": region,
        "collection": collection,
        "currency": REGIONS[region][1],
        "updated": datetime.now(timezone.utc).isoformat(),
        "item_count": len(active),
        "items": items,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "sv,en;q=0.8",
    })

    failures = []
    for region in REGIONS:
        for collection in COLLECTIONS:
            try:
                fresh, empty_by_design = scrape_collection(region, collection, session)
            except Exception as e:
                print(f"  FAILED {region}/{collection}: {e}", flush=True)
                failures.append(f"{region}/{collection}")
                continue

            if not fresh and not empty_by_design:
                # Zero items but the page didn't say "none here" — treat as a
                # parse break (e.g. markup changed) rather than silently
                # wiping the collection.
                print(f"  FAILED {region}/{collection}: parsed zero items", flush=True)
                failures.append(f"{region}/{collection}")
                continue

            merged = merge_with_existing(region, collection, fresh)
            path = write_file(region, collection, merged)
            active = sum(1 for i in merged if i.get("status") == "active")
            gone = sum(1 for i in merged if i.get("status") == "gone")
            print(f"{region}/{collection}: {active} active, {gone} gone -> {path.relative_to(REPO_ROOT)}", flush=True)

    if failures:
        print(f"\nFailed: {', '.join(failures)}", flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
