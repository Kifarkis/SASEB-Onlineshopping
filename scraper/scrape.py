#!/usr/bin/env python3
"""SAS EuroBonus Shopping tracker — multi-country edition."""
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

COUNTRIES = [
    {"code": "SE", "local_lang": "sv", "name": "Sverige", "languages": ["sv", "en"]},
    {"code": "DK", "local_lang": "da", "name": "Danmark", "languages": ["da", "en"]},
    {"code": "NO", "local_lang": "nb", "name": "Norge",   "languages": ["nb", "en"]},
    {"code": "FI", "local_lang": "en", "name": "Suomi",   "languages": ["en"]},
]

API_BASE = "https://onlineshopping.loyaltykey.com/api/v1"
SHOPS_URL = API_BASE + "/shops?filter[channel]=SAS&filter[language]={lang}&filter[country]={country}&filter[amount]=5000"
CATEGORIES_URL = API_BASE + "/shops/categories?filter[language]={lang}"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
HTML_FILE = REPO_ROOT / "docs" / "index.html"

CLOUDFLARE_TOKEN = "c0d97a34f9524bd18f638693155d6704"

STRINGS = {
    "sv": {
        "title": "EuroBonus Shopping",
        "filter_all": "Alla", "filter_campaigns": "Kampanjer",
        "filter_ending": "Slutar snart", "filter_gone": "Borta",
        "category_all": "Alla kategorier",
        "sort_az": "A–Ö", "sort_za": "Ö–A", "sort_recent": "Senast tillagd",
        "sort_best_eb_fixed": "Bäst EB-poäng — fast",
        "sort_best_eb_variable": "Bäst EB-poäng — rörlig",
        "sort_best_level_fixed": "Bäst nivåpoäng — fast",
        "sort_best_level_variable": "Bäst nivåpoäng — rörlig",
        "search_placeholder": "Sök butik — t.ex. Lenovo, Amazon, Ellos…",
        "meta_template": "{campaigns} aktiva kampanjer · {new} nya denna vecka · {shops} butiker · uppdaterad {ts}",
        "dark_mode": "Dark mode", "light_mode": "Light mode",
        "no_shops": "Inga butiker matchar.",
        "no_gone": "Inga försvunna butiker ännu.",
        "level_label": "Nivå", "points_short": "p",
        "unit_per_purchase": "/ köp", "unit_per_hundred": "/ 100 kr",
        "new_campaign_title": "Ny kampanj", "gone_since": "borta sedan",
        "footer_unaffiliated": "Oberoende sida, inte ansluten till SAS eller EuroBonus.",
        "footer_about": "Om sidan", "footer_privacy": "Integritet",
        "modal_shop_at": "Handla hos", "modal_close": "Stäng",
        "modal_campaign_period": "Kampanjperiod", "modal_campaign_ends": "Slutar",
        "modal_open_external": "Öppna direkt",
    },
    "en": {
        "title": "EuroBonus Shopping",
        "filter_all": "All", "filter_campaigns": "Campaigns",
        "filter_ending": "Ending soon", "filter_gone": "Gone",
        "category_all": "All categories",
        "sort_az": "A–Z", "sort_za": "Z–A", "sort_recent": "Recently added",
        "sort_best_eb_fixed": "Best EB points — flat",
        "sort_best_eb_variable": "Best EB points — variable",
        "sort_best_level_fixed": "Best level points — flat",
        "sort_best_level_variable": "Best level points — variable",
        "search_placeholder": "Search shops — e.g. Lenovo, Amazon, Ellos…",
        "meta_template": "{campaigns} active campaigns · {new} new this week · {shops} shops · updated {ts}",
        "dark_mode": "Dark mode", "light_mode": "Light mode",
        "no_shops": "No shops match.",
        "no_gone": "No disappeared shops yet.",
        "level_label": "Level", "points_short": "p",
        "unit_per_purchase": "/ purchase", "unit_per_hundred": "/ 100",
        "new_campaign_title": "New campaign", "gone_since": "gone since",
        "footer_unaffiliated": "Independent site, not affiliated with SAS or EuroBonus.",
        "footer_about": "About", "footer_privacy": "Privacy",
        "modal_shop_at": "Shop at", "modal_close": "Close",
        "modal_campaign_period": "Campaign", "modal_campaign_ends": "Ends",
        "modal_open_external": "Open directly",
    },
    "da": {
        "title": "EuroBonus Shopping",
        "filter_all": "Alle", "filter_campaigns": "Kampagner",
        "filter_ending": "Slutter snart", "filter_gone": "Væk",
        "category_all": "Alle kategorier",
        "sort_az": "A–Å", "sort_za": "Å–A", "sort_recent": "Senest tilføjet",
        "sort_best_eb_fixed": "Bedst EB-point — fast",
        "sort_best_eb_variable": "Bedst EB-point — variabel",
        "sort_best_level_fixed": "Bedst niveaupoint — fast",
        "sort_best_level_variable": "Bedst niveaupoint — variabel",
        "search_placeholder": "Søg butik — fx Lenovo, Amazon, Ellos…",
        "meta_template": "{campaigns} aktive kampagner · {new} nye denne uge · {shops} butikker · opdateret {ts}",
        "dark_mode": "Dark mode", "light_mode": "Light mode",
        "no_shops": "Ingen butikker matcher.",
        "no_gone": "Ingen forsvundne butikker endnu.",
        "level_label": "Niveau", "points_short": "p",
        "unit_per_purchase": "/ køb", "unit_per_hundred": "/ 100 kr",
        "new_campaign_title": "Ny kampagne", "gone_since": "væk siden",
        "footer_unaffiliated": "Uafhængig side, ikke tilknyttet SAS eller EuroBonus.",
        "footer_about": "Om", "footer_privacy": "Privatliv",
        "modal_shop_at": "Køb hos", "modal_close": "Luk",
        "modal_campaign_period": "Kampagne", "modal_campaign_ends": "Slutter",
        "modal_open_external": "Åbn direkte",
    },
    "nb": {
        "title": "EuroBonus Shopping",
        "filter_all": "Alle", "filter_campaigns": "Kampanjer",
        "filter_ending": "Slutter snart", "filter_gone": "Borte",
        "category_all": "Alle kategorier",
        "sort_az": "A–Å", "sort_za": "Å–A", "sort_recent": "Sist lagt til",
        "sort_best_eb_fixed": "Best EB-poeng — fast",
        "sort_best_eb_variable": "Best EB-poeng — variabel",
        "sort_best_level_fixed": "Best nivåpoeng — fast",
        "sort_best_level_variable": "Best nivåpoeng — variabel",
        "search_placeholder": "Søk butikk — f.eks. Lenovo, Amazon, Ellos…",
        "meta_template": "{campaigns} aktive kampanjer · {new} nye denne uken · {shops} butikker · oppdatert {ts}",
        "dark_mode": "Dark mode", "light_mode": "Light mode",
        "no_shops": "Ingen butikker matcher.",
        "no_gone": "Ingen forsvunne butikker ennå.",
        "level_label": "Nivå", "points_short": "p",
        "unit_per_purchase": "/ kjøp", "unit_per_hundred": "/ 100 kr",
        "new_campaign_title": "Ny kampanje", "gone_since": "borte siden",
        "footer_unaffiliated": "Uavhengig side, ikke tilknyttet SAS eller EuroBonus.",
        "footer_about": "Om", "footer_privacy": "Personvern",
        "modal_shop_at": "Handle hos", "modal_close": "Lukk",
        "modal_campaign_period": "Kampanje", "modal_campaign_ends": "Slutter",
        "modal_open_external": "Åpne direkte",
    },
}

ENDS_PATTERNS_EN = [
    (r"^om (\d+) dag$", r"in \1 day"),
    (r"^om (\d+) dagar$", r"in \1 days"),
    (r"^om (\d+) dage$", r"in \1 days"),
    (r"^om (\d+) vecka$", r"in \1 week"),
    (r"^om (\d+) veckor$", r"in \1 weeks"),
    (r"^om (\d+) uge$", r"in \1 week"),
    (r"^om (\d+) uger$", r"in \1 weeks"),
    (r"^om (\d+) uke$", r"in \1 week"),
    (r"^om (\d+) uker$", r"in \1 weeks"),
    (r"^om (\d+) timmar$", r"in \1 hours"),
    (r"^om (\d+) timme$", r"in \1 hour"),
    (r"^om (\d+) timer$", r"in \1 hours"),
    (r"^om (\d+) time$", r"in \1 hour"),
    (r"^om (\d+) minuter$", r"in \1 minutes"),
    (r"^om (\d+) minutter$", r"in \1 minutes"),
    (r"^om (\d+) minut$", r"in \1 minute"),
    (r"^om (\d+) minutt$", r"in \1 minute"),
    (r"^idag$", "today"),
]

def fetch_json(url):
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; sas-shopping-tracker/2.0)",
        "Accept": "application/json",
    })
    with urlopen(req, timeout=30) as response:
        return json.load(response)

def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default

def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

def today_iso():
    return date.today().isoformat()

def best_logo(shop):
    """Prefer the production branded image_url, fall back to logo, then None."""
    return shop.get("image_url") or shop.get("logo")

def translate_ends_en(text):
    if not text:
        return text
    stripped = text.strip()
    for pattern, replacement in ENDS_PATTERNS_EN:
        if re.match(pattern, stripped, re.IGNORECASE):
            return re.sub(pattern, replacement, stripped, flags=re.IGNORECASE)
    return text

_DESC_DANGER_TAGS = "(?:script|iframe|object|embed|style|form|input|button|link|meta)"
_DESC_DANGER_BLOCK = re.compile(rf"<{_DESC_DANGER_TAGS}\b[^>]*>.*?</[^>]+>", re.IGNORECASE | re.DOTALL)
_DESC_DANGER_VOID = re.compile(rf"<{_DESC_DANGER_TAGS}\b[^>]*/?>", re.IGNORECASE)
_DESC_ON_HANDLER = re.compile(r'\son\w+\s*=\s*("[^"]*"|\'[^\']*\'|[^\s>]+)', re.IGNORECASE)
_DESC_JS_HREF = re.compile(r'href\s*=\s*("javascript:[^"]*"|\'javascript:[^\']*\')', re.IGNORECASE)

def sanitize_description(raw_html):
    """Strip dangerous HTML; preserve formatting tags like <strong>, <p>, <ul>."""
    if not raw_html:
        return ""
    cleaned = _DESC_DANGER_BLOCK.sub("", raw_html)
    cleaned = _DESC_DANGER_VOID.sub("", cleaned)
    cleaned = _DESC_ON_HANDLER.sub("", cleaned)
    cleaned = _DESC_JS_HREF.sub("", cleaned)
    return cleaned

def update_state(api_shops, shops_state, history):
    """Merge the latest API snapshot into the persistent shop state.
    Detects new campaigns, ended campaigns, new shops, gone shops."""
    today = today_iso()
    api_uuids = {s["uuid"] for s in api_shops}
    counts = {"new_shops": 0, "new_campaigns": 0, "ended_campaigns": 0, "gone_shops": 0}

    for s in api_shops:
        uuid = s["uuid"]
        prev = shops_state.get(uuid, {})
        is_new = not prev

        has_campaign_now = s.get("has_campaign") == 1
        had_campaign_before = bool(prev.get("active_campaign"))
        current_points = s.get("points") or 0
        points_campaign = s.get("points_campaign") or 0
        points_channel = s.get("points_channel") or 0

        effective_max = max(current_points, points_campaign)
        prev_high = prev.get("all_time_high_points") or 0
        if effective_max > prev_high:
            all_time_high_points = effective_max
            all_time_high_date = today
        else:
            all_time_high_points = prev_high
            all_time_high_date = prev.get("all_time_high_date") or today

        active_campaign = prev.get("active_campaign")
        if has_campaign_now:
            ends_date = s.get("campaign_ends_date")
            if not had_campaign_before:
                active_campaign = {
                    "started": today,
                    "ends_date": ends_date,
                    "points_campaign": points_campaign,
                    "points_channel": points_channel,
                }
                counts["new_campaigns"] += 1
            else:
                active_campaign = {
                    **active_campaign,
                    "ends_date": ends_date,
                    "points_campaign": points_campaign,
                    "points_channel": points_channel,
                }
        else:
            if had_campaign_before:
                history.append({
                    "uuid": uuid,
                    "name": s.get("name") or prev.get("name"),
                    "started": active_campaign.get("started"),
                    "ended": today,
                    "points_campaign": active_campaign.get("points_campaign"),
                    "points_channel": active_campaign.get("points_channel"),
                })
                counts["ended_campaigns"] += 1
            active_campaign = None

        shops_state[uuid] = {
            "uuid": uuid,
            "name": s.get("name"),
            "slug": s.get("slug"),
            "logo": best_logo(s),
            "description": s.get("description"),
            "first_seen": prev.get("first_seen") or today,
            "last_seen": today,
            "status": "active",
            "current_points": current_points,
            "current_points_channel": points_channel,
            "current_points_campaign": points_campaign if has_campaign_now else 0,
            "currency": s.get("currency"),
            "commission_type": s.get("commission_type"),
            "category_id": s.get("categoryId"),
            "all_time_high_points": all_time_high_points,
            "all_time_high_date": all_time_high_date,
            "active_campaign": active_campaign,
            "campaign_ends_human": s.get("campaign_ends") if has_campaign_now else None,
            "campaign_ends_human_en": translate_ends_en(s.get("campaign_ends")) if has_campaign_now else None,
        }
        if is_new:
            counts["new_shops"] += 1

    for uuid, shop in shops_state.items():
        if uuid in api_uuids or shop.get("status") == "gone":
            continue
        shop["status"] = "gone"
        shop["gone_since"] = today
        counts["gone_shops"] += 1
        if shop.get("active_campaign"):
            history.append({
                "uuid": uuid,
                "name": shop.get("name"),
                "started": shop["active_campaign"].get("started"),
                "ended": today,
                "points_campaign": shop["active_campaign"].get("points_campaign"),
                "points_channel": shop["active_campaign"].get("points_channel"),
                "note": "shop_disappeared",
            })
            shop["active_campaign"] = None

    return shops_state, history, counts

def category_slug_from_name(name):
    if not name:
        return "uncategorized"
    return (
        name.lower()
        .replace("å", "a").replace("ä", "a").replace("ö", "o")
        .replace("æ", "ae").replace("ø", "o")
        .replace(" ", "-").replace("/", "-").replace("&", "and")
    )

def build_category_map(categories_data):
    """Return {category_id: {slug, name}} from the API response."""
    items = categories_data.get("data", []) if isinstance(categories_data, dict) else []
    mapping = {}
    for cat in items:
        cid = cat.get("category_id") or cat.get("id")
        if cid is None:
            continue
        name = cat.get("name") or f"Category {cid}"
        mapping[cid] = {
            "slug": cat.get("slug") or category_slug_from_name(name),
            "name": name,
        }
    return mapping

def points_display(shop):
    """Compute the points/level/bonus to display for a shop, taking active campaigns into account."""
    is_variable = shop.get("commission_type") == "variable"
    if shop.get("active_campaign"):
        camp = shop["active_campaign"]
        main = camp.get("points_campaign") or 0
        base = shop.get("current_points") or 0
        return {
            "main": main,
            "bonus": max(main - base, 0),
            "level": camp.get("points_channel") or 0,
            "show_campaign": True,
            "unit_variable": is_variable,
        }
    return {
        "main": shop.get("current_points") or 0,
        "bonus": 0,
        "level": shop.get("current_points_channel") or 0,
        "show_campaign": False,
        "unit_variable": is_variable,
    }

def prepare_country_dataset(shops_state, category_map):
    """Serialize shop state into the compact JSON shape consumed by the frontend."""
    shops_out = []
    for uuid, s in shops_state.items():
        disp = points_display(s)
        cat = category_map.get(
            s.get("category_id"),
            {"slug": "uncategorized", "name": "Uncategorized"},
        )
        ac = s.get("active_campaign") or {}
        shops_out.append({
            "uuid": uuid,
            "name": s.get("name"),
            "logo": s.get("logo"),
            "description": sanitize_description(s.get("description")),
            "status": s.get("status"),
            "category_slug": cat["slug"],
            "category_name": cat["name"],
            "main": disp["main"],
            "bonus": disp["bonus"],
            "level": disp["level"],
            "unit_variable": disp["unit_variable"],
            "has_campaign": disp["show_campaign"],
            "campaign_ends_human": s.get("campaign_ends_human"),
            "campaign_ends_human_en": s.get("campaign_ends_human_en"),
            "campaign_started": ac.get("started"),
            "campaign_ends_date": ac.get("ends_date"),
            "first_seen": s.get("first_seen"),
            "gone_since": s.get("gone_since"),
        })

    used_cat_ids = {
        s.get("category_id") for s in shops_state.values()
        if s.get("status") == "active" and s.get("category_id") is not None
    }
    category_list = sorted(
        ({"slug": category_map[cid]["slug"], "name": category_map[cid]["name"]}
         for cid in used_cat_ids if cid in category_map),
        key=lambda c: c["name"].lower(),
    )

    return {
        "shops": shops_out,
        "categories": category_list,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

def render_html(datasets):
    """Generate the full single-page HTML with embedded data and client-side rendering."""
    payload = {
        "datasets": json.dumps(datasets, ensure_ascii=False),
        "strings": json.dumps(STRINGS, ensure_ascii=False),
        "countries": json.dumps(COUNTRIES, ensure_ascii=False),
        "default_country": "SE",
        "default_lang": "sv",
        "cf_token": CLOUDFLARE_TOKEN,
    }

    return _HTML_TEMPLATE.format(**payload)

# Raw string: regex escapes (\b, \d, \w, \s) ship to the JS untouched.
# Do not remove the leading r — without it, Python turns \b into ASCII backspace
# (0x08), which silently breaks every regex word boundary in the embedded JS.
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="sv">
<head>
<meta charset="utf-8">
<meta name="description" content="Översikt över aktuella SAS EuroBonus Shopping-kampanjer i Sverige, Danmark, Norge och Finland. Hitta butiker med extra poäng och bonusar — uppdateras var sjätte timme.">
<meta property="og:title" content="EuroBonus Shopping — aktuella kampanjer">
<meta property="og:description" content="Översikt över aktuella SAS EuroBonus Shopping-kampanjer i Norden. Hitta butiker med extra poäng — uppdateras var sjätte timme.">
<meta property="og:type" content="website">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>EuroBonus Shopping</title>
<style>
:root {{
  --bg: #faf9f7; --surface: #ffffff;
  --border: rgba(0, 0, 0, 0.08); --border-strong: rgba(0, 0, 0, 0.16);
  --text: #1a1a1a; --text-muted: #595959; --text-faint: #9a9a9a;
  --accent: #1f6feb; --accent-bg: rgba(31, 111, 235, 0.08);
  --warn: #b85c00;
  --shadow-sticky: 0 4px 12px rgba(0, 0, 0, 0.06);
}}
html[data-theme="dark"] {{
  --bg: #0f0f10; --surface: #1a1a1c;
  --border: rgba(255, 255, 255, 0.08); --border-strong: rgba(255, 255, 255, 0.18);
  --text: #ededed; --text-muted: #b3b3b3; --text-faint: #666666;
  --accent: #6ea8ff; --accent-bg: rgba(110, 168, 255, 0.14);
  --warn: #f0a66c;
  --shadow-sticky: 0 4px 16px rgba(0, 0, 0, 0.4);
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  margin: 0; padding: 0 0 64px;
  font-size: 14px; line-height: 1.5;
}}
.sas-container {{ max-width: 1500px; margin: 0 auto; padding: 0 24px; }}

.sas-sticky-wrap {{ background: var(--bg); transition: box-shadow 0.2s ease, border-color 0.2s ease, padding-top 0.2s ease, padding-bottom 0.2s ease; padding-top: 32px; }}
.sas-sticky-wrap.is-stuck {{ box-shadow: var(--shadow-sticky); border-bottom: 0.5px solid var(--border); padding-top: 16px; padding-bottom: 8px; }}
@media (min-width: 641px) {{
  .sas-sticky-wrap {{ position: sticky; top: 0; z-index: 50; }}
}}

.sas-header {{ display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 24px; gap: 24px; flex-wrap: wrap; transition: margin-bottom 0.2s ease; }}
.sas-sticky-wrap.is-stuck .sas-header {{ margin-bottom: 12px; }}
.sas-meta {{ font-size: 14px; color: var(--text-muted); font-family: ui-monospace, "SF Mono", Menlo, monospace; max-height: 40px; opacity: 1; overflow: hidden; transition: max-height 0.2s ease, opacity 0.2s ease, margin 0.2s ease; }}
.sas-sticky-wrap.is-stuck .sas-meta {{ max-height: 0; opacity: 0; margin: 0; }}
.sas-title {{ font-size: 28px; font-weight: 500; letter-spacing: -0.02em; margin: 0 0 4px 0; transition: font-size 0.2s ease; }}
.sas-sticky-wrap.is-stuck .sas-title {{ font-size: 20px; }}
.sas-header-controls {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
.sas-header-select {{ font-family: inherit; font-size: 13px; padding: 7px 12px; border: 0.5px solid var(--border-strong); border-radius: 999px; background: var(--surface); color: var(--text); cursor: pointer; }}
.sas-toggle {{ background: none; border: 0.5px solid var(--border-strong); color: var(--text-muted); padding: 7px 14px; border-radius: 999px; font-size: 13px; cursor: pointer; font-family: inherit; }}
.sas-toggle:hover {{ color: var(--text); }}

.sas-filter-row {{ display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }}
.sas-chip {{ font-size: 14px; padding: 8px 16px; border: 0.5px solid var(--border); border-radius: 999px; background: transparent; color: var(--text-muted); cursor: pointer; font-family: inherit; white-space: nowrap; }}
.sas-chip:hover {{ color: var(--text); }}
.sas-chip.active {{ background: var(--surface); color: var(--text); border-color: var(--border-strong); }}
.sas-controls-right {{ margin-left: auto; display: flex; gap: 8px; flex-wrap: wrap; }}
.sas-list-control {{ font-family: inherit; font-size: 14px; padding: 8px 14px; border: 0.5px solid var(--border); border-radius: 999px; background: var(--surface); color: var(--text); cursor: pointer; }}
.sas-list-control:hover {{ border-color: var(--border-strong); }}
.sas-list-control:disabled {{ opacity: 0.4; cursor: not-allowed; }}

.sas-search {{ width: 100%; font-size: 16px; padding: 13px 18px; border: 0.5px solid var(--border); border-radius: 8px; background: var(--surface); color: var(--text); font-family: inherit; margin-bottom: 28px; }}
.sas-sticky-wrap.is-stuck .sas-search {{ margin-bottom: 0; padding: 10px 14px; font-size: 14px; }}
.sas-search:focus {{ outline: none; border-color: var(--border-strong); }}

.sas-jumper {{ display: flex; gap: 2px; padding: 6px 0 14px 0; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-faint); flex-wrap: wrap; }}
.sas-jumper-letter {{ padding: 4px 8px; cursor: pointer; border-radius: 4px; user-select: none; }}
.sas-jumper-letter:hover {{ color: var(--text); }}
.sas-jumper-letter.active {{ color: var(--text); background: var(--surface); }}

.sas-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; min-height: 150vh; }}
.sas-card {{ background: var(--surface); border: 0.5px solid var(--border); border-radius: 12px; padding: 18px 20px; display: flex; flex-direction: column; gap: 14px; min-height: 140px; color: inherit; transition: border-color 0.12s, transform 0.12s; cursor: pointer; position: relative; scroll-margin-top: 220px; }}
.sas-card:hover {{ border-color: var(--border-strong); transform: translateY(-1px); }}
.sas-card.campaign {{ border-color: var(--accent); }}
.sas-card-top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }}
.sas-card-identity {{ display: flex; align-items: center; gap: 12px; min-width: 0; }}
.sas-card-external {{ position: absolute; top: 12px; right: 12px; width: 28px; height: 28px; display: flex; align-items: center; justify-content: center; border-radius: 6px; color: var(--text-faint); background: transparent; border: none; cursor: pointer; padding: 0; }}
.sas-card-external:hover {{ color: var(--text); background: var(--bg); }}
.sas-card-external svg {{ width: 14px; height: 14px; }}

.sas-logo-wrap {{ display: flex; align-items: center; justify-content: center; background: #d1d5db; flex-shrink: 0; overflow: hidden; width: 48px; height: 48px; border-radius: 10px; }}
.sas-logo-img {{ width: 100%; height: 100%; object-fit: cover; }}
.sas-logo-fallback {{ width: 100%; height: 100%; color: #555; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 600; letter-spacing: -0.02em; }}
html[data-theme="dark"] .sas-logo-wrap {{ background: #9ca3af; }}

.sas-card-name {{ font-size: 17px; font-weight: 500; line-height: 1.25; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; padding-right: 36px; }}
.sas-new-dot {{ width: 7px; height: 7px; border-radius: 50%; background: var(--accent); flex-shrink: 0; position: absolute; top: 22px; right: 48px; }}
.sas-points-block {{ display: flex; flex-direction: column; gap: 8px; }}
.sas-points-row {{ display: flex; align-items: baseline; gap: 6px; flex-wrap: wrap; }}
.sas-points-main {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 26px; font-weight: 500; letter-spacing: -0.02em; line-height: 1; }}
.sas-eb-tag {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 11px; font-weight: 500; color: var(--text-faint); letter-spacing: 0.04em; }}
.sas-pill {{ display: inline-block; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; padding: 2px 8px; background: var(--accent-bg); color: var(--accent); border-radius: 999px; }}
.sas-points-unit {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-faint); }}
.sas-status-row {{ display: flex; align-items: baseline; gap: 6px; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 14px; }}
.sas-status-label {{ color: var(--text-faint); font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; }}
.sas-status-val {{ color: var(--text-muted); }}
.sas-card-foot {{ font-size: 14px; color: var(--text-muted); font-family: ui-monospace, "SF Mono", Menlo, monospace; padding-top: 10px; border-top: 0.5px solid var(--border); margin-top: auto; min-height: 32px; display: flex; align-items: center; }}
.sas-card-foot.empty {{ display: none; }}
.sas-days.urgent {{ color: var(--warn); }}
.sas-card-gone {{ opacity: 0.55; cursor: default; }}
.sas-card-gone .sas-points-main {{ font-size: 14px; }}

.sas-hidden {{ display: none !important; }}
.sas-empty {{ color: var(--text-faint); font-size: 14px; padding: 16px 0; }}

.sas-modal-backdrop {{ position: fixed; inset: 0; background: rgba(0, 0, 0, 0.45); z-index: 100; display: flex; align-items: flex-end; justify-content: center; opacity: 0; pointer-events: none; transition: opacity 0.2s ease; }}
.sas-modal-backdrop.open {{ opacity: 1; pointer-events: auto; }}
.sas-modal {{ background: var(--surface); width: 100%; max-width: 560px; border-radius: 20px 20px 0 0; max-height: 85vh; display: flex; flex-direction: column; transform: translateY(100%); transition: transform 0.25s ease; overflow: hidden; }}
.sas-modal-backdrop.open .sas-modal {{ transform: translateY(0); }}
.sas-modal-handle {{ width: 36px; height: 4px; border-radius: 2px; background: var(--border-strong); margin: 10px auto 0; flex-shrink: 0; }}
.sas-modal-body {{ overflow-y: auto; padding: 20px 24px 100px; flex: 1; }}
.sas-modal-head {{ display: flex; gap: 14px; align-items: center; margin-bottom: 18px; }}
.sas-modal-head .sas-logo-wrap {{ width: 56px; height: 56px; border-radius: 12px; }}
.sas-modal-title {{ font-size: 22px; font-weight: 500; letter-spacing: -0.01em; margin: 0 0 4px 0; }}
.sas-modal-category {{ font-size: 12px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.08em; }}
.sas-modal-stats {{ display: flex; flex-direction: column; gap: 10px; padding: 14px 16px; background: var(--bg); border-radius: 10px; margin-bottom: 18px; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 13px; }}
.sas-modal-stat-row {{ display: flex; justify-content: space-between; color: var(--text-muted); }}
.sas-modal-stat-row strong {{ color: var(--text); font-weight: 500; }}
.sas-modal-description {{ font-size: 14px; line-height: 1.6; color: var(--text-muted); }}
.sas-modal-description p {{ margin: 0 0 12px 0; }}
.sas-modal-description strong {{ color: var(--text); font-weight: 500; }}
.sas-modal-description a {{ color: var(--accent); }}
.sas-modal-description h1, .sas-modal-description h2, .sas-modal-description h3 {{ font-size: 15px; font-weight: 500; color: var(--text); margin: 16px 0 8px 0; }}
.sas-modal-description ul, .sas-modal-description ol {{ margin: 8px 0 12px 20px; padding: 0; }}
.sas-modal-description li {{ margin-bottom: 4px; }}
.sas-modal-footer {{ position: absolute; bottom: 0; left: 0; right: 0; padding: 14px 20px calc(14px + env(safe-area-inset-bottom, 0px)); background: var(--surface); border-top: 0.5px solid var(--border); display: flex; gap: 10px; }}
.sas-modal-primary {{ flex: 1; background: var(--accent); color: #fff; padding: 14px; border: 0; border-radius: 10px; font-size: 15px; font-weight: 500; cursor: pointer; text-decoration: none; text-align: center; font-family: inherit; }}
.sas-modal-primary:hover {{ opacity: 0.9; }}
.sas-modal-close {{ background: none; border: 0.5px solid var(--border-strong); color: var(--text-muted); padding: 14px 18px; border-radius: 10px; font-size: 14px; cursor: pointer; font-family: inherit; }}
.sas-modal-close:hover {{ color: var(--text); }}

.sas-footer {{ display: flex; justify-content: space-between; align-items: center; margin-top: 64px; padding-top: 24px; border-top: 0.5px solid var(--border); gap: 16px; flex-wrap: wrap; font-size: 12px; color: var(--text-faint); }}
.sas-footer a {{ color: var(--text-muted); text-decoration: none; }}
.sas-footer a:hover {{ color: var(--text); text-decoration: underline; }}
.sas-footer-right {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}

@media (max-width: 640px) {{
  body {{ padding: 0 0 48px; }}
  .sas-container {{ padding: 0 14px; }}
  .sas-sticky-wrap {{ padding-top: 20px; }}
  .sas-card {{ padding: 16px; min-height: 120px; }}
  .sas-points-main {{ font-size: 24px; }}
  .sas-controls-right {{ margin-left: 0; width: 100%; }}
  .sas-list-control {{ flex: 1; }}
  .sas-footer {{ flex-direction: column; align-items: flex-start; gap: 8px; }}
  .sas-modal {{ max-height: 90vh; }}
}}
</style>
</head>
<body>
<div class="sas-sticky-wrap" id="sticky-wrap">
  <div class="sas-container">
    <div class="sas-header">
      <div>
        <h1 class="sas-title" id="title-text">EuroBonus Shopping</h1>
        <div class="sas-meta" id="meta-text"></div>
      </div>
      <div class="sas-header-controls">
        <select class="sas-header-select" id="country-select" aria-label="Country"></select>
        <select class="sas-header-select" id="language-select" aria-label="Language"></select>
        <button class="sas-toggle" id="theme-toggle">Dark mode</button>
      </div>
    </div>
    <div class="sas-filter-row">
      <div id="view-filters" style="display: flex; gap: 8px; flex-wrap: wrap;"></div>
      <div class="sas-controls-right">
        <select id="category-select" class="sas-list-control" aria-label="Category"></select>
        <select id="sort-select" class="sas-list-control" aria-label="Sort"></select>
      </div>
    </div>
    <input class="sas-search" id="search-box" type="search">
  </div>
</div>

<main class="sas-container">
  <div class="sas-jumper" id="jumper"></div>
  <div class="sas-grid" id="shop-grid"></div>
  <div class="sas-empty sas-hidden" id="empty-state"></div>
</main>

<div class="sas-container">
  <footer class="sas-footer">
    <div><span id="footer-unaffiliated"></span></div>
    <div class="sas-footer-right">
      <a href="about.html" id="footer-about">About</a> ·
      <a href="privacy.html" id="footer-privacy">Privacy</a> ·
      <span>© 2026 David Kifarkis</span>
    </div>
  </footer>
</div>

<div class="sas-modal-backdrop" id="modal-backdrop">
  <div class="sas-modal" id="modal">
    <div class="sas-modal-handle"></div>
    <div class="sas-modal-body" id="modal-body"></div>
    <div class="sas-modal-footer">
      <a class="sas-modal-primary" id="modal-shop-btn" target="_blank" rel="noopener"></a>
      <button class="sas-modal-close" id="modal-close" aria-label="Close"></button>
    </div>
  </div>
</div>

<script id="sas-data" type="application/json">{datasets}</script>
<script id="sas-strings" type="application/json">{strings}</script>
<script id="sas-countries" type="application/json">{countries}</script>

<script>
(function() {{
  var DATA = JSON.parse(document.getElementById('sas-data').textContent);
  var STRINGS = JSON.parse(document.getElementById('sas-strings').textContent);
  var COUNTRIES = JSON.parse(document.getElementById('sas-countries').textContent);
  var DEFAULT_COUNTRY = '{default_country}';
  var DEFAULT_LANG = '{default_lang}';

  var params = new URLSearchParams(window.location.search);
  var country = (params.get('c') || DEFAULT_COUNTRY).toUpperCase();
  var lang = params.get('l') || DEFAULT_LANG;
  var countryDef = COUNTRIES.find(function(c) {{ return c.code === country; }}) || COUNTRIES[0];
  if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;

  var state = {{ view: 'all', category: 'all', query: '', sort: 'az' }};
  var shopsByUuid = {{}};

  var root = document.documentElement;
  var toggle = document.getElementById('theme-toggle');
  function prefersDark() {{ return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches; }}
  function isDark() {{
    var t = root.getAttribute('data-theme');
    return t === 'dark' || (t !== 'light' && prefersDark());
  }}
  try {{
    var stored = localStorage.getItem('sas-theme');
    if (stored === 'dark' || stored === 'light') root.setAttribute('data-theme', stored);
  }} catch (e) {{}}
  toggle.addEventListener('click', function() {{
    var next = isDark() ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    try {{ localStorage.setItem('sas-theme', next); }} catch (e) {{}}
    setToggleLabel();
  }});

  function t(key) {{ return (STRINGS[lang] || STRINGS.en)[key] || key; }}
  function setToggleLabel() {{ toggle.textContent = isDark() ? t('light_mode') : t('dark_mode'); }}

  function initials(name) {{
    var parts = (name || '?').split(/\s+/).filter(Boolean);
    if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
    return (name || '?').substring(0, 2).toUpperCase();
  }}

  function logoHTML(shop) {{
    if (shop.logo) {{
      return '<div class="sas-logo-wrap"><img src="' + shop.logo + '" alt="" class="sas-logo-img" loading="lazy"></div>';
    }}
    return '<div class="sas-logo-wrap"><div class="sas-logo-fallback">' + initials(shop.name) + '</div></div>';
  }}

  function unit(shop) {{ return shop.unit_variable ? t('unit_per_hundred') : t('unit_per_purchase'); }}

  function endsText(shop) {{
    if (lang === 'en' && shop.campaign_ends_human_en) return shop.campaign_ends_human_en;
    return shop.campaign_ends_human || '';
  }}

  function isUrgent(text) {{
    if (!text) return false;
    var l = text.toLowerCase();
    return /\b(\d+ time|\d+ timer|timme|timmar|hour|hours|minut\w*|1 dag|1 day|1 dage|idag|today)\b/.test(l);
  }}

  function shopUrl(uuid) {{
    return 'https://onlineshopping.flysas.com/sv-SE/butiker/about-you/' + encodeURIComponent(uuid);
  }}

  function isVariableSort(s) {{ return s === 'best_eb_variable' || s === 'best_level_variable'; }}
  function isFixedSort(s) {{ return s === 'best_eb_fixed' || s === 'best_level_fixed'; }}

  var backdrop = document.getElementById('modal-backdrop');
  var modalBody = document.getElementById('modal-body');
  var modalShopBtn = document.getElementById('modal-shop-btn');
  var modalClose = document.getElementById('modal-close');

  function openModal(shop) {{
    var rows = [];
    rows.push('<div class="sas-modal-stat-row"><span>EB ' + unit(shop).replace('/', '').trim() + '</span><strong>' + shop.main + ' EB</strong></div>');
    if (shop.bonus > 0) rows.push('<div class="sas-modal-stat-row"><span>Bonus</span><strong>+' + shop.bonus + ' EB</strong></div>');
    rows.push('<div class="sas-modal-stat-row"><span>' + t('level_label') + '</span><strong>' + shop.level + ' ' + t('points_short') + '</strong></div>');

    if (shop.has_campaign && shop.campaign_started) {{
      var range = shop.campaign_started + (shop.campaign_ends_date ? ' → ' + shop.campaign_ends_date : '');
      rows.push('<div class="sas-modal-stat-row"><span>' + t('modal_campaign_period') + '</span><strong>' + range + '</strong></div>');
    }}
    var et = endsText(shop);
    if (shop.has_campaign && et) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + t('modal_campaign_ends') + '</span><strong>' + et + '</strong></div>');
    }}

    modalBody.innerHTML =
      '<div class="sas-modal-head">' + logoHTML(shop) +
      '<div><div class="sas-modal-category">' + (shop.category_name || '') + '</div>' +
      '<h2 class="sas-modal-title">' + shop.name + '</h2></div></div>' +
      '<div class="sas-modal-stats">' + rows.join('') + '</div>' +
      '<div class="sas-modal-description">' + (shop.description || '') + '</div>';

    modalShopBtn.href = shopUrl(shop.uuid);
    modalShopBtn.textContent = t('modal_shop_at') + ' ' + shop.name;
    modalClose.textContent = t('modal_close');
    backdrop.classList.add('open');
    document.body.style.overflow = 'hidden';
  }}

  function closeModal() {{
    backdrop.classList.remove('open');
    document.body.style.overflow = '';
  }}

  backdrop.addEventListener('click', function(e) {{ if (e.target === backdrop) closeModal(); }});
  modalClose.addEventListener('click', closeModal);
  document.addEventListener('keydown', function(e) {{ if (e.key === 'Escape') closeModal(); }});

  function cardHTML(shop, ds) {{
    var today = new Date(ds.updated.split(' ')[0]);
    var started = shop.campaign_started ? new Date(shop.campaign_started) : null;
    var daysAgo = started ? Math.floor((today - started) / 86400000) : null;
    var isNew = shop.has_campaign && daysAgo !== null && daysAgo <= 2;
    var et = endsText(shop);
    var urgent = shop.has_campaign && et && isUrgent(et);

    var div = document.createElement('div');
    div.className = 'sas-card' + (shop.has_campaign ? ' campaign' : '');
    div.dataset.uuid = shop.uuid;
    div.dataset.name = (shop.name || '').toLowerCase();
    div.dataset.cat = shop.category_slug || '';
    div.dataset.campaign = shop.has_campaign ? '1' : '0';
    div.dataset.urgent = urgent ? '1' : '0';
    div.dataset.points = shop.main;
    div.dataset.level = shop.level;
    div.dataset.unitVariable = shop.unit_variable ? '1' : '0';
    div.dataset.firstSeen = shop.first_seen || '';
    div.dataset.letter = (shop.name || '#').charAt(0).toUpperCase();

    var bonusPill = shop.bonus > 0 ? '<span class="sas-pill">+' + shop.bonus + '</span>' : '';
    var newDot = isNew ? '<div class="sas-new-dot" title="' + t('new_campaign_title') + '"></div>' : '';
    var daysHTML = (shop.has_campaign && et) ? '<span class="sas-days ' + (urgent ? 'urgent' : '') + '">' + et + '</span>' : '';
    var footClass = 'sas-card-foot' + (daysHTML ? '' : ' empty');

    div.innerHTML =
      '<button class="sas-card-external" title="' + t('modal_open_external') + '" data-external-uuid="' + shop.uuid + '">' +
      '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3H3v10h10v-3"/><path d="M10 3h3v3"/><path d="M8 8l5-5"/></svg>' +
      '</button>' + newDot +
      '<div class="sas-card-top"><div class="sas-card-identity">' + logoHTML(shop) +
      '<div class="sas-card-name">' + shop.name + '</div></div></div>' +
      '<div class="sas-points-block">' +
      '<div class="sas-points-row"><span class="sas-points-main">' + shop.main + '</span>' +
      '<span class="sas-eb-tag">EB</span>' + bonusPill +
      '<span class="sas-points-unit">' + unit(shop) + '</span></div>' +
      '<div class="sas-status-row"><span class="sas-status-label">' + t('level_label') + '</span>' +
      '<span class="sas-status-val">' + shop.level + ' ' + t('points_short') + '</span></div></div>' +
      '<div class="' + footClass + '">' + daysHTML + '</div>';
    return div;
  }}

  function goneCardHTML(shop) {{
    var div = document.createElement('div');
    div.className = 'sas-card sas-card-gone';
    div.dataset.uuid = shop.uuid;
    div.dataset.name = (shop.name || '').toLowerCase();
    div.dataset.cat = shop.category_slug || '';
    div.innerHTML =
      '<div class="sas-card-top"><div class="sas-card-identity">' + logoHTML(shop) +
      '<div class="sas-card-name">' + shop.name + '</div></div></div>' +
      '<div class="sas-points-block"><div class="sas-points-row">' +
      '<span class="sas-points-main">' + t('gone_since') + ' ' + (shop.gone_since || '') + '</span></div></div>';
    return div;
  }}

  function getDataset() {{ return DATA[country] || Object.values(DATA)[0]; }}

  function buildShopList(ds) {{
    if (state.view === 'gone') {{
      return ds.shops.filter(function(s) {{ return s.status === 'gone'; }})
        .sort(function(a, b) {{ return (b.gone_since || '').localeCompare(a.gone_since || ''); }});
    }}

    var shops = ds.shops.filter(function(s) {{ return s.status === 'active'; }});

    if (state.view === 'campaigns') shops = shops.filter(function(s) {{ return s.has_campaign; }});
    else if (state.view === 'ending') shops = shops.filter(function(s) {{ return s.has_campaign && isUrgent(endsText(s)); }});

    if (isVariableSort(state.sort)) shops = shops.filter(function(s) {{ return s.unit_variable; }});
    else if (isFixedSort(state.sort)) shops = shops.filter(function(s) {{ return !s.unit_variable; }});

    if (state.category !== 'all') shops = shops.filter(function(s) {{ return s.category_slug === state.category; }});
    if (state.query) shops = shops.filter(function(s) {{ return (s.name || '').toLowerCase().indexOf(state.query) !== -1; }});

    if (state.sort === 'az') shops.sort(function(a, b) {{ return (a.name || '').localeCompare(b.name || '', lang); }});
    else if (state.sort === 'za') shops.sort(function(a, b) {{ return (b.name || '').localeCompare(a.name || '', lang); }});
    else if (state.sort === 'recent') shops.sort(function(a, b) {{ return (b.first_seen || '').localeCompare(a.first_seen || ''); }});
    else if (state.sort === 'best_eb_fixed' || state.sort === 'best_eb_variable') shops.sort(function(a, b) {{ return b.main - a.main; }});
    else if (state.sort === 'best_level_fixed' || state.sort === 'best_level_variable') shops.sort(function(a, b) {{ return b.level - a.level; }});

    return shops;
  }}

  function renderJumper(shops) {{
    var jumper = document.getElementById('jumper');
    jumper.innerHTML = '';
    if (state.view === 'gone' || !shops.length) return;
    if (state.sort !== 'az' && state.sort !== 'za') return;

    var letters = {{}};
    shops.forEach(function(s) {{
      var ltr = (s.name || '#').charAt(0).toUpperCase();
      if (/[A-ZÅÄÖ0-9]/.test(ltr)) letters[ltr] = true;
    }});
    var sorted = Object.keys(letters).sort();
    if (state.sort === 'za') sorted.reverse();

    sorted.forEach(function(ltr) {{
      var s = document.createElement('span');
      s.className = 'sas-jumper-letter';
      s.dataset.letter = ltr;
      s.textContent = ltr;
      s.addEventListener('click', function() {{
        document.querySelectorAll('#jumper .sas-jumper-letter').forEach(function(x) {{ x.classList.remove('active'); }});
        s.classList.add('active');
        var cards = document.querySelectorAll('#shop-grid .sas-card');
        for (var i = 0; i < cards.length; i++) {{
          if ((cards[i].dataset.letter || '').toUpperCase() === ltr) {{
            cards[i].scrollIntoView({{ behavior: 'smooth', block: 'start' }});
            break;
          }}
        }}
      }});
      jumper.appendChild(s);
    }});
  }}

  function renderGrid() {{
    var ds = getDataset();
    var grid = document.getElementById('shop-grid');
    var emptyState = document.getElementById('empty-state');
    var sortSel = document.getElementById('sort-select');

    document.querySelectorAll('#view-filters .sas-chip').forEach(function(c) {{
      c.classList.toggle('active', c.dataset.view === state.view);
    }});
    sortSel.disabled = state.view === 'gone';

    var shops = buildShopList(ds);
    grid.innerHTML = '';
    var renderFn = state.view === 'gone' ? goneCardHTML : function(s) {{ return cardHTML(s, ds); }};
    shops.forEach(function(s) {{ grid.appendChild(renderFn(s)); }});

    if (!shops.length) {{
      emptyState.classList.remove('sas-hidden');
      emptyState.textContent = state.view === 'gone' ? t('no_gone') : t('no_shops');
    }} else {{
      emptyState.classList.add('sas-hidden');
    }}

    renderJumper(shops);
  }}

  function render() {{
    var ds = getDataset();
    document.documentElement.lang = lang;
    document.getElementById('title-text').textContent = t('title');
    setToggleLabel();

    shopsByUuid = {{}};
    ds.shops.forEach(function(s) {{ shopsByUuid[s.uuid] = s; }});

    var active = ds.shops.filter(function(s) {{ return s.status === 'active'; }});
    var gone = ds.shops.filter(function(s) {{ return s.status === 'gone'; }});
    var campaigns = active.filter(function(s) {{ return s.has_campaign; }});
    var newThisWeek = campaigns.filter(function(s) {{
      if (!s.campaign_started) return false;
      var days = Math.floor((new Date(ds.updated.split(' ')[0]) - new Date(s.campaign_started)) / 86400000);
      return days >= 0 && days <= 7;
    }}).length;

    document.getElementById('meta-text').textContent = t('meta_template')
      .replace('{{campaigns}}', campaigns.length)
      .replace('{{new}}', newThisWeek)
      .replace('{{shops}}', active.length)
      .replace('{{ts}}', ds.updated);
    document.getElementById('search-box').placeholder = t('search_placeholder');
    document.getElementById('footer-unaffiliated').textContent = t('footer_unaffiliated');
    document.getElementById('footer-about').textContent = t('footer_about');
    document.getElementById('footer-privacy').textContent = t('footer_privacy');

    var sortSel = document.getElementById('sort-select');
    sortSel.innerHTML = '';
    [
      ['az', 'sort_az'], ['za', 'sort_za'], ['recent', 'sort_recent'],
      ['best_eb_fixed', 'sort_best_eb_fixed'],
      ['best_eb_variable', 'sort_best_eb_variable'],
      ['best_level_fixed', 'sort_best_level_fixed'],
      ['best_level_variable', 'sort_best_level_variable'],
    ].forEach(function(pair) {{
      var o = document.createElement('option');
      o.value = pair[0]; o.textContent = t(pair[1]);
      sortSel.appendChild(o);
    }});
    sortSel.value = state.sort;

    var viewFilters = document.getElementById('view-filters');
    viewFilters.innerHTML = '';
    [
      ['all', t('filter_all')],
      ['campaigns', t('filter_campaigns')],
      ['ending', t('filter_ending')],
      ['gone', t('filter_gone') + ' (' + gone.length + ')'],
    ].forEach(function(pair) {{
      var b = document.createElement('button');
      b.className = 'sas-chip' + (state.view === pair[0] ? ' active' : '');
      b.dataset.view = pair[0];
      b.textContent = pair[1];
      b.addEventListener('click', function() {{ state.view = pair[0]; renderGrid(); }});
      viewFilters.appendChild(b);
    }});

    var catSel = document.getElementById('category-select');
    catSel.innerHTML = '<option value="all">' + t('category_all') + '</option>';
    ds.categories.forEach(function(c) {{
      var o = document.createElement('option');
      o.value = c.slug; o.textContent = c.name;
      catSel.appendChild(o);
    }});
    catSel.value = state.category;
    catSel.onchange = function() {{ state.category = catSel.value; renderGrid(); }};

    renderGrid();
  }}

  document.addEventListener('click', function(e) {{
    var ext = e.target.closest('[data-external-uuid]');
    if (ext) {{
      e.stopPropagation();
      var sh = shopsByUuid[ext.dataset.externalUuid];
      if (sh) window.open(shopUrl(sh.uuid), '_blank', 'noopener');
      return;
    }}
    var card = e.target.closest('.sas-card[data-uuid]');
    if (card && !card.classList.contains('sas-card-gone')) {{
      var sh = shopsByUuid[card.dataset.uuid];
      if (sh) openModal(sh);
    }}
  }});

  var countrySel = document.getElementById('country-select');
  COUNTRIES.forEach(function(c) {{
    var o = document.createElement('option');
    o.value = c.code; o.textContent = c.name;
    countrySel.appendChild(o);
  }});
  countrySel.value = country;

  var langSel = document.getElementById('language-select');
  function rebuildLangSelector() {{
    var labels = {{ en: 'English', sv: 'Svenska', da: 'Dansk', nb: 'Norsk', fi: 'Suomi' }};
    langSel.innerHTML = '';
    countryDef.languages.forEach(function(l) {{
      var o = document.createElement('option');
      o.value = l; o.textContent = labels[l] || l;
      langSel.appendChild(o);
    }});
    langSel.value = lang;
  }}
  rebuildLangSelector();

  function updateUrl() {{
    var url = new URL(window.location);
    url.searchParams.set('c', country);
    url.searchParams.set('l', lang);
    window.history.replaceState({{}}, '', url);
  }}

  countrySel.addEventListener('change', function() {{
    country = countrySel.value;
    countryDef = COUNTRIES.find(function(c) {{ return c.code === country; }});
    if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;
    rebuildLangSelector();
    state.category = 'all';
    state.query = '';
    document.getElementById('search-box').value = '';
    updateUrl();
    render();
  }});
  langSel.addEventListener('change', function() {{
    lang = langSel.value;
    updateUrl();
    render();
  }});

  document.getElementById('search-box').addEventListener('input', function(e) {{
    state.query = e.target.value.trim().toLowerCase();
    renderGrid();
  }});
  document.getElementById('sort-select').addEventListener('change', function(e) {{
    state.sort = e.target.value;
    renderGrid();
  }});

  var stickyWrap = document.getElementById('sticky-wrap');
  var ticking = false;
  window.addEventListener('scroll', function() {{
    if (!ticking) {{
      window.requestAnimationFrame(function() {{
        stickyWrap.classList.toggle('is-stuck', window.scrollY > 20);
        ticking = false;
      }});
      ticking = true;
    }}
  }}, {{ passive: true }});

  updateUrl();
  render();
}})();
</script>

<!-- Cloudflare Web Analytics -->
<script defer src='https://static.cloudflareinsights.com/beacon.min.js' data-cf-beacon='{{"token": "{cf_token}"}}'></script>
<!-- End Cloudflare Web Analytics -->

<!-- GoatCounter -->
<script data-goatcounter="https://eurobonus.goatcounter.com/count" async src="//gc.zgo.at/count.js"></script>
<!-- End GoatCounter -->

</body>
</html>
"""

def main():
    datasets = {}
    all_succeeded = True

    for country in COUNTRIES:
        code = country["code"]
        lang = country["local_lang"]
        country_dir = DATA_DIR / code.lower()

        print(f"\n=== {code} ({lang}) ===")

        try:
            shops_payload = fetch_json(SHOPS_URL.format(lang=lang, country=code))
        except Exception as e:
            print(f"  Shops fetch failed: {e}", file=sys.stderr)
            all_succeeded = False
            continue

        api_shops = shops_payload.get("data", [])
        print(f"  Got {len(api_shops)} shops")

        try:
            cats_payload = fetch_json(CATEGORIES_URL.format(lang=lang))
        except Exception as e:
            print(f"  Categories fetch failed ({e}); using fallback", file=sys.stderr)
            cats_payload = {"data": []}

        category_map = build_category_map(cats_payload)

        shops_state = load_json(country_dir / "shops.json", {})
        history = load_json(country_dir / "history.json", [])
        shops_state, history, counts = update_state(api_shops, shops_state, history)
        print(
            f"  Transitions: {counts['new_shops']} new shops, "
            f"{counts['new_campaigns']} new campaigns, "
            f"{counts['ended_campaigns']} ended, "
            f"{counts['gone_shops']} newly gone"
        )

        save_json(country_dir / "shops.json", shops_state)
        save_json(country_dir / "history.json", history)
        save_json(country_dir / "categories.json", cats_payload)

        datasets[code] = prepare_country_dataset(shops_state, category_map)

    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    HTML_FILE.write_text(render_html(datasets), encoding="utf-8")
    print(f"\nWrote {HTML_FILE} with {len(datasets)} country datasets")

    if not all_succeeded:
        sys.exit(1)

if __name__ == "__main__":
    main()
