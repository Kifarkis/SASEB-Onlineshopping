#!/usr/bin/env python3
"""
SAS EuroBonus Shopping tracker.

Fetches current offers from loyaltykey.com every few hours and maintains
persistent state for history-based features (all-time highs, gone shops,
campaign history). Renders a static HTML tracker to docs/index.html.
"""
import html
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

API_SHOPS = (
    "https://onlineshopping.loyaltykey.com/api/v1/shops"
    "?filter[channel]=SAS"
    "&filter[language]=sv"
    "&filter[country]=SE"
    "&filter[amount]=5000"
)
API_CATEGORIES = (
    "https://onlineshopping.loyaltykey.com/api/v1/shops/categories"
    "?filter[language]=sv"
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SHOPS_FILE = REPO_ROOT / "data" / "shops.json"
HISTORY_FILE = REPO_ROOT / "data" / "history.json"
CATEGORIES_FILE = REPO_ROOT / "data" / "categories.json"
HTML_FILE = REPO_ROOT / "docs" / "index.html"


def fetch_json(url):
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; sas-shopping-tracker/1.0)",
        "Accept": "application/json",
    })
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def load_json(path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default
    return default


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def today_iso():
    return date.today().isoformat()


def update_state(api_shops, shops_state, history):
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
            "logo": s.get("logo"),
            "first_seen": prev.get("first_seen") or today,
            "last_seen": today,
            "status": "active",
            "current_points": current_points,
            "current_points_channel": points_channel,
            "current_points_campaign": points_campaign if has_campaign_now else 0,
            "currency": s.get("currency"),
            "commission_type": s.get("commission_type"),
            "category_id": s.get("categoryId"),
            "description": s.get("description"),
            "all_time_high_points": all_time_high_points,
            "all_time_high_date": all_time_high_date,
            "active_campaign": active_campaign,
            "campaign_ends_human": s.get("campaign_ends") if has_campaign_now else None,
        }
        if is_new:
            counts["new_shops"] += 1

    for uuid, shop in shops_state.items():
        if uuid in api_uuids:
            continue
        if shop.get("status") != "gone":
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


def points_display(shop):
    commission_type = shop.get("commission_type")
    unit = "/ 100 kr" if commission_type == "variable" else "/ köp"
    if shop.get("active_campaign"):
        camp = shop["active_campaign"]
        main = camp.get("points_campaign") or 0
        base = shop.get("current_points") or 0
        bonus = max(main - base, 0)
        return {
            "main": main, "bonus": bonus, "unit": unit,
            "level": camp.get("points_channel") or 0,
            "show_campaign": True,
            "commission_type": commission_type,
        }
    return {
        "main": shop.get("current_points") or 0,
        "bonus": 0, "unit": unit,
        "level": shop.get("current_points_channel") or 0,
        "show_campaign": False,
        "commission_type": commission_type,
    }


def escape(s):
    return html.escape(str(s or ""), quote=True)


def initials_from_name(name):
    parts = [p for p in (name or "?").split() if p]
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()
    return (name or "?")[:2].upper()


def logo_html(shop, size_class=""):
    logo_url = shop.get("logo")
    if logo_url:
        return (
            f'<img src="{escape(logo_url)}" alt="" '
            f'class="sas-logo-img {size_class}" loading="lazy">'
        )
    return f'<div class="sas-logo-fallback {size_class}">{escape(initials_from_name(shop.get("name")))}</div>'


def card_html(shop, discovered_today, category_slug):
    disp = points_display(shop)
    name = escape(shop.get("name"))
    uuid = escape(shop.get("uuid"))
    shop_url = f"https://onlineshopping.flysas.com/sv-SE/butiker/about-you/{uuid}"
    campaign_class = " campaign" if disp["show_campaign"] else ""
    new_dot = '<div class="sas-new-dot" title="Ny kampanj"></div>' if discovered_today else ""
    bonus_pill = f'<span class="sas-pill">+{disp["bonus"]}</span>' if disp["bonus"] > 0 else ""

    ends_text = escape(shop.get("campaign_ends_human") or "")
    days_remaining = ""
    if disp["show_campaign"] and ends_text:
        lower = ends_text.lower()
        urgent = "urgent" if ("1 dag" in lower or "idag" in lower or "timmar" in lower) else ""
        days_remaining = f'<span class="sas-days {urgent}">{ends_text}</span>'

    search_key = escape((shop.get("name") or "").lower())
    return (
        f'<a class="sas-card{campaign_class}" href="{shop_url}" target="_blank" rel="noopener" '
        f'data-name="{search_key}" data-cat="{escape(category_slug)}" '
        f'data-campaign="{1 if disp["show_campaign"] else 0}" '
        f'data-urgent="{1 if "urgent" in days_remaining else 0}">'
        f'<div class="sas-card-top">'
        f'  <div class="sas-card-identity">{logo_html(shop, "logo-lg")}<div class="sas-card-name">{name}</div></div>'
        f'  {new_dot}'
        f'</div>'
        f'<div class="sas-points-block">'
        f'  <div class="sas-points-row">'
        f'    <span class="sas-points-main">{disp["main"]}</span>'
        f'    <span class="sas-eb-tag">EB</span>'
        f'    {bonus_pill}'
        f'    <span class="sas-points-unit">{disp["unit"]}</span>'
        f'  </div>'
        f'  <div class="sas-status-row">'
        f'    <span class="sas-status-label">Nivå</span>'
        f'    <span class="sas-status-val">{disp["level"]} p</span>'
        f'  </div>'
        f'</div>'
        f'<div class="sas-card-foot">{days_remaining}</div>'
        f'</a>'
    )


def list_row_html(shop, category_slug, bar_percent):
    disp = points_display(shop)
    name = escape(shop.get("name"))
    uuid = escape(shop.get("uuid"))
    shop_url = f"https://onlineshopping.flysas.com/sv-SE/butiker/about-you/{uuid}"
    search_key = escape((shop.get("name") or "").lower())
    first_letter = (name[:1] or "#").upper()
    return (
        f'<a class="sas-list-row" href="{shop_url}" target="_blank" rel="noopener" '
        f'data-name="{search_key}" data-cat="{escape(category_slug)}" '
        f'data-letter="{escape(first_letter)}" '
        f'data-points="{disp["main"]}" data-level="{disp["level"]}" '
        f'data-first-seen="{escape(shop.get("first_seen") or "")}">'
        f'  <div class="sas-list-logo-wrap">{logo_html(shop, "logo-md")}</div>'
        f'  <div class="sas-list-name">{name}</div>'
        f'  <div class="sas-list-bar"><div class="sas-list-bar-fill" style="width: {bar_percent}%;"></div></div>'
        f'  <div class="sas-list-points">{disp["main"]} EB {disp["unit"]}</div>'
        f'  <div class="sas-list-level">{disp["level"]} nivå</div>'
        f'</a>'
    )


def gone_row_html(shop):
    name = escape(shop.get("name"))
    gone_since = escape(shop.get("gone_since") or "")
    last_seen = escape(shop.get("last_seen") or "")
    return (
        f'<div class="sas-list-row sas-list-row-gone">'
        f'  <div class="sas-list-logo-wrap">{logo_html(shop, "logo-md")}</div>'
        f'  <div class="sas-list-name">{name}</div>'
        f'  <div class="sas-list-bar"></div>'
        f'  <div class="sas-list-points">borta sedan {gone_since or last_seen}</div>'
        f'  <div class="sas-list-level"></div>'
        f'</div>'
    )


def category_slug_from_name(name):
    if not name:
        return "okategoriserad"
    return (
        name.lower()
        .replace("å", "a").replace("ä", "a").replace("ö", "o")
        .replace(" ", "-").replace("/", "-").replace("&", "och")
    )


def build_category_map(categories_data):
    """Map categoryId (int) -> {slug, name}."""
    mapping = {}
    items = categories_data.get("data", []) if isinstance(categories_data, dict) else []
    for cat in items:
        cid = cat.get("category_id") or cat.get("id")
        name = cat.get("name") or f"Kategori {cid}"
        slug = cat.get("slug") or category_slug_from_name(name)
        if cid is not None:
            mapping[cid] = {"slug": slug, "name": name}
    return mapping


def render_html(shops_state, category_map):
    today = today_iso()
    active = [s for s in shops_state.values() if s.get("status") == "active"]
    gone = [s for s in shops_state.values() if s.get("status") == "gone"]

    def cat_info(shop):
        cid = shop.get("category_id")
        if cid in category_map:
            return category_map[cid]
        return {"slug": "okategoriserad", "name": "Okategoriserad"}

    campaigns = [s for s in active if s.get("active_campaign")]
    campaigns.sort(key=lambda s: (
        0 if s.get("commission_type") == "fixed" else 1,
        -(points_display(s)["main"]),
        (s.get("name") or "").lower(),
    ))

    non_campaign = [s for s in active if not s.get("active_campaign")]
    non_campaign.sort(key=lambda s: (s.get("name") or "").lower())

    gone.sort(key=lambda s: s.get("gone_since") or "", reverse=True)

    max_fixed = max((points_display(s)["main"] for s in non_campaign if s.get("commission_type") == "fixed"), default=1)
    max_variable = max((points_display(s)["main"] for s in non_campaign if s.get("commission_type") == "variable"), default=1)

    def bar_percent(shop):
        disp = points_display(shop)
        m = max_fixed if shop.get("commission_type") == "fixed" else max_variable
        if not m:
            return 0
        return round(min(100, (disp["main"] / m) * 100))

    used_category_ids = {s.get("category_id") for s in active if s.get("category_id") is not None}
    categories_in_use = [
        (category_map[cid]["slug"], category_map[cid]["name"])
        for cid in used_category_ids if cid in category_map
    ]
    categories_in_use.sort(key=lambda x: x[1].lower())

    campaign_cards = "".join(
        card_html(
            s,
            discovered_today=(s["active_campaign"].get("started") == today),
            category_slug=cat_info(s)["slug"],
        )
        for s in campaigns
    )
    all_rows = "".join(
        list_row_html(s, cat_info(s)["slug"], bar_percent(s))
        for s in non_campaign
    )
    gone_rows = "".join(gone_row_html(s) for s in gone)

    letters_present = sorted({(s.get("name") or "#")[:1].upper() for s in non_campaign})
    jumper_html = "".join(
        f'<span class="sas-jumper-letter" data-letter="{escape(ltr)}">{escape(ltr)}</span>'
        for ltr in letters_present if ltr.isalpha() or ltr.isdigit()
    )

category_options_html = '<option value="all">Alla kategorier</option>' + "".join(
        f'<option value="{escape(slug)}">{escape(name)}</option>'
        for slug, name in categories_in_use
    )

    updated_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total_active = len(active)
    total_campaigns = len(campaigns)
    total_gone = len(gone)

    return f"""<!DOCTYPE html>
<html lang="sv">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>EuroBonus Shopping — kampanjer</title>
<style>
:root {{
  --bg: #faf9f7; --surface: #ffffff;
  --border: rgba(0, 0, 0, 0.08); --border-strong: rgba(0, 0, 0, 0.16);
  --text: #1a1a1a; --text-muted: #6b6b6b; --text-faint: #9a9a9a;
  --accent: #1f6feb; --accent-bg: rgba(31, 111, 235, 0.08);
  --warn: #b85c00;
}}
html[data-theme="dark"] {{
  --bg: #0f0f10; --surface: #1a1a1c;
  --border: rgba(255, 255, 255, 0.08); --border-strong: rgba(255, 255, 255, 0.18);
  --text: #ededed; --text-muted: #a0a0a0; --text-faint: #666666;
  --accent: #6ea8ff; --accent-bg: rgba(110, 168, 255, 0.14);
  --warn: #f0a66c;
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
  background: var(--bg); color: var(--text);
  margin: 0; padding: 32px 24px 64px;
  font-size: 14px; line-height: 1.5;
}}
.sas-container {{ max-width: 1200px; margin: 0 auto; }}
.sas-header {{ display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 24px; gap: 24px; flex-wrap: wrap; }}
.sas-title {{ font-size: 28px; font-weight: 500; letter-spacing: -0.02em; margin: 0 0 4px 0; }}
.sas-meta {{ font-size: 14px; color: var(--text-muted); font-family: ui-monospace, "SF Mono", Menlo, monospace; }}
.sas-toggle {{ background: none; border: 0.5px solid var(--border-strong); color: var(--text-muted); padding: 7px 14px; border-radius: 999px; font-size: 13px; cursor: pointer; font-family: inherit; }}
.sas-toggle:hover {{ color: var(--text); }}

.sas-filter-row {{ display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }}
.sas-chip {{ font-size: 14px; padding: 8px 16px; border: 0.5px solid var(--border); border-radius: 999px; background: transparent; color: var(--text-muted); cursor: pointer; font-family: inherit; white-space: nowrap; }}
.sas-chip:hover {{ color: var(--text); }}
.sas-chip.active {{ background: var(--surface); color: var(--text); border-color: var(--border-strong); }}
.sas-category-wrap {{ margin-left: auto; }}
.sas-category-select {{ font-family: inherit; font-size: 14px; padding: 8px 14px; border: 0.5px solid var(--border); border-radius: 999px; background: var(--surface); color: var(--text); cursor: pointer; }}
.sas-category-select:hover {{ border-color: var(--border-strong); }}

.sas-search {{ width: 100%; font-size: 16px; padding: 13px 18px; border: 0.5px solid var(--border); border-radius: 8px; background: var(--surface); color: var(--text); font-family: inherit; margin-bottom: 28px; }}
.sas-search:focus {{ outline: none; border-color: var(--border-strong); }}

.sas-section-label {{ font-size: 12px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-faint); margin: 32px 0 14px 0; }}
.sas-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; }}
.sas-card {{ background: var(--surface); border: 0.5px solid var(--border); border-radius: 12px; padding: 18px 20px; display: flex; flex-direction: column; gap: 14px; min-height: 140px; text-decoration: none; color: inherit; transition: border-color 0.12s, transform 0.12s; }}
.sas-card:hover {{ border-color: var(--border-strong); transform: translateY(-1px); }}
.sas-card.campaign {{ border-color: var(--accent); }}
.sas-card-top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }}
.sas-card-identity {{ display: flex; align-items: center; gap: 12px; min-width: 0; }}
.sas-logo-img.logo-lg, .sas-logo-fallback.logo-lg {{ width: 32px; height: 32px; border-radius: 6px; }}
.sas-logo-img.logo-md, .sas-logo-fallback.logo-md {{ width: 28px; height: 28px; border-radius: 5px; }}
.sas-logo-img {{ object-fit: contain; background: var(--bg); }}
.sas-logo-fallback {{ background: var(--bg); color: var(--text-muted); display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 500; letter-spacing: -0.02em; }}
.sas-card-name {{ font-size: 17px; font-weight: 500; line-height: 1.25; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.sas-new-dot {{ width: 7px; height: 7px; border-radius: 50%; background: var(--accent); flex-shrink: 0; margin-top: 11px; }}
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
.sas-days.urgent {{ color: var(--warn); }}

.sas-list-controls {{ display: flex; align-items: center; gap: 16px; margin-bottom: 4px; flex-wrap: wrap; }}
.sas-list-sort {{ display: flex; align-items: center; gap: 10px; font-size: 13px; color: var(--text-muted); }}
.sas-list-sort select {{ font-family: inherit; font-size: 13px; padding: 6px 10px; border: 0.5px solid var(--border); border-radius: 8px; background: var(--surface); color: var(--text); }}
.sas-jumper {{ display: flex; gap: 2px; padding: 6px 0 14px 0; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-faint); flex-wrap: wrap; }}
.sas-jumper-letter {{ padding: 4px 8px; cursor: pointer; border-radius: 4px; user-select: none; }}
.sas-jumper-letter:hover {{ color: var(--text); }}
.sas-jumper-letter.active {{ color: var(--text); background: var(--surface); }}

.sas-list {{ display: flex; flex-direction: column; }}
.sas-list-row {{ display: grid; grid-template-columns: 32px 1fr 80px auto auto; gap: 16px; align-items: center; padding: 14px 4px; border-bottom: 0.5px solid var(--border); text-decoration: none; color: inherit; scroll-margin-top: 20px; }}
.sas-list-row:hover .sas-list-name {{ color: var(--accent); }}
.sas-list-row-gone {{ opacity: 0.55; }}
.sas-list-logo-wrap {{ width: 28px; height: 28px; }}
.sas-list-name {{ font-size: 16px; }}
.sas-list-bar {{ height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; width: 80px; }}
.sas-list-bar-fill {{ height: 100%; background: var(--text-muted); border-radius: 2px; opacity: 0.45; }}
.sas-list-points {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 14px; font-weight: 500; color: var(--text-muted); min-width: 140px; text-align: right; }}
.sas-list-level {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 13px; color: var(--text-faint); min-width: 90px; text-align: right; }}

.sas-hidden {{ display: none !important; }}
.sas-empty {{ color: var(--text-faint); font-size: 14px; padding: 16px 0; }}

@media (max-width: 640px) {{
  body {{ padding: 20px 14px 48px; }}
  .sas-list-row {{ grid-template-columns: 28px 1fr auto; gap: 12px; }}
  .sas-list-level, .sas-list-bar {{ display: none; }}
  .sas-list-points {{ min-width: 110px; font-size: 13px; }}
  .sas-card {{ padding: 16px; min-height: 120px; }}
  .sas-points-main {{ font-size: 24px; }}
}}
</style>
</head>
<body>
<div class="sas-container">
  <div class="sas-header">
    <div>
      <h1 class="sas-title">EuroBonus Shopping</h1>
      <div class="sas-meta">{total_campaigns} aktiva kampanjer · {total_active} butiker · uppdaterad {updated_ts}</div>
    </div>
    <button class="sas-toggle" id="theme-toggle">Dark mode</button>
  </div>

<div class="sas-filter-row">
    <div id="view-filters" style="display: flex; gap: 8px; flex-wrap: wrap;">
      <button class="sas-chip active" data-view="all">Alla</button>
      <button class="sas-chip" data-view="campaigns">Kampanjer</button>
      <button class="sas-chip" data-view="ending">Slutar snart</button>
      <button class="sas-chip" data-view="gone">Borta ({total_gone})</button>
    </div>
    <div class="sas-category-wrap">
      <select id="category-select" class="sas-category-select" aria-label="Kategori">
        {category_options_html}
      </select>
    </div>
  </div>

  <input class="sas-search" id="search-box" type="search" placeholder="Sök butik — t.ex. Lenovo, Amazon, Ellos…">

  <section data-section="campaigns">
    <div class="sas-section-label">Aktiva kampanjer</div>
    <div class="sas-grid" id="campaign-grid">{campaign_cards or '<div class="sas-empty">Inga aktiva kampanjer just nu.</div>'}</div>
  </section>

  <section data-section="all-shops">
    <div class="sas-section-label">Alla butiker</div>
    <div class="sas-list-controls">
      <div class="sas-list-sort">
        <span>Sortera</span>
        <select id="sort-select">
          <option value="az">A–Ö</option>
          <option value="za">Ö–A</option>
          <option value="points-desc">Mest EB-poäng</option>
          <option value="level-desc">Mest nivåpoäng</option>
          <option value="recent">Senast tillagd</option>
        </select>
      </div>
    </div>
    <div class="sas-jumper" id="jumper">{jumper_html}</div>
    <div class="sas-list" id="shop-list">{all_rows}</div>
  </section>

  <section data-section="gone" class="sas-hidden">
    <div class="sas-section-label">Butiker som försvunnit</div>
    <div class="sas-list">{gone_rows or '<div class="sas-empty">Inga försvunna butiker ännu.</div>'}</div>
  </section>
</div>

<script>
(function() {{
  var root = document.documentElement;
  var toggle = document.getElementById('theme-toggle');
  function prefersDark() {{ return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches; }}
  function isDark() {{
    var t = root.getAttribute('data-theme');
    if (t === 'dark') return true;
    if (t === 'light') return false;
    return prefersDark();
  }}
  function setToggleLabel() {{ toggle.textContent = isDark() ? 'Light mode' : 'Dark mode'; }}
  var stored = null;
  try {{ stored = localStorage.getItem('sas-theme'); }} catch (e) {{}}
  if (stored === 'dark' || stored === 'light') root.setAttribute('data-theme', stored);
  setToggleLabel();
  toggle.addEventListener('click', function() {{
    var next = isDark() ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    try {{ localStorage.setItem('sas-theme', next); }} catch (e) {{}}
    setToggleLabel();
  }});

  var state = {{ view: 'all', category: 'all', query: '', sort: 'az' }};
  var cards = Array.prototype.slice.call(document.querySelectorAll('#campaign-grid .sas-card'));
  var listEl = document.getElementById('shop-list');
  var rows = Array.prototype.slice.call(listEl.querySelectorAll('.sas-list-row'));
  var campaignsSection = document.querySelector('[data-section="campaigns"]');
  var allShopsSection = document.querySelector('[data-section="all-shops"]');
  var goneSection = document.querySelector('[data-section="gone"]');
  var viewChips = document.querySelectorAll('#view-filters .sas-chip');
  var categorySelect = document.getElementById('category-select');
  var searchBox = document.getElementById('search-box');
  var sortSelect = document.getElementById('sort-select');
  var jumperLetters = document.querySelectorAll('#jumper .sas-jumper-letter');

  function matchesQuery(el) {{
    if (!state.query) return true;
    return (el.dataset.name || '').indexOf(state.query) !== -1;
  }}
  function matchesCategory(el) {{
    if (state.category === 'all') return true;
    return el.dataset.cat === state.category;
  }}

  function applyFilters() {{
    campaignsSection.classList.remove('sas-hidden');
    allShopsSection.classList.remove('sas-hidden');
    goneSection.classList.add('sas-hidden');

    if (state.view === 'gone') {{
      campaignsSection.classList.add('sas-hidden');
      allShopsSection.classList.add('sas-hidden');
      goneSection.classList.remove('sas-hidden');
      return;
    }}
    if (state.view === 'campaigns') {{
      allShopsSection.classList.add('sas-hidden');
    }}

    cards.forEach(function(c) {{
      var show = matchesQuery(c) && matchesCategory(c);
      if (state.view === 'ending') show = show && c.dataset.urgent === '1';
      c.style.display = show ? '' : 'none';
    }});
    rows.forEach(function(r) {{
      var show = matchesQuery(r) && matchesCategory(r);
      r.style.display = show ? '' : 'none';
    }});
    if (state.view === 'ending') {{
      allShopsSection.classList.add('sas-hidden');
    }}
  }}

  viewChips.forEach(function(c) {{
    c.addEventListener('click', function() {{
      viewChips.forEach(function(x) {{ x.classList.remove('active'); }});
      c.classList.add('active');
      state.view = c.dataset.view;
      applyFilters();
    }});
  }});
  categorySelect.addEventListener('change', function() {{
    state.category = categorySelect.value;
    applyFilters();
  }});
  searchBox.addEventListener('input', function() {{
    state.query = searchBox.value.trim().toLowerCase();
    applyFilters();
  }});

  function sortRows() {{
    var sorted = rows.slice();
    if (state.sort === 'az') sorted.sort(function(a, b) {{ return a.dataset.name.localeCompare(b.dataset.name, 'sv'); }});
    else if (state.sort === 'za') sorted.sort(function(a, b) {{ return b.dataset.name.localeCompare(a.dataset.name, 'sv'); }});
    else if (state.sort === 'points-desc') sorted.sort(function(a, b) {{ return Number(b.dataset.points) - Number(a.dataset.points); }});
    else if (state.sort === 'level-desc') sorted.sort(function(a, b) {{ return Number(b.dataset.level) - Number(a.dataset.level); }});
    else if (state.sort === 'recent') sorted.sort(function(a, b) {{ return (b.dataset.firstSeen || '').localeCompare(a.dataset.firstSeen || ''); }});
    sorted.forEach(function(r) {{ listEl.appendChild(r); }});
  }}
  sortSelect.addEventListener('change', function() {{
    state.sort = sortSelect.value;
    sortRows();
  }});

  jumperLetters.forEach(function(l) {{
    l.addEventListener('click', function() {{
      var letter = l.dataset.letter;
      jumperLetters.forEach(function(x) {{ x.classList.remove('active'); }});
      l.classList.add('active');
      for (var i = 0; i < rows.length; i++) {{
        var r = rows[i];
        if (r.style.display === 'none') continue;
        if ((r.dataset.name || '').charAt(0).toUpperCase() === letter) {{
          r.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
          break;
        }}
      }}
    }});
  }});
}})();
</script>
</body>
</html>
"""


def main():
    print(f"Fetching {API_SHOPS}")
    try:
        shops_payload = fetch_json(API_SHOPS)
    except Exception as e:
        print(f"Shops fetch failed: {e}", file=sys.stderr)
        sys.exit(1)
    api_shops = shops_payload.get("data", [])
    print(f"Got {len(api_shops)} shops from API")
    if len(api_shops) < 50:
        print(f"WARNING: only {len(api_shops)} shops — pagination may have kicked in", file=sys.stderr)

    try:
        cats_payload = fetch_json(API_CATEGORIES)
        save_json(CATEGORIES_FILE, cats_payload)
    except Exception as e:
        print(f"Categories fetch failed ({e}); using cached.", file=sys.stderr)
        cats_payload = load_json(CATEGORIES_FILE, {"data": []})
    category_map = build_category_map(cats_payload)
    print(f"Loaded {len(category_map)} categories")

    shops_state = load_json(SHOPS_FILE, {})
    history = load_json(HISTORY_FILE, [])
    shops_state, history, counts = update_state(api_shops, shops_state, history)
    print(
        f"Transitions: {counts['new_shops']} new shops, {counts['new_campaigns']} new campaigns, "
        f"{counts['ended_campaigns']} ended, {counts['gone_shops']} newly gone"
    )

    save_json(SHOPS_FILE, shops_state)
    save_json(HISTORY_FILE, history)
    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    HTML_FILE.write_text(render_html(shops_state, category_map), encoding="utf-8")
    print(f"Wrote {SHOPS_FILE}, {HISTORY_FILE}, {CATEGORIES_FILE}, and {HTML_FILE}")


if __name__ == "__main__":
    main()
