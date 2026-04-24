#!/usr/bin/env python3
"""
SAS EuroBonus Shopping tracker.
Fetches current offers from loyaltykey.com, tracks which campaigns
are new since last run, and generates a static HTML page.
"""
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

API_BASE = (
    "https://onlineshopping.loyaltykey.com/api/v1/shops"
    "?filter[channel]=SAS"
    "&filter[language]=sv"
    "&filter[country]=SE"
    "&filter[amount]=5000"
)

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_FILE = REPO_ROOT / "data" / "offers.json"
HTML_FILE = REPO_ROOT / "public" / "index.html"


def fetch_shops():
    """
    Fetch all shops. We ask for amount=5000 which the API treats as 'give
    me everything in one page'. If that ever stops working we'd need to
    follow the `links.next` chain, but for now this matches what the
    official SAS site does.
    """
    req = Request(API_BASE, headers={
        "User-Agent": "Mozilla/5.0 (compatible; sas-shopping-tracker/1.0)",
        "Accept": "application/json",
    })
    with urlopen(req, timeout=30) as response:
        payload = json.load(response)
    return payload.get("data", [])


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
    return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def update_discovered_dates(shops, state):
    """
    State key: uuid. Value: {"campaign_end_date": str, "discovered": "YYYY-MM-DD"}.
    A campaign counts as 'new' when its end-date changes or the uuid first
    appears. When a campaign ends (has_campaign=0), we drop the entry so the
    next campaign will get a fresh discovered date.
    """
    today = date.today().isoformat()
    new_state = {}
    for shop in shops:
        if shop.get("has_campaign") != 1:
            continue
        uuid = shop["uuid"]
        end_date = shop.get("campaign_ends_date")
        prev = state.get(uuid)
        if prev and prev.get("campaign_end_date") == end_date:
            discovered = prev["discovered"]
        else:
            discovered = today
        new_state[uuid] = {
            "campaign_end_date": end_date,
            "discovered": discovered,
        }
    return new_state


def points_cell(shop):
    """Return (main_number, bonus_number)."""
    if shop.get("has_campaign") == 1:
        main = shop.get("points_campaign") or 0
        base = shop.get("points") or 0
        return main, max(main - base, 0)
    return shop.get("points") or 0, 0


def render_html(shops, discovered_map):
    def sort_key(s):
        is_campaign = s.get("has_campaign") == 1
        main, _ = points_cell(s)
        return (0 if is_campaign else 1, -main, s.get("name", "").lower())

    shops_sorted = sorted(shops, key=sort_key)

    rows = []
    for s in shops_sorted:
        uuid = s["uuid"]
        name = s["name"]
        main, bonus = points_cell(s)
        level = s.get("points_channel") or 0
        currency = s.get("currency") or ""
        unit = "%" if currency == "%" else ""
        is_campaign = s.get("has_campaign") == 1
        kvar = s.get("campaign_ends") or "" if is_campaign else ""
        discovered = discovered_map.get(uuid, {}).get("discovered", "") if is_campaign else ""
        shop_url = f"https://onlineshopping.flysas.com/sv-SE/butiker/about-you/{uuid}"

        row_class = "campaign" if is_campaign else ""
        bonus_html = f'<span class="extra">+{bonus}</span>' if bonus > 0 else ""

        rows.append(
            f'<tr class="{row_class}">'
            f'<td class="shop"><a href="{shop_url}" target="_blank" rel="noopener">{name}</a></td>'
            f'<td class="points">{main}{unit} {bonus_html}</td>'
            f'<td class="level">{level}</td>'
            f'<td>{kvar}</td>'
            f'<td>{discovered}</td>'
            f'</tr>'
        )

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(shops)
    active = sum(1 for s in shops if s.get("has_campaign") == 1)

    return f"""<!DOCTYPE html>
<html lang="sv">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SAS EuroBonus Shopping — kampanjer</title>
<style>
  body {{ font-family: system-ui, -apple-system, sans-serif;
         background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }}
  h1 {{ margin: 0 0 4px 0; }}
  .meta {{ color: #94a3b8; font-size: 14px; margin-bottom: 20px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 10px 12px; border-bottom: 1px solid #1e293b; text-align: left; }}
  th {{ background: #020617; position: sticky; top: 0; font-weight: 600; }}
  tr.campaign {{ background: #064e3b; }}
  a {{ color: #38bdf8; text-decoration: none; font-weight: 600; }}
  a:hover {{ text-decoration: underline; }}
  .points {{ color: #facc15; font-weight: 700; white-space: nowrap; }}
  .extra {{ color: #4ade80; margin-left: 6px; }}
  .level {{ color: #94a3b8; }}
  @media (max-width: 700px) {{
    body {{ padding: 10px; }}
    table {{ font-size: 14px; }}
    .level {{ display: none; }}
  }}
</style>
</head>
<body>
<h1>SAS EuroBonus Shopping — kampanjer</h1>
<div class="meta">Senast uppdaterad: {updated} · {active} aktiva kampanjer av {total} butiker totalt</div>
<table>
  <thead>
    <tr><th>Namn</th><th>Poäng</th><th class="level">Nivåpoäng</th><th>Kvar</th><th>Upptäckt</th></tr>
  </thead>
  <tbody>{''.join(rows)}</tbody>
</table>
</body>
</html>
"""


def main():
    print(f"Fetching {API_BASE}")
    try:
        shops = fetch_shops()
    except Exception as e:
        print(f"Fetch failed: {e}", file=sys.stderr)
        sys.exit(1)

    active = sum(1 for s in shops if s.get("has_campaign") == 1)
    print(f"Got {len(shops)} shops ({active} active campaigns)")

    if len(shops) < 50:
        print(f"WARNING: only {len(shops)} shops returned — pagination may have kicked in",
              file=sys.stderr)

    state = load_state()
    new_state = update_discovered_dates(shops, state)
    save_state(new_state)

    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    HTML_FILE.write_text(render_html(shops, new_state), encoding="utf-8")
    print(f"Wrote {HTML_FILE} and {STATE_FILE}")


if __name__ == "__main__":
    main()
