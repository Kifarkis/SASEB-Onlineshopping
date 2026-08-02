#!/usr/bin/env python3
"""SAS EuroBonus Shopping tracker — multi-country edition."""
import json
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

COUNTRIES = [
    {"code": "SE", "local_lang": "sv", "name": "Sverige", "languages": ["sv", "en"]},
    {"code": "DK", "local_lang": "da", "name": "Danmark", "languages": ["da", "en"]},
    {"code": "NO", "local_lang": "nb", "name": "Norge",   "languages": ["nb", "en"]},
    {"code": "FI", "local_lang": "en", "name": "Suomi",   "languages": ["en"]},
]

# Everyday adds Faroes (Danish per spec) and is the canonical full country list
# for the combined page country picker. Online data won't have FO entries.
EVERYDAY_COUNTRIES = COUNTRIES + [
    {"code": "FO", "local_lang": "da", "name": "Føroyar", "languages": ["da", "en"]},
]

# Everyday category names per language, keyed by category_id (1-12).
# Sourced from eurobonus.shopping's category dropdown in each language.
# IDs 7 and 10 are not represented in observed data and may be unused.
EVERYDAY_CATEGORIES = {
    1:  {"sv": "Hotell, Kryssning",      "da": "Hotel, Krydstogt",     "nb": "Hotell, Cruise",       "en": "Hotel, Cruise"},
    2:  {"sv": "Hälsa, Wellness",        "da": "Sundhed, Wellness",    "nb": "Helse, velvære",       "en": "Health, Wellness"},
    3:  {"sv": "Event",                  "da": "Event",                "nb": "Event",                "en": "Event"},
    4:  {"sv": "Sport och Fritid",       "da": "Sport, Fritid",        "nb": "Sport, Fritid",        "en": "Sports and Leisure"},
    5:  {"sv": "Heminredning",           "da": "Bolig",                "nb": "Bolig",                "en": "Home, Interiors"},
    6:  {"sv": "Bilar, Elbilsladdning",  "da": "Biler, Elbilopladning","nb": "Biler, Elbillading",   "en": "Auto, EV Charging"},
    8:  {"sv": "Kläder, mode",           "da": "Tøj, Mode",            "nb": "Klær, Mote",           "en": "Clothes, Fashion"},
    9:  {"sv": "Livsmedel",              "da": "Dagligvarer",          "nb": "Dagligvare",           "en": "Grocery"},
    11: {"sv": "Restaurant, Bar, Café",  "da": "Restaurant, Bar, Café","nb": "Restaurant, Bar, Kafé","en": "Restaurant, Bar, Café"},
    12: {"sv": "Annat",                  "da": "Anden",                "nb": "Annet",                "en": "Other"},
}

# Shop tab country list. The webshop has no Finnish storefront; instead it has
# an "Other (EUR)" catalogue served from the /en/ path. Gift cards are not
# offered there at all (deals are), which the scraper records as an empty
# collection rather than a failure.
SHOP_COUNTRIES = [
    {"code": "SE", "local_lang": "sv", "name": "Sverige", "languages": ["sv", "en"]},
    {"code": "DK", "local_lang": "da", "name": "Danmark", "languages": ["da", "en"]},
    {"code": "NO", "local_lang": "nb", "name": "Norge",   "languages": ["nb", "en"]},
    {"code": "EU", "local_lang": "en", "name": "Other (EUR)", "languages": ["en"]},
]

# Frontend country code -> directory written by scrape_shop.py.
SHOP_DIR_FOR_CODE = {"SE": "se", "DK": "dk", "NO": "no", "EU": "en"}

# Product URLs and images share long constant prefixes. Strip them here and
# rebuild in JS so the embedded payload doesn't repeat ~140 bytes per item.
SHOP_URL_PREFIX = "https://www.saseurobonusshop.com/"
SHOP_IMG_PREFIX = (
    "https://www.saseurobonusshop.com/media/catalog/product/cache/2/image/"
    "509x509/9df78eab33525d08d6e5fb8d27136e95/"
)

API_BASE = "https://onlineshopping.loyaltykey.com/api/v1"
SHOPS_URL = API_BASE + "/shops?filter[channel]=SAS&filter[language]={lang}&filter[country]={country}&filter[amount]=5000"
CATEGORIES_URL = API_BASE + "/shops/categories?filter[language]={lang}"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
EVERYDAY_DATA_DIR = REPO_ROOT / "data" / "everyday"
SHOP_DATA_DIR = REPO_ROOT / "data" / "shop"
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
        "tab_online": "Online", "tab_everyday": "I butik",
        "meta_template_everyday": "{shops} ställen · {onsite} i butik · {online_count} online · uppdaterad {ts}",
        "filter_onsite": "I butik", "filter_online": "Online",
        "search_placeholder_everyday": "Sök butik, stad eller adress…",
        "no_shops_everyday": "Inga ställen matchar.",
        "online_only": "Online", "points_per_100_unit": "p / 100 kr",
        "modal_visit": "Öppna webbplats", "modal_directions": "Vägbeskrivning",
        "modal_phone": "Telefon", "modal_cards": "Betalkort", "modal_address": "Adress",
        "points_disclaimer": "Intjänade poäng överförs till ditt EuroBonus-konto mellan 3 och 40 dagar efter köpet, beroende på typ av handlare. Vid retur dras de intjänade poängen av.",
        "filter_near_me": "Nära mig", "near_me_off": "Stäng av",
        "near_loading": "Hämtar din position…",
        "near_denied": "Position nekad. Aktivera platsdelning i webbläsaren för att använda Nära mig.",
        "near_unavailable": "Det gick inte att hämta din position just nu. Försök igen.",
        "near_unsupported": "Din webbläsare stöder inte platsdelning.",
        "sort_az_everyday": "A–Ö", "sort_za_everyday": "Ö–A",
        "sort_points_everyday": "Mest poäng", "sort_distance_everyday": "Närmast först",
        "tab_shop": "Shoppen",
        "meta_template_shop": "{items} produkter · {giftcards} presentkort · {deals} deals · uppdaterad {ts}",
        "filter_giftcards": "Presentkort", "filter_deals": "Månadens deals",
        "search_placeholder_shop": "Sök produkt eller varumärke…",
        "no_shops_shop": "Inga produkter matchar.",
        "no_gone_shop": "Inga borttagna produkter ännu.",
        "sort_points_asc": "Billigast i poäng", "sort_points_desc": "Dyrast i poäng",
        "shop_or": "eller", "shop_earn_up_to": "Tjäna upp till", "shop_earn_rate": "Tjäna",
        "shop_points_unit": "poäng", "shop_per_100": "p / 100 kr",
        "modal_open_shop": "Öppna i EuroBonus Shop",
        "modal_points_price": "Pris i poäng", "modal_cash_price": "Pris i pengar",
        "modal_max_earn": "Max intjäning", "modal_earn_rate": "Intjäning",
        "modal_first_seen": "Sedd sedan", "modal_lowest": "Lägsta pris hittills",
        "shop_disclaimer": "Priser och intjäning hämtas från SAS EuroBonus Shop. Presentkort säljs i flera valörer — intjäningen visas som ett maxvärde eftersom valörerna kräver inloggning.",
        "shop_login_note": "Kräver inloggning på SAS EuroBonus Shop för köp.",
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
        "tab_online": "Online", "tab_everyday": "In store",
        "meta_template_everyday": "{shops} places · {onsite} in store · {online_count} online · updated {ts}",
        "filter_onsite": "In store", "filter_online": "Online",
        "search_placeholder_everyday": "Search shop, city, or address…",
        "no_shops_everyday": "No places match.",
        "online_only": "Online", "points_per_100_unit": "p / 100",
        "modal_visit": "Open website", "modal_directions": "Directions",
        "modal_phone": "Phone", "modal_cards": "Cards", "modal_address": "Address",
        "points_disclaimer": "Earned points are transferred to your EuroBonus account between 3 and 40 days after the purchase, depending on the merchant type. Returned items reverse the earned points.",
        "filter_near_me": "Near me", "near_me_off": "Turn off",
        "near_loading": "Getting your location…",
        "near_denied": "Location denied. Enable location sharing in your browser to use Near me.",
        "near_unavailable": "Could not get your location right now. Try again.",
        "near_unsupported": "Your browser does not support location sharing.",
        "sort_az_everyday": "A–Z", "sort_za_everyday": "Z–A",
        "sort_points_everyday": "Most points", "sort_distance_everyday": "Nearest first",
        "tab_shop": "Shop",
        "meta_template_shop": "{items} products · {giftcards} gift cards · {deals} deals · updated {ts}",
        "filter_giftcards": "Gift cards", "filter_deals": "Deals of the month",
        "search_placeholder_shop": "Search product or brand…",
        "no_shops_shop": "No products match.",
        "no_gone_shop": "No removed products yet.",
        "sort_points_asc": "Cheapest in points", "sort_points_desc": "Most points",
        "shop_or": "or", "shop_earn_up_to": "Earn up to", "shop_earn_rate": "Earn",
        "shop_points_unit": "points", "shop_per_100": "p / 100",
        "modal_open_shop": "Open in EuroBonus Shop",
        "modal_points_price": "Price in points", "modal_cash_price": "Price in cash",
        "modal_max_earn": "Max earning", "modal_earn_rate": "Earning",
        "modal_first_seen": "Seen since", "modal_lowest": "Lowest price so far",
        "shop_disclaimer": "Prices and earning are taken from the SAS EuroBonus Shop. Gift cards come in several denominations — earning is shown as a maximum because the denominations require a login.",
        "shop_login_note": "Requires a SAS EuroBonus Shop login to purchase.",
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
        "tab_online": "Online", "tab_everyday": "I butik",
        "meta_template_everyday": "{shops} steder · {onsite} i butik · {online_count} online · opdateret {ts}",
        "filter_onsite": "I butik", "filter_online": "Online",
        "search_placeholder_everyday": "Søg butik, by eller adresse…",
        "no_shops_everyday": "Ingen steder matcher.",
        "online_only": "Online", "points_per_100_unit": "p / 100 kr",
        "modal_visit": "Åbn hjemmeside", "modal_directions": "Rutevejledning",
        "modal_phone": "Telefon", "modal_cards": "Betalingskort", "modal_address": "Adresse",
        "points_disclaimer": "Optjente point overføres til din EuroBonus-konto mellem 3 og 40 dage efter købet, afhængigt af butikstypen. Returneres varen, fratrækkes de optjente point.",
        "filter_near_me": "Nær mig", "near_me_off": "Slå fra",
        "near_loading": "Henter din position…",
        "near_denied": "Position nægtet. Aktivér placeringsdeling i din browser for at bruge Nær mig.",
        "near_unavailable": "Kunne ikke hente din position lige nu. Prøv igen.",
        "near_unsupported": "Din browser understøtter ikke placeringsdeling.",
        "sort_az_everyday": "A–Å", "sort_za_everyday": "Å–A",
        "sort_points_everyday": "Flest point", "sort_distance_everyday": "Nærmest først",
        "tab_shop": "Shoppen",
        "meta_template_shop": "{items} produkter · {giftcards} gavekort · {deals} deals · opdateret {ts}",
        "filter_giftcards": "Gavekort", "filter_deals": "Månedens deals",
        "search_placeholder_shop": "Søg produkt eller mærke…",
        "no_shops_shop": "Ingen produkter matcher.",
        "no_gone_shop": "Ingen fjernede produkter endnu.",
        "sort_points_asc": "Billigst i point", "sort_points_desc": "Dyrest i point",
        "shop_or": "eller", "shop_earn_up_to": "Optjen op til", "shop_earn_rate": "Optjen",
        "shop_points_unit": "point", "shop_per_100": "p / 100 kr",
        "modal_open_shop": "Åbn i EuroBonus Shop",
        "modal_points_price": "Pris i point", "modal_cash_price": "Pris i penge",
        "modal_max_earn": "Maks. optjening", "modal_earn_rate": "Optjening",
        "modal_first_seen": "Set siden", "modal_lowest": "Laveste pris hidtil",
        "shop_disclaimer": "Priser og optjening hentes fra SAS EuroBonus Shop. Gavekort sælges i flere valører — optjeningen vises som en maksimumværdi, fordi valørerne kræver login.",
        "shop_login_note": "Kræver login på SAS EuroBonus Shop for at købe.",
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
        "tab_online": "Online", "tab_everyday": "I butikk",
        "meta_template_everyday": "{shops} steder · {onsite} i butikk · {online_count} online · oppdatert {ts}",
        "filter_onsite": "I butikk", "filter_online": "Online",
        "search_placeholder_everyday": "Søk butikk, by eller adresse…",
        "no_shops_everyday": "Ingen steder matcher.",
        "online_only": "Online", "points_per_100_unit": "p / 100 kr",
        "modal_visit": "Åpne nettside", "modal_directions": "Veibeskrivelse",
        "modal_phone": "Telefon", "modal_cards": "Betalingskort", "modal_address": "Adresse",
        "points_disclaimer": "Opptjente poeng overføres til din EuroBonus-konto mellom 3 og 40 dager etter kjøpet, avhengig av butikktypen. Ved retur trekkes opptjente poeng fra.",
        "filter_near_me": "Nær meg", "near_me_off": "Slå av",
        "near_loading": "Henter posisjonen din…",
        "near_denied": "Posisjon nektet. Aktiver posisjonsdeling i nettleseren for å bruke Nær meg.",
        "near_unavailable": "Kunne ikke hente posisjonen din akkurat nå. Prøv igjen.",
        "near_unsupported": "Nettleseren din støtter ikke posisjonsdeling.",
        "sort_az_everyday": "A–Å", "sort_za_everyday": "Å–A",
        "sort_points_everyday": "Flest poeng", "sort_distance_everyday": "Nærmest først",
        "tab_shop": "Shoppen",
        "meta_template_shop": "{items} produkter · {giftcards} gavekort · {deals} deals · oppdatert {ts}",
        "filter_giftcards": "Gavekort", "filter_deals": "Månedens deals",
        "search_placeholder_shop": "Søk produkt eller merke…",
        "no_shops_shop": "Ingen produkter matcher.",
        "no_gone_shop": "Ingen fjernede produkter ennå.",
        "sort_points_asc": "Billigst i poeng", "sort_points_desc": "Dyrest i poeng",
        "shop_or": "eller", "shop_earn_up_to": "Tjen opp til", "shop_earn_rate": "Tjen",
        "shop_points_unit": "poeng", "shop_per_100": "p / 100 kr",
        "modal_open_shop": "Åpne i EuroBonus Shop",
        "modal_points_price": "Pris i poeng", "modal_cash_price": "Pris i penger",
        "modal_max_earn": "Maks. opptjening", "modal_earn_rate": "Opptjening",
        "modal_first_seen": "Sett siden", "modal_lowest": "Laveste pris hittil",
        "shop_disclaimer": "Priser og opptjening hentes fra SAS EuroBonus Shop. Gavekort selges i flere valører — opptjeningen vises som en maksverdi fordi valørene krever innlogging.",
        "shop_login_note": "Krever innlogging på SAS EuroBonus Shop for kjøp.",
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

# === Everyday data layer ===
# Reads per-country JSON produced by scrape_everyday.py and shapes it for the
# combined frontend. Defensive markdown-link unwrap remains here — the source
# data has been known to ship [text](url) for some entries.

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+|www\.[^)\s]+)\)")


def unwrap_md_url(value):
    if not value:
        return ""
    text = value.strip()
    m = _MARKDOWN_LINK_RE.search(text)
    return m.group(2).strip() if m else text


def normalize_url(value):
    v = unwrap_md_url(value)
    if not v:
        return ""
    if v.startswith(("http://", "https://")):
        return v
    return "https://" + v


def maps_url_for(shop):
    """Google Maps directions URL with destination only.
    Maps fills in the user's current location as origin automatically.
    """
    if shop.get("lat") is not None and shop.get("lng") is not None:
        return f"https://www.google.com/maps/dir/?api=1&destination={shop['lat']},{shop['lng']}"
    parts = [shop.get("name"), shop.get("address"), shop.get("city"), shop.get("postcode")]
    parts = [p for p in parts if p and p != "."]
    if not parts:
        return ""
    return "https://www.google.com/maps/dir/?api=1&destination=" + quote(", ".join(parts))


def _clean_str(value):
    s = (value or "").strip()
    return "" if s == "." else s


def load_everyday_country(code):
    path = EVERYDAY_DATA_DIR / code.lower() / "shops.json"
    if not path.exists():
        return {"shops": [], "updated": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"shops": [], "updated": None}


def prepare_everyday_dataset(country_code):
    raw = load_everyday_country(country_code)
    out = []
    for s in raw.get("shops", []):
        if s.get("status") != "active":
            continue
        out.append({
            "uuid": s.get("uuid"),
            "name": s.get("name"),
            "city": _clean_str(s.get("city")),
            "address": _clean_str(s.get("address")),
            "postcode": _clean_str(s.get("postcode")),
            "lat": s.get("lat"),
            "lng": s.get("lng"),
            "mode": s.get("mode") or "onsite",
            "category_id": s.get("category_id"),
            "points": s.get("points_per_100") or 0,
            "currency": s.get("currency") or "",
            "website": normalize_url(s.get("website")),
            "phone": (s.get("phone") or "").strip(),
            "description": s.get("description") or "",
            "cards_accepted": s.get("cards_accepted") or [],
            "has_campaign": bool(s.get("has_campaign")),
            "campaign_title": s.get("campaign_title"),
            "campaign_description": s.get("campaign_description"),
            "maps_url": maps_url_for(s),
        })
    out.sort(key=lambda x: (x["name"] or "").lower())
    onsite = sum(1 for x in out if x["mode"] == "onsite")
    online = sum(1 for x in out if x["mode"] == "online")
    return {
        "shops": out,
        "onsite_count": onsite,
        "online_count": online,
        "updated": raw.get("updated"),
    }


# === Shop data layer ===
# Reads per-region JSON produced by scrape_shop.py. Two collections per region
# (giftcards, deals) are merged into a single list carrying a `coll` field, so
# the frontend can filter with chips rather than juggling two datasets.


def load_shop_collection(country_code, collection):
    directory = SHOP_DIR_FOR_CODE.get(country_code, country_code.lower())
    path = SHOP_DATA_DIR / directory / f"{collection}.json"
    if not path.exists():
        return {"items": [], "updated": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"items": [], "updated": None}


def _strip_prefix(value, prefix):
    v = (value or "").strip()
    if v.startswith(prefix):
        return v[len(prefix):]
    return v


def prepare_shop_dataset(country_code):
    items = []
    updated = None
    currency = ""
    counts = {"giftcards": 0, "deals": 0}

    for collection in ("giftcards", "deals"):
        raw = load_shop_collection(country_code, collection)
        if raw.get("updated"):
            # Both collections are written in the same run; either timestamp
            # is representative, so keep the later one.
            if updated is None or raw["updated"] > updated:
                updated = raw["updated"]
        currency = raw.get("currency") or currency

        for it in raw.get("items", []):
            status = it.get("status") or "active"
            if status == "active":
                counts[collection] += 1
            items.append({
                "uuid": it.get("uuid"),
                "name": it.get("name") or "",
                "brand": it.get("brand") or "",
                "coll": collection,
                "path": _strip_prefix(it.get("url"), SHOP_URL_PREFIX),
                "img": _strip_prefix(it.get("image"), SHOP_IMG_PREFIX),
                "points": it.get("points_price"),
                "cash": it.get("cash_price"),
                "unit": it.get("cash_unit") or "",
                "max_earn": it.get("max_earn_points"),
                "rate": it.get("points_per_100"),
                "low": it.get("lowest_points_price"),
                "first_seen": it.get("first_seen"),
                "status": status,
                "gone_since": it.get("gone_since"),
            })

    items.sort(key=lambda x: (x["name"] or "").lower())
    return {
        "shops": items,
        "giftcard_count": counts["giftcards"],
        "deal_count": counts["deals"],
        "currency": currency,
        "updated": updated,
    }


def render_html(online_datasets, everyday_datasets, shop_datasets):
    """Generate the full single-page HTML with both datasets embedded.

    Pre-renders content for the default country/language directly into the
    HTML so that elements aren't empty on first paint. Without this, the
    A-Z jumper, meta line, and tab labels start as empty divs that JS
    fills after page load — causing layout shift (CLS). JS still rebuilds
    these on country/language change.
    """
    default_country = "SE"
    default_lang = "sv"
    sv = STRINGS[default_lang]
    if default_country not in online_datasets:
        default_country = next(iter(online_datasets))
    ds = online_datasets[default_country]

    # Pre-render: A-Z jumper letters from active shop names
    active = [s for s in ds["shops"] if s.get("status") == "active"]
    letters = sorted({(s.get("name") or "#")[0].upper() for s in active})
    jumper_letters = "".join(
        f'<span class="sas-jumper-letter" data-letter="{l}">{l}</span>'
        for l in letters
    )

    # Pre-render: meta line (campaigns count + new this week + total + updated)
    campaigns = [s for s in active if s.get("has_campaign")]
    try:
        from datetime import datetime as _dt
        upd_date = _dt.strptime(ds["updated"].split(" ")[0], "%Y-%m-%d")
    except Exception:
        upd_date = None
    new_this_week = 0
    if upd_date:
        for s in campaigns:
            cs = s.get("campaign_started")
            if not cs:
                continue
            try:
                started = _dt.strptime(cs, "%Y-%m-%d")
                days = (upd_date - started).days
                if 0 <= days <= 7:
                    new_this_week += 1
            except Exception:
                pass

    meta_text = (sv["meta_template"]
        .replace("{campaigns}", str(len(campaigns)))
        .replace("{new}", str(new_this_week))
        .replace("{shops}", str(len(active)))
        .replace("{ts}", ds["updated"]))

    payload = {
        "datasets": json.dumps(online_datasets, ensure_ascii=False),
        "everyday_datasets": json.dumps(everyday_datasets, ensure_ascii=False),
        "shop_datasets": json.dumps(shop_datasets, ensure_ascii=False),
        "strings": json.dumps(STRINGS, ensure_ascii=False),
        "countries": json.dumps(COUNTRIES, ensure_ascii=False),
        "everyday_countries": json.dumps(EVERYDAY_COUNTRIES, ensure_ascii=False),
        "shop_countries": json.dumps(SHOP_COUNTRIES, ensure_ascii=False),
        "everyday_categories": json.dumps(EVERYDAY_CATEGORIES, ensure_ascii=False),
        "shop_url_prefix": SHOP_URL_PREFIX,
        "shop_img_prefix": SHOP_IMG_PREFIX,
        "default_country": default_country,
        "default_lang": default_lang,
        "default_jumper": jumper_letters,
        "default_meta_text": meta_text,
        "default_tab_online": sv["tab_online"],
        "default_tab_everyday": sv["tab_everyday"],
        "default_tab_shop": sv["tab_shop"],
        "default_search_placeholder": sv["search_placeholder"],
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
  --text: #1a1a1a; --text-muted: #595959; --text-faint: #707070;
  --accent: #1858c7; --accent-bg: rgba(24, 88, 199, 0.08);
  --warn: #b85c00;
  --shadow-sticky: 0 4px 12px rgba(0, 0, 0, 0.06);
}}
html[data-theme="dark"] {{
  --bg: #0f0f10; --surface: #1a1a1c;
  --border: rgba(255, 255, 255, 0.08); --border-strong: rgba(255, 255, 255, 0.18);
  --text: #ededed; --text-muted: #b3b3b3; --text-faint: #8a8a8a;
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
.sas-meta {{ font-size: 14px; color: var(--text-muted); font-family: ui-monospace, "SF Mono", Menlo, monospace; min-height: 20px; max-height: 40px; opacity: 1; overflow: hidden; transition: max-height 0.2s ease, opacity 0.2s ease, margin 0.2s ease; }}
.sas-sticky-wrap.is-stuck .sas-meta {{ max-height: 0; opacity: 0; margin: 0; }}
.sas-title {{ font-size: 28px; font-weight: 500; letter-spacing: -0.02em; margin: 0 0 4px 0; transition: font-size 0.2s ease; }}
.sas-sticky-wrap.is-stuck .sas-title {{ font-size: 20px; }}
.sas-header-controls {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
.sas-header-select {{ font-family: inherit; font-size: 13px; padding: 7px 12px; border: 0.5px solid var(--border-strong); border-radius: 999px; background: var(--surface); color: var(--text); cursor: pointer; min-width: 96px; }}
.sas-toggle {{ background: none; border: 0.5px solid var(--border-strong); color: var(--text-muted); padding: 7px 14px; border-radius: 999px; font-size: 13px; cursor: pointer; font-family: inherit; }}
.sas-toggle:hover {{ color: var(--text); }}

.sas-filter-row {{ display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; min-height: 36px; }}
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

.sas-jumper {{ display: flex; gap: 2px; padding: 6px 0 14px 0; min-height: 44px; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-faint); flex-wrap: wrap; }}
.sas-jumper-letter {{ padding: 4px 8px; cursor: pointer; border-radius: 4px; user-select: none; }}
.sas-jumper-letter:hover {{ color: var(--text); }}
.sas-jumper-letter.active {{ color: var(--text); background: var(--surface); }}

.sas-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; min-height: 150vh; align-content: start; }}
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

/* === Tabs + everyday-only styles === */
.sas-meta-row {{ display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }}
.sas-tab-pill {{ display: inline-flex; gap: 0; border: 0.5px solid var(--border-strong); border-radius: 999px; padding: 3px; background: var(--surface); flex-shrink: 0; min-width: 140px; }}
.sas-tab-btn {{ font-family: inherit; font-size: 13px; padding: 5px 14px; border: 0; border-radius: 999px; background: transparent; color: var(--text-muted); cursor: pointer; flex: 1; }}
.sas-tab-btn:hover {{ color: var(--text); }}
.sas-tab-btn.active {{ background: var(--accent); color: #fff; }}

/* Everyday card layout mirrors online card structure: identity row (mode icon + name) on top */
.sas-card-everyday {{ min-height: 200px; }}
.sas-eyebrow {{ font-size: 11px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-faint); display: flex; align-items: center; gap: 8px; min-height: 14px; padding-right: 32px; }}
.sas-eyebrow-tag {{ background: var(--accent-bg); color: var(--accent); padding: 2px 8px; border-radius: 999px; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 10px; letter-spacing: 0.04em; }}
.sas-card-everyday .sas-card-name {{ white-space: normal; padding-right: 0; font-size: 17px; }}
.sas-mode-icon {{ width: 48px; height: 48px; border-radius: 10px; background: var(--accent-bg); color: var(--accent); display: flex; align-items: center; justify-content: center; flex-shrink: 0; }}
.sas-mode-icon svg {{ width: 22px; height: 22px; }}
.sas-distance {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--accent); margin-left: auto; }}
.sas-cards-row {{ display: flex; gap: 6px; flex-wrap: wrap; padding-top: 4px; }}
.sas-card-pill-net {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 10px; letter-spacing: 0.04em; color: var(--text-faint); padding: 2px 7px; border: 0.5px solid var(--border); border-radius: 4px; }}
.sas-near-error {{ font-size: 13px; color: var(--warn); padding: 10px 14px; margin-bottom: 12px; border: 0.5px solid var(--border); border-radius: 8px; background: var(--surface); }}
.sas-card-address {{ font-size: 13px; color: var(--text-muted); line-height: 1.4; }}
.sas-points-row-everyday {{ display: flex; align-items: baseline; gap: 6px; padding-top: 8px; }}
.sas-points-main-everyday {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 22px; font-weight: 500; letter-spacing: -0.02em; line-height: 1; }}
.sas-card-foot-address {{ display: block; line-height: 1.4; font-size: 13px; }}

/* Everyday modal extras */
.sas-modal-eyebrow {{ font-size: 11px; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px; }}
.sas-modal-disclaimer {{ font-size: 12px; line-height: 1.5; color: var(--text-faint); padding: 12px 14px; margin-top: 18px; border: 0.5px solid var(--border); border-radius: 8px; background: var(--bg); }}
.sas-modal-stat-row a {{ color: var(--accent); text-decoration: none; }}
.sas-modal-stat-row a:hover {{ text-decoration: underline; }}
.sas-modal-secondary {{ flex: 1; background: transparent; color: var(--text); padding: 13px; border: 0.5px solid var(--border-strong); border-radius: 10px; font-size: 14px; font-weight: 500; cursor: pointer; text-decoration: none; text-align: center; font-family: inherit; }}
.sas-modal-secondary:hover {{ background: var(--accent-bg); color: var(--accent); border-color: var(--accent); }}

/* === Shop tab === */
.sas-tab-pill {{ min-width: 210px; }}
.sas-card-shop {{ min-height: 260px; gap: 10px; }}
.sas-shop-thumb {{ width: 100%; aspect-ratio: 1 / 1; border-radius: 10px; background: #fff; display: flex; align-items: center; justify-content: center; overflow: hidden; flex-shrink: 0; }}
html[data-theme="dark"] .sas-shop-thumb {{ background: #f2f2f2; }}
.sas-shop-thumb img {{ width: 100%; height: 100%; object-fit: contain; padding: 8px; }}
.sas-card-shop .sas-card-name {{ white-space: normal; padding-right: 0; font-size: 15px; line-height: 1.3; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
.sas-shop-brand {{ font-size: 11px; font-weight: 500; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-faint); min-height: 14px; }}
.sas-shop-foot {{ margin-top: auto; display: flex; flex-direction: column; gap: 4px; }}
.sas-shop-price-row {{ display: flex; align-items: baseline; gap: 6px; flex-wrap: wrap; }}
.sas-shop-points {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 20px; font-weight: 500; letter-spacing: -0.02em; line-height: 1; }}
.sas-shop-cash {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 12px; color: var(--text-faint); }}
.sas-shop-earn {{ display: inline-block; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 11px; padding: 2px 8px; background: var(--accent-bg); color: var(--accent); border-radius: 999px; }}
.sas-shop-drop {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 11px; color: var(--warn); }}
.sas-modal-thumb {{ width: 100%; max-height: 220px; display: flex; align-items: center; justify-content: center; background: #fff; border-radius: 12px; margin-bottom: 16px; overflow: hidden; }}
html[data-theme="dark"] .sas-modal-thumb {{ background: #f2f2f2; }}
.sas-modal-thumb img {{ max-width: 100%; max-height: 220px; object-fit: contain; padding: 12px; }}

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
  .sas-meta {{ min-height: 40px; }}
  .sas-jumper {{ min-height: 80px; }}
  .sas-filter-row {{ min-height: 84px; }}
}}
</style>
</head>
<body>
<div class="sas-sticky-wrap" id="sticky-wrap">
  <div class="sas-container">
    <div class="sas-header">
      <div>
        <h1 class="sas-title" id="title-text">EuroBonus Shopping</h1>
        <div class="sas-meta-row">
          <div class="sas-tab-pill" role="tablist">
            <button class="sas-tab-btn active" id="tab-online" data-tab="online" role="tab">{default_tab_online}</button>
            <button class="sas-tab-btn" id="tab-everyday" data-tab="everyday" role="tab">{default_tab_everyday}</button>
            <button class="sas-tab-btn" id="tab-shop" data-tab="shop" role="tab">{default_tab_shop}</button>
          </div>
          <div class="sas-meta" id="meta-text">{default_meta_text}</div>
        </div>
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
    <input class="sas-search" id="search-box" type="search" placeholder="{default_search_placeholder}">
  </div>
</div>

<main class="sas-container">
  <div class="sas-jumper" id="jumper">{default_jumper}</div>
  <div class="sas-near-error sas-hidden" id="near-error"></div>
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
      <a class="sas-modal-secondary" id="modal-directions-btn" target="_blank" rel="noopener" style="display:none"></a>
      <button class="sas-modal-close" id="modal-close" aria-label="Close"></button>
    </div>
  </div>
</div>

<script id="sas-data" type="application/json">{datasets}</script>
<script id="sas-everyday-data" type="application/json">{everyday_datasets}</script>
<script id="sas-strings" type="application/json">{strings}</script>
<script id="sas-countries" type="application/json">{countries}</script>
<script id="sas-everyday-countries" type="application/json">{everyday_countries}</script>
<script id="sas-shop-data" type="application/json">{shop_datasets}</script>
<script id="sas-shop-countries" type="application/json">{shop_countries}</script>
<script id="sas-everyday-categories" type="application/json">{everyday_categories}</script>

<script>
(function() {{
  var DATA = JSON.parse(document.getElementById('sas-data').textContent);
  var EVERYDAY_DATA = JSON.parse(document.getElementById('sas-everyday-data').textContent);
  var STRINGS = JSON.parse(document.getElementById('sas-strings').textContent);
  var COUNTRIES = JSON.parse(document.getElementById('sas-countries').textContent);
  var EVERYDAY_COUNTRIES = JSON.parse(document.getElementById('sas-everyday-countries').textContent);
  var SHOP_DATA = JSON.parse(document.getElementById('sas-shop-data').textContent);
  var SHOP_COUNTRIES = JSON.parse(document.getElementById('sas-shop-countries').textContent);
  var SHOP_URL_PREFIX = '{shop_url_prefix}';
  var SHOP_IMG_PREFIX = '{shop_img_prefix}';
  var EVERYDAY_CATEGORIES = JSON.parse(document.getElementById('sas-everyday-categories').textContent);
  var DEFAULT_COUNTRY = '{default_country}';
  var DEFAULT_LANG = '{default_lang}';

  var params = new URLSearchParams(window.location.search);
  var tabParam = params.get('t');
  var tab = (tabParam === 'everyday' || tabParam === 'shop') ? tabParam : 'online';
  var country = (params.get('c') || DEFAULT_COUNTRY).toUpperCase();
  var lang = params.get('l') || DEFAULT_LANG;

  function activeCountries() {{
    if (tab === 'everyday') return EVERYDAY_COUNTRIES;
    if (tab === 'shop') return SHOP_COUNTRIES;
    return COUNTRIES;
  }}
  function activeData() {{
    if (tab === 'everyday') return EVERYDAY_DATA;
    if (tab === 'shop') return SHOP_DATA;
    return DATA;
  }}

  var countryDef = activeCountries().find(function(c) {{ return c.code === country; }}) || activeCountries()[0];
  country = countryDef.code;
  if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;

  var state = {{ view: 'all', category: 'all', query: '', sort: 'az', mode: 'all', sortEveryday: 'az', nearMe: false, userPos: null, shopColl: 'all', sortShop: 'az' }};
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
    modalShopBtn.style.display = '';
    var directionsBtn = document.getElementById('modal-directions-btn');
    if (directionsBtn) directionsBtn.style.display = 'none';
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
    var eyebrowHTML = shop.category_name ? '<div class="sas-eyebrow">' + shop.category_name + '</div>' : '';

    div.innerHTML =
      '<button class="sas-card-external" title="' + t('modal_open_external') + '" data-external-uuid="' + shop.uuid + '">' +
      '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3H3v10h10v-3"/><path d="M10 3h3v3"/><path d="M8 8l5-5"/></svg>' +
      '</button>' + newDot +
      eyebrowHTML +
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

  function getDataset() {{
    var src = activeData();
    return src[country] || Object.values(src)[0] || {{ shops: [], updated: '' }};
  }}

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

  // === Everyday rendering ===
  function escapeHtml(s) {{
    return (s == null ? '' : String(s)).replace(/[&<>"']/g, function(c) {{
      return {{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}}[c];
    }});
  }}

  function iconExt() {{
    return '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M6 3H4a1 1 0 00-1 1v8a1 1 0 001 1h8a1 1 0 001-1v-2"/><path d="M9 3h4v4M13 3L7 9"/></svg>';
  }}
  function iconMap() {{
    return '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M8 14s5-4 5-8a5 5 0 10-10 0c0 4 5 8 5 8z"/><circle cx="8" cy="6" r="2"/></svg>';
  }}
  function iconStorefront() {{
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l1-5h16l1 5"/><path d="M3 9v11h18V9"/><path d="M3 9c0 1.5 1 2.5 2.5 2.5S8 10.5 8 9c0 1.5 1 2.5 2.5 2.5S13 10.5 13 9c0 1.5 1 2.5 2.5 2.5S18 10.5 18 9c0 1.5 1 2.5 2.5 2.5S23 10.5 23 9"/><path d="M9 20v-5h6v5"/></svg>';
  }}
  function iconGlobe() {{
    return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18"/><path d="M12 3a14 14 0 010 18M12 3a14 14 0 000 18"/></svg>';
  }}

  // Haversine — returns distance in km between two lat/lng pairs
  function distanceKm(lat1, lng1, lat2, lng2) {{
    var R = 6371;
    var toRad = function(d) {{ return d * Math.PI / 180; }};
    var dLat = toRad(lat2 - lat1);
    var dLng = toRad(lng2 - lng1);
    var a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLng / 2) * Math.sin(dLng / 2);
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }}

  function fmtDistance(km) {{
    if (km == null) return '';
    if (km < 1) return Math.round(km * 1000) + ' m';
    if (km < 10) return km.toFixed(1) + ' km';
    return Math.round(km) + ' km';
  }}

  function everydayCategoryName(shop) {{
    if (!shop.category_id) return '';
    var entry = EVERYDAY_CATEGORIES[shop.category_id];
    if (!entry) return '';
    return entry[lang] || entry.en || '';
  }}

  function everydayEyebrowText(shop) {{
    if (shop.mode === 'online') return t('online_only');
    return shop.city || '';
  }}

  function everydayAddressLine(shop) {{
    if (shop.mode === 'online') return '';
    var bits = [];
    if (shop.address) bits.push(shop.address);
    if (shop.postcode || shop.city) {{
      var pc = [shop.postcode, shop.city].filter(Boolean).join(' ');
      if (pc) bits.push(pc);
    }}
    return bits.join(' · ');
  }}

  function fmtTs(iso) {{
    if (!iso) return '';
    try {{
      var d = new Date(iso);
      return d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0') + ' ' +
        String(d.getUTCHours()).padStart(2, '0') + ':' +
        String(d.getUTCMinutes()).padStart(2, '0') + ' UTC';
    }} catch (e) {{ return iso; }}
  }}

  function everydayCardHTML(shop) {{
    var div = document.createElement('div');
    div.className = 'sas-card sas-card-everyday' + (shop.has_campaign ? ' campaign' : '');
    div.dataset.uuid = shop.uuid;
    div.dataset.mode = shop.mode;
    div.dataset.letter = (shop.name || '#').charAt(0).toUpperCase();

    var addr = everydayAddressLine(shop);
    var unit = escapeHtml(t('points_per_100_unit'));

    var distance = (shop._distanceKm != null) ? '<span class="sas-distance">' + escapeHtml(fmtDistance(shop._distanceKm)) + '</span>' : '';
    var catName = everydayCategoryName(shop);
    var eyebrowInner;
    if (catName) {{
      eyebrowInner = escapeHtml(catName);
    }} else if (shop.mode === 'online') {{
      eyebrowInner = '<span class="sas-eyebrow-tag">' + escapeHtml(t('online_only')) + '</span>';
    }} else {{
      eyebrowInner = escapeHtml(shop.city || '');
    }}
    var eyebrow = '<div class="sas-eyebrow">' + eyebrowInner + distance + '</div>';

    var modeIcon = '<div class="sas-mode-icon" aria-hidden="true">' +
      (shop.mode === 'online' ? iconGlobe() : iconStorefront()) + '</div>';

    var cardsRow = '';
    if (shop.cards_accepted && shop.cards_accepted.length) {{
      cardsRow = '<div class="sas-cards-row">' +
        shop.cards_accepted.map(function(c) {{ return '<span class="sas-card-pill-net">' + escapeHtml(c) + '</span>'; }}).join('') +
        '</div>';
    }}

    // External link icon top-right, mirrors online cards. Direct shortcut to
    // the website; uses data-stop so the global click handler doesn't also
    // fire the modal.
    var externalBtn = shop.website
      ? '<a class="sas-card-external" href="' + escapeHtml(shop.website) + '" target="_blank" rel="noopener" title="' + escapeHtml(t('modal_visit')) + '" data-stop>' +
          '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3H3v10h10v-3"/><path d="M10 3h3v3"/><path d="M8 8l5-5"/></svg>' +
        '</a>'
      : '';

    // Footer holds address (parallels online cards' campaign-info footer slot).
    // Empty for online-only shops with no usable address.
    var footHTML = addr ? '<div class="sas-card-foot"><span class="sas-card-foot-address">' + escapeHtml(addr) + '</span></div>' : '<div class="sas-card-foot empty"></div>';

    div.innerHTML =
      externalBtn +
      eyebrow +
      '<div class="sas-card-top"><div class="sas-card-identity">' + modeIcon +
      '<div class="sas-card-name">' + escapeHtml(shop.name) + '</div></div></div>' +
      '<div class="sas-points-row-everyday"><span class="sas-points-main-everyday">' + shop.points + '</span><span class="sas-points-unit">' + unit + '</span></div>' +
      cardsRow +
      footHTML;
    return div;
  }}

  function openEverydayModal(shop) {{
    var rows = [];
    rows.push('<div class="sas-modal-stat-row"><span>EB</span><strong>' + shop.points + ' ' + escapeHtml(t('points_per_100_unit')) + '</strong></div>');
    if (shop.cards_accepted && shop.cards_accepted.length) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_cards')) + '</span><strong>' + shop.cards_accepted.map(escapeHtml).join(' · ') + '</strong></div>');
    }}
    if (shop.phone) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_phone')) + '</span><strong><a href="tel:' + encodeURIComponent(shop.phone) + '">' + escapeHtml(shop.phone) + '</a></strong></div>');
    }}
    var addr = everydayAddressLine(shop);
    if (addr) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_address')) + '</span><strong>' + escapeHtml(addr) + '</strong></div>');
    }}

    var html = '<div class="sas-modal-eyebrow">' + escapeHtml(everydayEyebrowText(shop)) + '</div>' +
      '<h2 class="sas-modal-title">' + escapeHtml(shop.name) + '</h2>' +
      '<div class="sas-modal-stats">' + rows.join('') + '</div>';

    if (shop.has_campaign && (shop.campaign_title || shop.campaign_description)) {{
      html += '<div class="sas-modal-eyebrow" style="margin-top:14px">' + escapeHtml(t('modal_campaign_period')) + '</div>';
      if (shop.campaign_title) html += '<div class="sas-modal-description"><strong>' + escapeHtml(shop.campaign_title) + '</strong></div>';
      if (shop.campaign_description) html += '<div class="sas-modal-description">' + shop.campaign_description + '</div>';
    }}

    if (shop.description) {{
      html += '<div class="sas-modal-description" style="margin-top: 16px;">' + shop.description + '</div>';
    }}

    html += '<div class="sas-modal-disclaimer">' + escapeHtml(t('points_disclaimer')) + '</div>';

    modalBody.innerHTML = html;

    var primary = document.getElementById('modal-shop-btn');
    var secondary = document.getElementById('modal-directions-btn');
    if (shop.website) {{
      primary.href = shop.website;
      primary.textContent = t('modal_visit');
      primary.style.display = '';
    }} else {{
      primary.style.display = 'none';
    }}
    if (shop.maps_url && shop.mode !== 'online') {{
      secondary.href = shop.maps_url;
      secondary.textContent = t('modal_directions');
      secondary.style.display = '';
    }} else {{
      secondary.style.display = 'none';
    }}
    document.getElementById('modal-close').textContent = t('modal_close');
    backdrop.classList.add('open');
    document.body.style.overflow = 'hidden';
  }}

  function renderEverydayJumper(shops) {{
    var jumper = document.getElementById('jumper');
    jumper.innerHTML = '';
    if (!shops.length) return;
    if (state.sortEveryday !== 'az' && state.sortEveryday !== 'za') return;

    var letters = {{}};
    shops.forEach(function(s) {{
      var ltr = (s.name || '#').charAt(0).toUpperCase();
      if (/[A-ZÅÄÖÆØ0-9]/.test(ltr)) letters[ltr] = true;
    }});
    var sorted = Object.keys(letters).sort();
    if (state.sortEveryday === 'za') sorted.reverse();

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

  function showNearError(key) {{
    var box = document.getElementById('near-error');
    box.textContent = t(key);
    box.classList.remove('sas-hidden');
  }}
  function hideNearError() {{
    document.getElementById('near-error').classList.add('sas-hidden');
  }}

  function requestUserPosition() {{
    return new Promise(function(resolve, reject) {{
      if (!('geolocation' in navigator)) {{ reject({{ kind: 'unsupported' }}); return; }}
      navigator.geolocation.getCurrentPosition(
        function(pos) {{ resolve({{ lat: pos.coords.latitude, lng: pos.coords.longitude }}); }},
        function(err) {{
          if (err && err.code === 1) reject({{ kind: 'denied' }});
          else reject({{ kind: 'unavailable' }});
        }},
        {{ enableHighAccuracy: false, timeout: 10000, maximumAge: 60000 }}
      );
    }});
  }}

  function activateNearMe() {{
    hideNearError();
    showNearError('near_loading');
    requestUserPosition().then(function(pos) {{
      state.userPos = pos;
      state.nearMe = true;
      state.sortEveryday = 'distance';
      hideNearError();
      renderEveryday();
    }}).catch(function(err) {{
      state.nearMe = false;
      state.userPos = null;
      var key = err && err.kind === 'unsupported' ? 'near_unsupported'
              : err && err.kind === 'denied' ? 'near_denied'
              : 'near_unavailable';
      showNearError(key);
      renderEveryday();
    }});
  }}

  function deactivateNearMe() {{
    state.nearMe = false;
    state.userPos = null;
    if (state.sortEveryday === 'distance') state.sortEveryday = 'az';
    hideNearError();
    renderEveryday();
  }}

  function renderEverydayGrid() {{
    var ds = getDataset();
    var grid = document.getElementById('shop-grid');
    var emptyState = document.getElementById('empty-state');
    grid.innerHTML = '';

    document.querySelectorAll('#view-filters .sas-chip').forEach(function(c) {{
      if (c.dataset.view === 'near-me') c.classList.toggle('active', state.nearMe);
      else c.classList.toggle('active', !state.nearMe && c.dataset.view === state.mode);
    }});

    // Build filtered list. Near-me forces onsite-only with valid coords.
    var filtered = ds.shops.filter(function(s) {{
      if (state.nearMe) {{
        if (s.mode !== 'onsite') return false;
        if (s.lat == null || s.lng == null) return false;
      }} else if (state.mode !== 'all' && s.mode !== state.mode) {{
        return false;
      }}
      if (state.query) {{
        var hay = ((s.name || '') + ' ' + (s.city || '') + ' ' + (s.address || '') + ' ' + (s.postcode || '')).toLowerCase();
        if (hay.indexOf(state.query) === -1) return false;
      }}
      return true;
    }});

    // Decorate with distance when in near-me mode
    if (state.nearMe && state.userPos) {{
      filtered.forEach(function(s) {{
        s._distanceKm = distanceKm(state.userPos.lat, state.userPos.lng, s.lat, s.lng);
      }});
    }} else {{
      filtered.forEach(function(s) {{ s._distanceKm = null; }});
    }}

    // Sort
    var sortKey = state.nearMe ? 'distance' : state.sortEveryday;
    if (sortKey === 'distance') {{
      filtered.sort(function(a, b) {{ return (a._distanceKm || 0) - (b._distanceKm || 0); }});
    }} else if (sortKey === 'za') {{
      filtered.sort(function(a, b) {{ return (b.name || '').localeCompare(a.name || '', lang); }});
    }} else if (sortKey === 'points') {{
      filtered.sort(function(a, b) {{ return (b.points || 0) - (a.points || 0); }});
    }} else {{
      filtered.sort(function(a, b) {{ return (a.name || '').localeCompare(b.name || '', lang); }});
    }}

    if (!filtered.length) {{
      emptyState.classList.remove('sas-hidden');
      emptyState.textContent = t('no_shops_everyday');
    }} else {{
      emptyState.classList.add('sas-hidden');
    }}

    var frag = document.createDocumentFragment();
    filtered.forEach(function(s) {{ frag.appendChild(everydayCardHTML(s)); }});
    grid.appendChild(frag);

    renderEverydayJumper(filtered);
  }}

  function renderEveryday() {{
    var ds = getDataset();
    document.documentElement.lang = lang;
    document.getElementById('title-text').textContent = t('title');
    setToggleLabel();

    shopsByUuid = {{}};
    ds.shops.forEach(function(s) {{ shopsByUuid[s.uuid] = s; }});

    document.getElementById('meta-text').textContent = t('meta_template_everyday')
      .replace('{{shops}}', ds.shops.length)
      .replace('{{onsite}}', ds.onsite_count || 0)
      .replace('{{online_count}}', ds.online_count || 0)
      .replace('{{ts}}', fmtTs(ds.updated));
    document.getElementById('search-box').placeholder = t('search_placeholder_everyday');
    document.getElementById('footer-unaffiliated').textContent = t('footer_unaffiliated');
    document.getElementById('footer-about').textContent = t('footer_about');
    document.getElementById('footer-privacy').textContent = t('footer_privacy');

    // Hide categories — everyday has no category names yet
    document.getElementById('category-select').style.display = 'none';

    // Show sort with everyday-specific options
    var sortSel = document.getElementById('sort-select');
    sortSel.style.display = '';
    sortSel.innerHTML = '';
    [
      ['az', 'sort_az_everyday'],
      ['za', 'sort_za_everyday'],
      ['points', 'sort_points_everyday'],
    ].forEach(function(pair) {{
      var o = document.createElement('option');
      o.value = pair[0]; o.textContent = t(pair[1]);
      sortSel.appendChild(o);
    }});
    if (state.nearMe) {{
      var o = document.createElement('option');
      o.value = 'distance'; o.textContent = t('sort_distance_everyday');
      sortSel.appendChild(o);
      sortSel.value = 'distance';
      sortSel.disabled = true;
    }} else {{
      sortSel.value = state.sortEveryday;
      sortSel.disabled = false;
    }}

    var viewFilters = document.getElementById('view-filters');
    viewFilters.innerHTML = '';
    var chips = [
      ['all', t('filter_all')],
      ['onsite', t('filter_onsite') + ' (' + (ds.onsite_count || 0) + ')'],
      ['online', t('filter_online') + ' (' + (ds.online_count || 0) + ')'],
    ];
    chips.forEach(function(pair) {{
      var b = document.createElement('button');
      b.className = 'sas-chip' + (!state.nearMe && state.mode === pair[0] ? ' active' : '');
      b.dataset.view = pair[0];
      b.textContent = pair[1];
      b.addEventListener('click', function() {{
        if (state.nearMe) {{ deactivateNearMe(); state.mode = pair[0]; return; }}
        state.mode = pair[0]; renderEverydayGrid();
      }});
      viewFilters.appendChild(b);
    }});
    // Near-me chip
    var near = document.createElement('button');
    near.className = 'sas-chip' + (state.nearMe ? ' active' : '');
    near.dataset.view = 'near-me';
    near.textContent = state.nearMe ? (t('filter_near_me') + ' · ' + t('near_me_off')) : t('filter_near_me');
    near.addEventListener('click', function() {{
      if (state.nearMe) deactivateNearMe();
      else activateNearMe();
    }});
    viewFilters.appendChild(near);

    renderEverydayGrid();
  }}

  // === Shop rendering ===
  function shopNum(n) {{
    if (n == null) return '';
    return String(n).replace(/\B(?=(\d{{3}})+(?!\d))/g, '\u00a0');
  }}

  // Names beginning with a digit ("2-pack Handdukar") all bucket under '#'
  // rather than each digit getting its own jumper entry before A.
  function shopLetter(name) {{
    var c = (name || '#').charAt(0).toUpperCase();
    return /[0-9]/.test(c) ? '#' : c;
  }}

  function shopEarnLabel(item) {{
    // An exact rate is only derivable for single-SKU items. Gift cards sold in
    // several denominations expose a maximum instead, which is not a rate.
    if (item.rate != null) return t('shop_earn_rate') + ' ' + item.rate + ' ' + t('shop_per_100');
    if (item.max_earn != null) return t('shop_earn_up_to') + ' ' + shopNum(item.max_earn) + ' ' + t('shop_points_unit');
    return '';
  }}

  // Images above the fold are the LCP candidate. Lazy-loading them defers the
  // request until after layout runs, which measurably delays LCP on mobile.
  // Load the first row eagerly with high priority, lazy for everything after.
  var SHOP_EAGER_IMAGES = 4;

  function shopCardHTML(item, idx) {{
    var div = document.createElement('div');
    var isGone = item.status === 'gone';
    div.className = 'sas-card sas-card-shop' + (isGone ? ' sas-card-gone' : '');
    div.dataset.uuid = item.uuid;
    div.dataset.letter = shopLetter(item.name);

    var eager = idx < SHOP_EAGER_IMAGES;
    var imgAttrs = eager
      ? 'loading="eager" fetchpriority="high" decoding="async"'
      : 'loading="lazy" decoding="async"';
    var thumb = item.img
      ? '<div class="sas-shop-thumb"><img src="' + escapeHtml(SHOP_IMG_PREFIX + item.img) + '" alt="" ' + imgAttrs + '></div>'
      : '<div class="sas-shop-thumb"></div>';

    var earn = shopEarnLabel(item);
    var earnHTML = earn ? '<span class="sas-shop-earn">' + escapeHtml(earn) + '</span>' : '';

    // Only surface a drop when the current price is above the recorded low.
    var dropHTML = '';
    if (!isGone && item.low != null && item.points != null && item.points > item.low) {{
      dropHTML = '<span class="sas-shop-drop">' + escapeHtml(t('modal_lowest')) + ': ' + shopNum(item.low) + '</span>';
    }}

    var priceHTML = isGone
      ? '<div class="sas-shop-price-row"><span class="sas-shop-cash">' + escapeHtml(t('gone_since')) + ' ' + escapeHtml(item.gone_since || '') + '</span></div>'
      : '<div class="sas-shop-price-row"><span class="sas-shop-points">' + shopNum(item.points) + '</span>' +
        '<span class="sas-shop-cash">' + escapeHtml(t('shop_points_unit')) + '</span></div>' +
        '<div class="sas-shop-price-row"><span class="sas-shop-cash">' + escapeHtml(t('shop_or')) + ' ' +
        shopNum(item.cash) + ' ' + escapeHtml(item.unit || '') + '</span></div>';

    div.innerHTML =
      thumb +
      '<div class="sas-shop-brand">' + escapeHtml(item.brand || '') + '</div>' +
      '<div class="sas-card-name">' + escapeHtml(item.name) + '</div>' +
      '<div class="sas-shop-foot">' + priceHTML +
      (earnHTML || dropHTML ? '<div class="sas-cards-row">' + earnHTML + dropHTML + '</div>' : '') +
      '</div>';
    return div;
  }}

  function openShopModal(item) {{
    var rows = [];
    if (item.points != null) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_points_price')) + '</span><strong>' + shopNum(item.points) + ' ' + escapeHtml(t('shop_points_unit')) + '</strong></div>');
    }}
    if (item.cash != null) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_cash_price')) + '</span><strong>' + shopNum(item.cash) + ' ' + escapeHtml(item.unit || '') + '</strong></div>');
    }}
    if (item.rate != null) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_earn_rate')) + '</span><strong>' + item.rate + ' ' + escapeHtml(t('shop_per_100')) + '</strong></div>');
    }} else if (item.max_earn != null) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_max_earn')) + '</span><strong>' + shopNum(item.max_earn) + ' ' + escapeHtml(t('shop_points_unit')) + '</strong></div>');
    }}
    if (item.low != null && item.points != null && item.points > item.low) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_lowest')) + '</span><strong>' + shopNum(item.low) + ' ' + escapeHtml(t('shop_points_unit')) + '</strong></div>');
    }}
    if (item.first_seen) {{
      rows.push('<div class="sas-modal-stat-row"><span>' + escapeHtml(t('modal_first_seen')) + '</span><strong>' + escapeHtml(item.first_seen) + '</strong></div>');
    }}

    var thumb = item.img
      ? '<div class="sas-modal-thumb"><img src="' + escapeHtml(SHOP_IMG_PREFIX + item.img) + '" alt=""></div>'
      : '';

    modalBody.innerHTML =
      thumb +
      '<div class="sas-modal-eyebrow">' + escapeHtml(item.brand || '') + '</div>' +
      '<h2 class="sas-modal-title">' + escapeHtml(item.name) + '</h2>' +
      '<div class="sas-modal-stats">' + rows.join('') + '</div>' +
      '<div class="sas-modal-disclaimer">' + escapeHtml(t('shop_disclaimer')) + '<br>' + escapeHtml(t('shop_login_note')) + '</div>';

    var primary = document.getElementById('modal-shop-btn');
    var secondary = document.getElementById('modal-directions-btn');
    if (item.path) {{
      primary.href = SHOP_URL_PREFIX + item.path;
      primary.textContent = t('modal_open_shop');
      primary.style.display = '';
    }} else {{
      primary.style.display = 'none';
    }}
    secondary.style.display = 'none';
    document.getElementById('modal-close').textContent = t('modal_close');
    backdrop.classList.add('open');
    document.body.style.overflow = 'hidden';
  }}

  function renderShopGrid() {{
    var ds = getDataset();
    var grid = document.getElementById('shop-grid');
    var emptyState = document.getElementById('empty-state');
    grid.innerHTML = '';

    document.querySelectorAll('#view-filters .sas-chip').forEach(function(c) {{
      c.classList.toggle('active', c.dataset.view === state.shopColl);
    }});
    document.getElementById('sort-select').disabled = state.shopColl === 'gone';

    var filtered;
    if (state.shopColl === 'gone') {{
      filtered = ds.shops.filter(function(s) {{ return s.status === 'gone'; }});
    }} else {{
      filtered = ds.shops.filter(function(s) {{
        if (s.status !== 'active') return false;
        if (state.shopColl !== 'all' && s.coll !== state.shopColl) return false;
        return true;
      }});
    }}

    if (state.query) {{
      filtered = filtered.filter(function(s) {{
        return ((s.name || '') + ' ' + (s.brand || '')).toLowerCase().indexOf(state.query) !== -1;
      }});
    }}

    if (state.shopColl === 'gone') {{
      filtered.sort(function(a, b) {{ return (b.gone_since || '').localeCompare(a.gone_since || ''); }});
    }} else if (state.sortShop === 'za') {{
      filtered.sort(function(a, b) {{ return (b.name || '').localeCompare(a.name || '', lang); }});
    }} else if (state.sortShop === 'points_asc') {{
      filtered.sort(function(a, b) {{ return (a.points || 0) - (b.points || 0); }});
    }} else if (state.sortShop === 'points_desc') {{
      filtered.sort(function(a, b) {{ return (b.points || 0) - (a.points || 0); }});
    }} else if (state.sortShop === 'recent') {{
      filtered.sort(function(a, b) {{ return (b.first_seen || '').localeCompare(a.first_seen || ''); }});
    }} else {{
      filtered.sort(function(a, b) {{ return (a.name || '').localeCompare(b.name || '', lang); }});
    }}

    if (!filtered.length) {{
      emptyState.classList.remove('sas-hidden');
      emptyState.textContent = state.shopColl === 'gone' ? t('no_gone_shop') : t('no_shops_shop');
    }} else {{
      emptyState.classList.add('sas-hidden');
    }}

    var frag = document.createDocumentFragment();
    filtered.forEach(function(s, i) {{ frag.appendChild(shopCardHTML(s, i)); }});
    grid.appendChild(frag);

    renderShopJumper(filtered);
  }}

  function renderShopJumper(items) {{
    var jumper = document.getElementById('jumper');
    jumper.innerHTML = '';
    if (!items.length) return;
    if (state.shopColl === 'gone') return;
    if (state.sortShop !== 'az' && state.sortShop !== 'za') return;

    var letters = {{}};
    items.forEach(function(s) {{
      var ltr = shopLetter(s.name);
      if (/[A-ZÅÄÖÆØ#]/.test(ltr)) letters[ltr] = true;
    }});
    var sorted = Object.keys(letters).sort();
    if (state.sortShop === 'za') sorted.reverse();

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

  function renderShop() {{
    var ds = getDataset();
    document.documentElement.lang = lang;
    document.getElementById('title-text').textContent = t('title');
    setToggleLabel();

    shopsByUuid = {{}};
    ds.shops.forEach(function(s) {{ shopsByUuid[s.uuid] = s; }});

    var activeCount = ds.shops.filter(function(s) {{ return s.status === 'active'; }}).length;
    var goneCount = ds.shops.length - activeCount;

    document.getElementById('meta-text').textContent = t('meta_template_shop')
      .replace('{{items}}', activeCount)
      .replace('{{giftcards}}', ds.giftcard_count || 0)
      .replace('{{deals}}', ds.deal_count || 0)
      .replace('{{ts}}', fmtTs(ds.updated));
    document.getElementById('search-box').placeholder = t('search_placeholder_shop');
    document.getElementById('footer-unaffiliated').textContent = t('footer_unaffiliated');
    document.getElementById('footer-about').textContent = t('footer_about');
    document.getElementById('footer-privacy').textContent = t('footer_privacy');

    document.getElementById('category-select').style.display = 'none';

    var sortSel = document.getElementById('sort-select');
    sortSel.style.display = '';
    sortSel.innerHTML = '';
    [
      ['az', 'sort_az'], ['za', 'sort_za'],
      ['points_asc', 'sort_points_asc'], ['points_desc', 'sort_points_desc'],
      ['recent', 'sort_recent'],
    ].forEach(function(pair) {{
      var o = document.createElement('option');
      o.value = pair[0]; o.textContent = t(pair[1]);
      sortSel.appendChild(o);
    }});
    sortSel.value = state.sortShop;

    var viewFilters = document.getElementById('view-filters');
    viewFilters.innerHTML = '';
    var chips = [
      ['all', t('filter_all')],
      ['giftcards', t('filter_giftcards') + ' (' + (ds.giftcard_count || 0) + ')'],
      ['deals', t('filter_deals') + ' (' + (ds.deal_count || 0) + ')'],
    ];
    if (goneCount) chips.push(['gone', t('filter_gone') + ' (' + goneCount + ')']);
    chips.forEach(function(pair) {{
      var b = document.createElement('button');
      b.className = 'sas-chip' + (state.shopColl === pair[0] ? ' active' : '');
      b.dataset.view = pair[0];
      b.textContent = pair[1];
      b.addEventListener('click', function() {{ state.shopColl = pair[0]; renderShopGrid(); }});
      viewFilters.appendChild(b);
    }});

    renderShopGrid();
  }}

  function render() {{
    // Update tab pill state every render
    document.getElementById('tab-online').classList.toggle('active', tab === 'online');
    document.getElementById('tab-everyday').classList.toggle('active', tab === 'everyday');
    document.getElementById('tab-shop').classList.toggle('active', tab === 'shop');
    document.getElementById('tab-online').textContent = t('tab_online');
    document.getElementById('tab-everyday').textContent = t('tab_everyday');
    document.getElementById('tab-shop').textContent = t('tab_shop');

    if (tab === 'everyday') {{ renderEveryday(); return; }}
    if (tab === 'shop') {{ renderShop(); return; }}

    // Restore online-only controls if user came back from everyday
    document.getElementById('sort-select').style.display = '';
    document.getElementById('category-select').style.display = '';

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
    if (e.target.closest('[data-stop]')) return; // let card action buttons (visit/maps) pass through
    var ext = e.target.closest('[data-external-uuid]');
    if (ext) {{
      e.stopPropagation();
      var sh = shopsByUuid[ext.dataset.externalUuid];
      if (sh) window.open(shopUrl(sh.uuid), '_blank', 'noopener');
      return;
    }}
    var card = e.target.closest('.sas-card[data-uuid]');
    if (card) {{
      var sh = shopsByUuid[card.dataset.uuid];
      if (!sh) return;
      if (tab === 'shop') {{ openShopModal(sh); return; }}
      if (card.classList.contains('sas-card-gone')) return;
      if (tab === 'everyday') openEverydayModal(sh);
      else openModal(sh);
    }}
  }});

  var countrySel = document.getElementById('country-select');
  function rebuildCountrySelector() {{
    countrySel.innerHTML = '';
    activeCountries().forEach(function(c) {{
      var o = document.createElement('option');
      o.value = c.code; o.textContent = c.name;
      countrySel.appendChild(o);
    }});
    // If current country isn't valid for this tab, fall back to first available
    var match = activeCountries().find(function(c) {{ return c.code === country; }});
    if (!match) {{
      countryDef = activeCountries()[0];
      country = countryDef.code;
      if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;
    }}
    countrySel.value = country;
  }}
  rebuildCountrySelector();

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
    if (tab === 'everyday' || tab === 'shop') url.searchParams.set('t', tab);
    else url.searchParams.delete('t');
    window.history.replaceState({{}}, '', url);
  }}

  function switchTab(newTab) {{
    if (newTab === tab) return;
    tab = newTab;
    // Reset filter/search state on tab change so chips don't carry over
    state.view = 'all';
    state.mode = 'all';
    state.category = 'all';
    state.query = '';
    state.sort = 'az';
    state.sortEveryday = 'az';
    state.shopColl = 'all';
    state.sortShop = 'az';
    state.nearMe = false;
    state.userPos = null;
    hideNearError();
    document.getElementById('search-box').value = '';
    rebuildCountrySelector();
    countryDef = activeCountries().find(function(c) {{ return c.code === country; }}) || activeCountries()[0];
    if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;
    rebuildLangSelector();
    updateUrl();
    render();
  }}
  document.getElementById('tab-online').addEventListener('click', function() {{ switchTab('online'); }});
  document.getElementById('tab-everyday').addEventListener('click', function() {{ switchTab('everyday'); }});
  document.getElementById('tab-shop').addEventListener('click', function() {{ switchTab('shop'); }});

  countrySel.addEventListener('change', function() {{
    country = countrySel.value;
    countryDef = activeCountries().find(function(c) {{ return c.code === country; }}) || activeCountries()[0];
    if (countryDef.languages.indexOf(lang) === -1) lang = countryDef.local_lang;
    rebuildLangSelector();
    state.category = 'all';
    state.query = '';
    // Country change invalidates near-me — different region
    state.nearMe = false;
    state.userPos = null;
    hideNearError();
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
    if (tab === 'everyday') renderEverydayGrid();
    else if (tab === 'shop') renderShopGrid();
    else renderGrid();
  }});
  document.getElementById('sort-select').addEventListener('change', function(e) {{
    if (tab === 'everyday') {{
      state.sortEveryday = e.target.value;
      renderEverydayGrid();
    }} else if (tab === 'shop') {{
      state.sortShop = e.target.value;
      renderShopGrid();
    }} else {{
      state.sort = e.target.value;
      renderGrid();
    }}
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

    # Load everyday datasets (already scraped + saved by scrape_everyday.py).
    # We just read from disk; no API calls here.
    everyday_datasets = {}
    for country in EVERYDAY_COUNTRIES:
        code = country["code"]
        everyday_datasets[code] = prepare_everyday_dataset(code)
    everyday_total = sum(len(d["shops"]) for d in everyday_datasets.values())
    print(f"\nLoaded everyday data: {everyday_total} shops across {len(everyday_datasets)} countries")

    # Load shop datasets (scraped + saved weekly by scrape_shop.py).
    shop_datasets = {}
    for country in SHOP_COUNTRIES:
        code = country["code"]
        shop_datasets[code] = prepare_shop_dataset(code)
    shop_total = sum(
        sum(1 for i in d["shops"] if i.get("status") == "active")
        for d in shop_datasets.values()
    )
    print(f"Loaded shop data: {shop_total} active items across {len(shop_datasets)} regions")

    if "SE" not in datasets:
        print(
            "  Default country SE missing after fetch failures; "
            "leaving the existing site in place and exiting non-zero.",
            file=sys.stderr,
        )
        sys.exit(1)

    HTML_FILE.parent.mkdir(parents=True, exist_ok=True)
    HTML_FILE.write_text(render_html(datasets, everyday_datasets, shop_datasets), encoding="utf-8")
    print(f"\nWrote {HTML_FILE} with {len(datasets)} online + {len(everyday_datasets)} everyday + {len(shop_datasets)} shop country datasets")

    if not all_succeeded:
        sys.exit(1)

if __name__ == "__main__":
    main()
