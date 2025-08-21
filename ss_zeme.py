#%%
# ss_lv_plots_two_phase.py
# Phase 1: Discover regions, subregions, pages
# Phase 2: Scrape listing pages -> ad links -> ad details (incl. Ciems)

import re
import time
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#%%

BASE  = "https://www.ss.lv"
ROOT  = f"{BASE}/lv/real-estate/plots-and-lands/"

# Tunables
LISTING_DELAY = 0.10   # delay between listing-page requests (discovery & scraping)
AD_DELAY      = 0.10   # delay between ad-page requests
VERBOSE       = True   # print progress

# -------------------------------
# Robust session (lazy init)
# -------------------------------
_SESSION = None
def _make_retry():
    try:  # urllib3 v2
        return Retry(total=3, backoff_factor=0.5,
                     status_forcelist=(429, 500, 502, 503, 504),
                     allowed_methods=frozenset(["GET"]))
    except TypeError:  # urllib3 v1
        return Retry(total=3, backoff_factor=0.5,
                     status_forcelist=(429, 500, 502, 503, 504),
                     method_whitelist=frozenset(["GET"]))

def sess():
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/123.0 Safari/537.36"),
            "Accept-Language": "lv-LV,lv;q=0.9,en;q=0.8",
        })
        s.mount("https://", HTTPAdapter(max_retries=_make_retry()))
        _SESSION = s
    return _SESSION

#%%

# -------------------------------
# Helpers
# -------------------------------
def norm_cat(url: str) -> str:
    """Normalize category URL (absolute + trailing slash; strip /sell/ & pages)."""
    if url.startswith("/"):
        url = BASE + url
    url = url.rstrip("/")
    url = re.sub(r"/sell(?:/.*)?$", "", url)  # remove any /sell/... tail
    return url + "/"

def get_soup(url: str) -> BeautifulSoup:
    r = sess().get(url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml"), r.url

def listing_rows_from_html(html: str):
    soup = BeautifulSoup(html, "lxml")
    return [tr for tr in soup.find_all("tr") if tr.get("id", "").startswith("tr_")]

def listing_rows(url: str):
    r = sess().get(url, timeout=25, allow_redirects=True)
    if r.status_code != 200:
        return [], r.url
    rows = [tr for tr in BeautifulSoup(r.text, "lxml").find_all("tr")
            if tr.get("id", "").startswith("tr_")]
    return rows, r.url

def row_to_link(row):
    tds = row.find_all("td")
    if len(tds) < 3:
        return None
    a = tds[1].find("a", href=True)
    if not a:
        return None
    return BASE + a["href"]

#%%

# ============================================================
# PHASE 1: DISCOVERY
# ============================================================

def discover_regions(root=ROOT):
    """Return a sorted list of region URLs directly under ROOT."""
    soup, _ = get_soup(norm_cat(root))
    regions = []
    seen = set()
    for a in soup.find_all("a", class_="a_category", href=True):
        href = a["href"]
        if not href.startswith("/lv/real-estate/plots-and-lands/"):
            continue
        # expect immediate children like /.../riga/
        url = norm_cat(href)
        # filter out root itself and duplicates
        if url != norm_cat(root) and url not in seen:
            # keep only immediate children (one more path segment than ROOT)
            # ROOT path segments count:
            base_parts = norm_cat(root).strip("/").split("/")
            url_parts  = url.strip("/").split("/")
            if len(url_parts) == len(base_parts) + 1:
                regions.append(url)
                seen.add(url)
    if VERBOSE:
        print(f"[DISCOVERY] Regions found: {len(regions)}")
    return sorted(regions)

def discover_subregions(region_url: str):
    """Return a sorted list of subregion URLs directly under a region (one level)."""
    soup, _ = get_soup(norm_cat(region_url))
    subs = []
    seen = set()
    for a in soup.find_all("a", class_="a_category", href=True):
        href = a["href"]
        if not href.startswith("/lv/real-estate/plots-and-lands/"):
            continue
        url = norm_cat(href)
        # only accept one level deeper than region
        base_parts = norm_cat(region_url).strip("/").split("/")
        url_parts  = url.strip("/").split("/")
        if len(url_parts) == len(base_parts) + 1:
            if url not in seen:
                subs.append(url)
                seen.add(url)
    return sorted(subs)

def discover_pagination_for_sell(sell_url: str, max_pages=300):
    """
    Return a list of actual listing page URLs for a given /sell/:
      [/sell/, /sell/page2.html, ...]
    Stop when:
      - page has no listing rows, or
      - request gets redirected away from requested page (nonexistent page).
    """
    pages = []
    page = 1
    while page <= max_pages:
        url = sell_url if page == 1 else f"{sell_url}page{page}.html"
        rows, final_url = listing_rows(url)

        if page > 1 and final_url.rstrip("/") != url.rstrip("/"):
            # redirected (e.g., to base /sell/) -> stop
            break
        if not rows:
            break

        pages.append(url)
        page += 1
        time.sleep(LISTING_DELAY)
    return pages

def phase1_discover_inventory(root=ROOT, include_region_if_no_subs=True):
    """
    Returns:
      regions:                [region_url, ...]
      subregions_by_region:   {region_url: [subregion_url, ...], ...}
      listing_pages:          [(owner_url, page_url), ...] where owner_url is the region or subregion
    """
    regions = discover_regions(root)
    subregions_by_region = {}
    listing_pages = []

    for reg in regions:
        subs = discover_subregions(reg)
        subregions_by_region[reg] = subs

        targets = subs if subs else ([reg] if include_region_if_no_subs else [])
        for target in targets:
            sell_url = norm_cat(target) + "sell/"
            pages = discover_pagination_for_sell(sell_url)
            for p in pages:
                listing_pages.append((target, p))
            if VERBOSE:
                name = target.replace(ROOT, "")
                print(f"[DISCOVERY] {name}: pages={len(pages)}")

    if VERBOSE:
        total_pages = len(listing_pages)
        total_subs  = sum(len(v) for v in subregions_by_region.values())
        print(f"[SUMMARY] Regions={len(regions)}, Subregions={total_subs}, Listing pages={total_pages}")

    return regions, subregions_by_region, listing_pages

# ============================================================
# PHASE 2: SCRAPING
# ============================================================

def collect_ad_links_from_pages(listing_pages):
    """
    listing_pages: list of (owner_url, page_url)
    Returns: list of unique ad links
    """
    seen = set()
    all_links = []
    for idx, (_, page_url) in enumerate(listing_pages, start=1):
        r = sess().get(page_url, timeout=25)
        if r.status_code != 200:
            if VERBOSE:
                print(f"[WARN] {r.status_code} on {page_url}")
            continue
        rows = listing_rows_from_html(r.text)
        added = 0
        for row in rows:
            href = row_to_link(row)
            if href and href not in seen:
                seen.add(href)
                all_links.append(href)
                added += 1
        if VERBOSE:
            print(f"[LISTING {idx}/{len(listing_pages)}] +{added} links (unique total {len(all_links)})")
        time.sleep(LISTING_DELAY)
    return all_links

def extract_text_by_id(soup, el_id, default="NA"):
    el = soup.find(id=el_id)
    return el.get_text(strip=True) if el else default

def extract_datums(soup):
    nodes = soup.find_all(string=re.compile(r"\bDatums:"))
    if not nodes:
        return "NA"
    node = nodes[0]
    parent_text = node.parent.get_text(" ", strip=True) if getattr(node, "parent", None) else str(node)
    m = re.search(r"(\d{2}\.\d{2}\.\d{4}\.|\d{4}-\d{2}-\d{2})", parent_text)
    return m.group(1) if m else parent_text

def parse_ad_details(url):
    r = sess().get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    return {
        "Link":          url,
        "Pilseta":       extract_text_by_id(soup, "tdo_20"),
        "Iela":          extract_text_by_id(soup, "tdo_11"),
        "Ciems":         extract_text_by_id(soup, "tdo_368"),  # NEW: Village
        "Platiba":       extract_text_by_id(soup, "tdo_3"),
        "Cena":          extract_text_by_id(soup, "tdo_8"),
        "Zemes Tips":    extract_text_by_id(soup, "tdo_228"),
        "Zemes Numurs":  extract_text_by_id(soup, "tdo_1631"),
        "Datums":        extract_datums(soup),
    }

def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["Cena"] = df["Cena"].fillna("NA").astype(str)
    df["Cena EUR"] = (
        df["Cena"].str.extract(r"([\d\s]+)\s*€", expand=False)
        .str.replace(" ", "", regex=False)
    )
    df["Cena m2"] = (
        df["Cena"].str.extract(r"\(([\d\.,\s]+)\s*€/m²\)", expand=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["Platiba"] = df["Platiba"].fillna("NA").astype(str)
    plat_split = df["Platiba"].str.extract(r"([\d\.,]+)\s*(.*)")
    df["Platiba Daudzums"] = (
        plat_split[0]
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["Platiba Mervieniba"] = plat_split[1].fillna("")

    for col in ["Cena EUR", "Cena m2", "Platiba Daudzums"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.drop(columns=["Platiba", "Cena"], errors="ignore")

def phase2_scrape_inventory(listing_pages):
    # 1) Collect all unique ad links
    ad_links = collect_ad_links_from_pages(listing_pages)
    if VERBOSE:
        print(f"[SCRAPE] Unique ad links: {len(ad_links)}")

    # 2) Parse ad details
    details = []
    for i, link in enumerate(ad_links, start=1):
        try:
            details.append(parse_ad_details(link))
        except Exception as e:
            print(f"[WARN] {i}/{len(ad_links)} failed: {link} -> {e}")
        if VERBOSE and i % 50 == 0:
            print(f"[SCRAPE] Parsed {i}/{len(ad_links)} ads")
        time.sleep(AD_DELAY)

    df = pd.DataFrame(details)
    if df.empty:
        return df

    df["Datu iev."] = datetime.today().strftime("%Y-%m-%d")
    df = normalize_numeric_columns(df)
    df = df.drop_duplicates(subset=["Link"]).reset_index(drop=True)
    return df


#%%
# ============================================================
# MAIN
# ============================================================
def run_two_phase(root=ROOT):
    # PHASE 1: discover
    regions, subregions_by_region, listing_pages = phase1_discover_inventory(root)

    # (Optional) you can inspect the discovered inventory here
    # print(regions)
    # print(subregions_by_region)
    # print([p for _, p in listing_pages][:20])

    # PHASE 2: scrape
    df = phase2_scrape_inventory(listing_pages)
    return df

if __name__ == "__main__":
    df_zeme = run_two_phase(ROOT)
    print(f"\nTotal adverts scraped (unique): {len(df_zeme)}\n")
    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(df_zeme.head(10).to_string(index=False))

    # Manual exports (uncomment if you want)
    # df_zeme.to_csv("ss_lv_zeme_two_phase.csv", index=False)
    # df_zeme.to_excel("ss_lv_zeme_two_phase.xlsx", index=False)
    # df_zeme.to_parquet("ss_lv_zeme_two_phase.parquet", index=False)

# %%
