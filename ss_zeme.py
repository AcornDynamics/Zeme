#%%
# ss_lv_plots_and_lands_recursive.py

import re
import time
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://www.ss.lv"
ROOT = f"{BASE}/lv/real-estate/plots-and-lands/"

#%%
# ============================================================
# Robust retrying session (lazy); compatible with urllib3 v1/v2
# ============================================================
_SESSION = None

def _make_retry():
    try:
        # urllib3 v2
        return Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
    except TypeError:
        # urllib3 v1
        return Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=frozenset(["GET"]),
        )

def get_session():
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            ),
            "Accept-Language": "lv-LV,lv;q=0.9,en;q=0.8",
        })
        s.mount("https://", HTTPAdapter(max_retries=_make_retry()))
        _SESSION = s
    return _SESSION

# ============================================================
# Utilities
# ============================================================
def normalize_cat_url(url: str) -> str:
    """Ensure absolute URL, trailing slash, and keep only category node (no /sell/)."""
    if url.startswith("/"):
        url = BASE + url
    # strip any sell/ or pages
    url = re.sub(r"(?:/sell/.*)$", "/", url.rstrip("/")) + "/"
    return url

def get_soup(url: str) -> BeautifulSoup:
    r = get_session().get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ============================================================
# 1) Discover ALL category nodes (regions + subregions) recursively
# ============================================================
def discover_all_category_nodes(start=ROOT, delay=0.1, verbose=True):
    """
    BFS over category pages following <a class='a_category'> links that stay within
    /lv/real-estate/plots-and-lands/... hierarchy. Returns a set of category node URLs.
    """
    start = normalize_cat_url(start)
    visited = set()
    queue = [start]

    while queue:
        cur = queue.pop(0)
        if cur in visited:
            continue
        visited.add(cur)

        try:
            soup = get_soup(cur)
        except Exception as e:
            if verbose:
                print(f"[WARN] Failed to fetch {cur}: {e}")
            continue

        # Find child categories
        for a in soup.find_all("a", class_="a_category", href=True):
            href = a["href"]
            # only follow under the same top category
            if not href.startswith("/lv/real-estate/plots-and-lands/"):
                continue
            child = normalize_cat_url(href)
            # skip direct /sell/ links; we generate them ourselves
            if "/sell/" in child:
                continue
            if child not in visited and child not in queue:
                queue.append(child)

        if verbose and len(visited) % 10 == 0:
            print(f"[DISCOVERY] Visited {len(visited)} category pages...")
        time.sleep(delay)

    if verbose:
        print(f"[DISCOVERY] Total category nodes discovered: {len(visited)}")
    return visited

# ============================================================
# 2) Convert category nodes to /sell/ URLs (plus top aggregate)
# ============================================================
def build_sell_urls_from_nodes(nodes: set, include_aggregate=True):
    sell_urls = set()
    for node in nodes:
        sell_urls.add(node + "sell/")
    if include_aggregate:
        sell_urls.add(ROOT + "sell/")
    return sorted(sell_urls)

# ============================================================
# 3) Paginate each /sell/ and collect ad links
# ============================================================
def listing_rows(listing_url):
    resp = get_session().get(listing_url, timeout=25)
    if resp.status_code != 200:
        print(f"[WARN] {resp.status_code} for {listing_url}")
        return []
    soup = BeautifulSoup(resp.text, "lxml")
    return [tr for tr in soup.find_all("tr") if tr.get("id", "").startswith("tr_")]

def row_to_link(row, baseurl=BASE):
    tds = row.find_all("td")
    if len(tds) < 3:
        return None
    a = tds[1].find("a", href=True)
    if not a:
        return None
    return baseurl + a["href"]

def collect_links_for_sell(sell_url, delay=0.1, max_pages=5000, verbose=False):
    page = 1
    links, seen = [], set()
    while page <= max_pages:
        url = sell_url if page == 1 else f"{sell_url}page{page}.html"
        rows = listing_rows(url)
        if not rows:
            if verbose:
                print(f"  [INFO] {url}: no rows, stop.")
            break
        added = 0
        for r in rows:
            href = row_to_link(r)
            if href and href not in seen:
                seen.add(href)
                links.append(href)
                added += 1
        if verbose:
            print(f"  [INFO] {url}: +{added} (total {len(links)})")
        page += 1
        time.sleep(delay)
    return links

def collect_all_listing_links_from_sells(sell_urls, delay=0.1, verbose=True):
    all_links, seen = [], set()
    for i, su in enumerate(sell_urls, start=1):
        if verbose:
            print(f"[SELL {i}/{len(sell_urls)}] {su}")
        links = collect_links_for_sell(su, delay=delay, verbose=verbose)
        new_links = [l for l in links if l not in seen]
        seen.update(new_links)
        all_links.extend(new_links)
        if verbose:
            print(f"  [SUMMARY] this sell: {len(links)}, new: {len(new_links)}, total unique: {len(all_links)}")
    return all_links

# ============================================================
# 4) Parse ad pages
# ============================================================
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
    soup = get_soup(url)
    return {
        "Link": url,
        "Pilseta":       extract_text_by_id(soup, "tdo_20"),   # City
        "Iela":          extract_text_by_id(soup, "tdo_11"),   # Street
        "Ciems":         extract_text_by_id(soup, "tdo_368"),  # Village
        "Platiba":       extract_text_by_id(soup, "tdo_3"),    # Area
        "Cena":          extract_text_by_id(soup, "tdo_8"),    # Price
        "Zemes Tips":    extract_text_by_id(soup, "tdo_228"),  # Land usage/type
        "Zemes Numurs":  extract_text_by_id(soup, "tdo_1631"), # Cadastral/land number
        "Datums":        extract_datums(soup),                 # Date string
    }

# ============================================================
# 5) Main pipeline
# ============================================================
def data_collection_date():
    return datetime.today().strftime("%Y-%m-%d")

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

def scrape_ss_lv_plots_and_lands_recursive(delay_between_requests=0.1, verbose=True):
    # 1) Discover all category nodes (regions + subregions)
    nodes = discover_all_category_nodes(ROOT, delay=delay_between_requests, verbose=verbose)

    # 2) Build their /sell/ URLs (plus aggregate /sell/)
    sell_urls = build_sell_urls_from_nodes(nodes, include_aggregate=True)
    if verbose:
        print(f"[INFO] Total /sell/ URLs to crawl: {len(sell_urls)}")

    # 3) Collect all unique ad links across all /sell/ pages with pagination
    all_links = collect_all_listing_links_from_sells(sell_urls, delay=delay_between_requests, verbose=verbose)
    if verbose:
        print(f"[INFO] Total unique ad links collected: {len(all_links)}")

    # 4) Parse details per ad
    details = []
    for i, link in enumerate(all_links, start=1):
        try:
            details.append(parse_ad_details(link))
        except Exception as e:
            print(f"[WARN] {i}/{len(all_links)} failed: {link} -> {e}")
        if verbose and i % 50 == 0:
            print(f"[INFO] Parsed {i}/{len(all_links)}")
        time.sleep(delay_between_requests)

    df = pd.DataFrame(details)
    if df.empty:
        return df

    # 5) Add collection date and normalize numerics
    df["Datu iev."] = data_collection_date()
    df = normalize_numeric_columns(df)

    # 6) Deduplicate by Link (safety)
    df = df.drop_duplicates(subset=["Link"]).reset_index(drop=True)
    return df
#%%
# ============================================================
# Run
# ============================================================
if __name__ == "__main__":
    df_zeme = scrape_ss_lv_plots_and_lands_recursive(verbose=True)
    print(f"\nTotal adverts scraped (unique): {len(df_zeme)}\n")
    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(df_zeme.head(10).to_string(index=False))

    # Manual exports (uncomment what you need):
    # df_zeme.to_csv("ss_lv_zeme_recursive.csv", index=False)
    # df_zeme.to_excel("ss_lv_zeme_recursive.xlsx", index=False)
    # df_zeme.to_parquet("ss_lv_zeme_recursive.parquet", index=False)

# %%
