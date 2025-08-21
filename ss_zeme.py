import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ----------------------------
# HTTP session with retries
# ----------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0 Safari/537.36"
        )
    })
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

SESSION = make_session()

# ----------------------------
# Listing-page helpers
# ----------------------------
def getRows(listing_url):
    """Return SS.lv listing <tr> rows for a category page."""
    resp = SESSION.get(listing_url, timeout=20)
    if resp.status_code != 200:
        print(f"[WARN] Bad request {resp.status_code} for {listing_url}")
        return []
    soup = BeautifulSoup(resp.text, 'lxml')
    rows = []
    for el in soup.find_all('tr'):
        if 'id' in el.attrs and el.attrs['id'].startswith('tr_'):
            rows.append(el)
    # SS.lv often has a trailing non-ad row; prior code removed last row
    return rows[:-1] if rows else []

def row_to_link(row, baseurl='https://www.ss.com'):
    """Extract ad link + title from a listing row."""
    tds = row.find_all('td')
    link = baseurl + tds[1].a['href']
    title = tds[2].get_text(strip=True).replace('\r', '').replace('\n', '')
    return link, title

def collect_all_listing_links(base_sell_url, delay=0.12, max_pages=5000):
    """
    Iterate pages: /sell/, /sell/page2.html, /sell/page3.html, ...
    Stop when a page has no ad rows or max_pages reached.
    """
    page = 1
    links = []
    seen = set()

    while page <= max_pages:
        url = base_sell_url if page == 1 else f"{base_sell_url}page{page}.html"
        rows = getRows(url)
        if not rows:
            break

        for r in rows:
            href, _title = row_to_link(r)
            if href not in seen:
                seen.add(href)
                links.append(href)

        print(f"[INFO] Page {page}: +{len(rows)} rows, total unique links: {len(links)}")
        page += 1
        time.sleep(delay)

    return links

# ----------------------------
# Ad page parsing
# ----------------------------
def get_url_soup(url):
    r = SESSION.get(url, timeout=25)
    r.raise_for_status()
    return BeautifulSoup(r.text, 'lxml')

def extract_text_by_id(soup, el_id, default="NA"):
    el = soup.find(id=el_id)
    return el.get_text(strip=True) if el else default

def extract_datums(soup):
    txt_nodes = soup.find_all(string=re.compile(r'\bDatums:'))
    if not txt_nodes:
        return "NA"
    node = txt_nodes[0]
    parent_text = node.parent.get_text(" ", strip=True) if node.parent else str(node)
    m = re.search(r'(\d{2}\.\d{2}\.\d{4}\.|\d{4}-\d{2}-\d{2})', parent_text)
    return m.group(1) if m else parent_text

def parse_ad_details(url):
    soup = get_url_soup(url)
    data = {
        "Link": url,
        "Pilseta":       extract_text_by_id(soup, "tdo_20"),
        "Iela":          extract_text_by_id(soup, "tdo_11"),
        "Ciems":         extract_text_by_id(soup, "tdo_368"),   # Ciems (village)
        "Platiba":       extract_text_by_id(soup, "tdo_3"),
        "Cena":          extract_text_by_id(soup, "tdo_8"),
        "Zemes Tips":    extract_text_by_id(soup, "tdo_228"),
        "Zemes Numurs":  extract_text_by_id(soup, "tdo_1631"),
        "Datums":        extract_datums(soup),
    }
    return data

def data_collection_date():
    return datetime.today().strftime('%Y-%m-%d')

# ----------------------------
# Main scrape
# ----------------------------
def scrape_ss_lv_plots_and_lands_all():
    base_sell_url = "https://www.ss.lv/lv/real-estate/plots-and-lands/sell/"

    # 1) Collect ALL ad links across ALL pages under /sell/
    links = collect_all_listing_links(base_sell_url)

    # 2) Fetch details for each ad (with a polite delay)
    details = []
    for i, link in enumerate(links, start=1):
        try:
            details.append(parse_ad_details(link))
        except Exception as e:
            print(f"[WARN] {i}/{len(links)} failed: {link} -> {e}")
        time.sleep(0.12)

    df = pd.DataFrame(details)
    if df.empty:
        return df

    # 3) Add collection date
    df['Datu iev.'] = data_collection_date()

    # 4) Normalize price/area like before
    df['Cena'] = df['Cena'].fillna('NA').astype(str)
    df['Cena EUR'] = (
        df['Cena']
        .str.extract(r'([\d\s]+)\s*€', expand=False)
        .str.replace(' ', '', regex=False)
    )
    df['Cena m2'] = (
        df['Cena']
        .str.extract(r'\(([\d\.,\s]+)\s*€/m²\)', expand=False)
        .str.replace(' ', '', regex=False)
        .str.replace(',', '.', regex=False)
    )

    df['Platiba'] = df['Platiba'].fillna('NA').astype(str)
    plat_split = df['Platiba'].str.extract(r'([\d\.,]+)\s*(.*)')
    df['Platiba Daudzums'] = (
        plat_split[0]
        .str.replace(' ', '', regex=False)
        .str.replace(',', '.', regex=False)
    )
    df['Platiba Mervieniba'] = plat_split[1].fillna('')

    for col in ['Cena EUR', 'Cena m2', 'Platiba Daudzums']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 5) Drop raw columns if you don't need them
    df = df.drop(columns=['Platiba', 'Cena'], errors='ignore')

    # 6) Ensure uniqueness by link (in case of cross-page promos/dups)
    df = df.drop_duplicates(subset=['Link']).reset_index(drop=True)

    return df

# ----------------------------
# Run
# ----------------------------
if __name__ == "__main__":
    df_zeme = scrape_ss_lv_plots_and_lands_all()
    print(f"Total adverts scraped: {len(df_zeme)}")
    print(df_zeme.head(10).to_string(index=False))
    # Export manually if needed:
    # df_zeme.to_csv("ss_lv_zeme.csv", index=False)
    # df_zeme.to_excel("ss_lv_zeme.xlsx", index=False)
    # df_zeme.to_parquet("ss_lv_zeme.parquet", index=False)
