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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0 Safari/537.36"
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
# Helpers for listing pages
# ----------------------------
def getUrlList(url, prefix='https://www.ss.com', postfix='sell/', tag='a', class_='a_category'):
    resp = SESSION.get(url, timeout=15)
    if resp.status_code != 200:
        print(f'Unexpected status code {resp.status_code}. Stopping parse')
        return []
    soup = BeautifulSoup(resp.text, 'lxml')
    return [prefix + el['href'] + postfix for el in soup.find_all(tag, class_)]

def processRow(row, baseurl='https://www.ss.com'):
    tds = row.find_all('td')
    link = baseurl + tds[1].a['href']
    title = tds[2].get_text(strip=True).replace('\r', '').replace('\n', '')
    return [link, title]

def getRows(url):
    resp = SESSION.get(url, timeout=15)
    if resp.status_code != 200:
        print(f"Bad request {resp.status_code} for {url}")
        return []
    soup = BeautifulSoup(resp.text, 'lxml')
    rows = []
    for el in soup.find_all('tr'):
        if 'id' in el.attrs and 'tr_' in el.attrs['id']:
            rows.append(el)
    return rows[:-1] if rows else []

def processPage(url):
    rows = getRows(url)
    return [processRow(r) for r in rows]

def processPages(urls, delay=0.15):
    results = []
    for u in urls:
        results += processPage(u)
        time.sleep(delay)
    return results

# ----------------------------
# Ad page parsing
# ----------------------------
def get_url_soup(url):
    r = SESSION.get(url, timeout=20)
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
        "Pilseta": extract_text_by_id(soup, "tdo_20"),
        "Iela": extract_text_by_id(soup, "tdo_11"),
        "Ciems": extract_text_by_id(soup, "tdo_368"),   # NEW: Ciems
        "Platiba": extract_text_by_id(soup, "tdo_3"),
        "Cena": extract_text_by_id(soup, "tdo_8"),
        "Zemes Tips": extract_text_by_id(soup, "tdo_228"),
        "Zemes Numurs": extract_text_by_id(soup, "tdo_1631"),
        "Datums": extract_datums(soup),
    }
    return data

def data_collection_date():
    return datetime.today().strftime('%Y-%m-%d')

# ----------------------------
# Main scrape function
# ----------------------------
def scrape_ss_lv_plots_and_lands():
    base_url = "https://www.ss.lv/lv/real-estate/plots-and-lands/"
    region_listing_urls = getUrlList(base_url)

    listings = processPages(region_listing_urls)
    if not listings:
        return pd.DataFrame(columns=[
            'Link','Pilseta','Iela','Ciems','Platiba','Cena','Zemes Tips',
            'Zemes Numurs','Datums','Datu iev.','Cena EUR','Cena m2',
            'Platiba Daudzums','Platiba Mervieniba'
        ])

    links = [row[0] for row in listings]

    details = []
    for i, link in enumerate(links, start=1):
        try:
            details.append(parse_ad_details(link))
        except Exception as e:
            print(f"Failed {i}/{len(links)}: {link} -> {e}")
        time.sleep(0.15)

    df = pd.DataFrame(details)
    if df.empty:
        return df

    df['Datu iev.'] = data_collection_date()

    # Price split
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

    # Area split
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

    df = df.drop(columns=['Platiba', 'Cena'], errors='ignore')
    return df

# ----------------------------
# Run & preview
# ----------------------------
if __name__ == "__main__":
    df_zeme = scrape_ss_lv_plots_and_lands()
    print(f"Rows: {len(df_zeme)}")
    print(df_zeme.head(10).to_string(index=False))
    # Export manually if needed:
    # df_zeme.to_csv("ss_lv_zeme.csv", index=False)
    # df_zeme.to_excel("ss_lv_zeme.xlsx", index=False)
    # df_zeme.to_parquet("ss_lv_zeme.parquet", index=False)
