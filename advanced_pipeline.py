"""
Advanced IMDB scraping + cleaning + anomaly detection pipeline.

Usage:
  - Configure CHROME_DRIVER_PATH or ensure chromedriver is in PATH.
  - pip install -r requirements (selenium, pandas, numpy, scipy, tqdm)
  - Run: python advanced_pipeline.py

Outputs:
  - movies_cleaned.json : cleaned record list (React-friendly)
  - movies_analysis.json: summary + anomalies found (optional)
"""

import re
import time
import json
import math
import logging
from typing import List, Dict, Optional

import numpy as np
import requests
import pandas as pd
from scipy import stats
import random
from time import sleep
import concurrent.futures
from bs4 import BeautifulSoup
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# webdriver-manager to auto-install compatible chromedriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Optional progress bar if available
try:
    from tqdm import tqdm
except Exception:
    tqdm = lambda x: x

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("imdb-advanced")

# ---- Configuration ----
CHROME_DRIVER_PATH = None  # or "/path/to/chromedriver"
HEADLESS = True
PAGE_LOAD_WAIT = 8  # seconds max for WebDriverWait
# Keep polite by default, but avoid long demos for small limits.
SLEEP_BETWEEN_REQUESTS = 0.1
DEFAULT_LIMIT = 200  # change as needed

# In "fast" runs we can skip Selenium entirely for the link collection step by
# using IMDB's server-rendered search endpoint (top_250 pages) which is much
# faster and avoids Chrome startup.
FAST_LINKS_VIA_REQUESTS = True


def collect_top_links_via_requests(limit: int) -> List[str]:
    """Collect up to `limit` IMDB title URLs using server-rendered search pagination."""
    search_url = "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating"
    full: List[str] = []
    seen = set()
    per_page = 50
    start = 1
    attempts = 0
    while len(full) < limit and attempts < 10:
        paged = f"{search_url}&start={start}"
        try:
            resp = requests.get(paged, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        except Exception:
            attempts += 1
            sleep(0.5 + attempts * 0.5)
            continue
        if resp.status_code != 200:
            attempts += 1
            sleep(0.5 + attempts * 0.5)
            continue
        ids = re.findall(r"/title/tt\d+", resp.text)
        for item in ids:
            base = item.split('?')[0]
            url = "https://www.imdb.com" + base + '/'
            if url not in seen:
                seen.add(url)
                full.append(url)
            if len(full) >= limit:
                break
        start += per_page
        attempts = 0
        sleep(0.2 + random.random() * 0.2)
    return full[:limit]

# ---- Utilities ----
def setup_driver(headless=True):
    options = Options()
    if headless:
        # Use headless new if available
        options.add_argument("--headless=new" if "--headless=new" else "--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Optional: set window-size to ensure consistent rendering
    options.add_argument("--window-size=1920,1080")
    # Anti-detection / more realistic headers
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    user_agent = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
    options.add_argument(f"--user-agent={user_agent}")
    # Use webdriver-manager to install matching chromedriver binary
    if CHROME_DRIVER_PATH:
        service = Service(CHROME_DRIVER_PATH)
    else:
        service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        # Small script to reduce "navigator.webdriver" being true
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        # Not all driver versions support CDP in same way; ignore if fails
        pass
    return driver

def safe_text(elem):
    try:
        return elem.text.strip()
    except Exception:
        try:
            return elem.get_attribute("textContent").strip()
        except Exception:
            return ""

def parse_duration_to_minutes(duration_str: Optional[str]) -> Optional[int]:
    """
    Convert "2h 30m" or "150 min" to integer minutes.
    Returns None if cannot parse.
    """
    if not duration_str or not isinstance(duration_str, str):
        return None
    s = duration_str.strip().lower()
    # support ISO 8601 durations appearing in JSON-LD like 'PT2H22M' or 'PT45M'
    m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', s, re.I)
    if m_iso:
        h = int(m_iso.group(1)) if m_iso.group(1) else 0
        mm = int(m_iso.group(2)) if m_iso.group(2) else 0
        # ignore seconds (group 3) for minute precision
        if h or mm:
            return h * 60 + mm
    # direct minutes like "150 min"
    m = re.search(r'(\d+)\s*min', s)
    if m:
        return int(m.group(1))
    # hours and minutes like "2h 30m", "2 h 30 min", "2h"
    m = re.search(r'(?:(\d+)\s*h(?:ours?)?)\s*(?:(\d+)\s*m(?:in)?)?', s)
    if m:
        hours = int(m.group(1)) if m.group(1) else 0
        minutes = int(m.group(2)) if m.group(2) else 0
        return hours * 60 + minutes
    # fallback: numbers only
    m = re.search(r'(\d{2,3})', s)
    if m:
        return int(m.group(1))
    return None

def parse_int_votes(vote_str: Optional[str]) -> Optional[int]:
    if vote_str is None:
        return None
    s = str(vote_str).strip()
    s = s.replace(",", "").replace(".", "")
    m = re.search(r'(\d+)', s)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None

def safe_float(s):
    try:
        if s is None or (isinstance(s, float) and np.isnan(s)):
            return None
        return float(str(s).strip())
    except Exception:
        return None

# ---- Scraping ----
class IMDbAdvancedScraper:
    def __init__(self, driver=None):
        self.own_driver = driver is None
        self.driver = driver or setup_driver(HEADLESS)
        self.wait = WebDriverWait(self.driver, PAGE_LOAD_WAIT)

    def close(self):
        if self.own_driver and self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

    def get_top_movie_links(self, chart_url="https://www.imdb.com/chart/top", limit=100) -> List[str]:
        if chart_url == "__force_requests_fallback__":
            # Skip Selenium page loading and go straight to server-rendered pagination.
            logger.info("Fast link collection: forcing requests pagination fallback")
            links = []
        else:
            logger.info("Fetching top chart page: %s", chart_url)
            self.driver.get(chart_url)
        try:
            # Wait for table rows to appear; adapt if IMDB structure changes
            if chart_url != "__force_requests_fallback__":
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody.lister-list tr")))
        except Exception:
            if chart_url != "__force_requests_fallback__":
                time.sleep(2)
        if chart_url != "__force_requests_fallback__":
            links = []
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody.lister-list tr")
                for r in rows[:limit]:
                    try:
                        a = r.find_element(By.CSS_SELECTOR, "td.titleColumn a")
                        href = a.get_attribute("href")
                        if href:
                            links.append(href.split("?")[0])
                    except Exception:
                        continue
            except Exception:
                # Fallback: find anchors that look like title links
                anchors = self.driver.find_elements(By.XPATH, "//a[contains(@href,'/title/tt')]")
                for a in anchors:
                    href = a.get_attribute("href")
                    if "/title/tt" in href:
                        links.append(href.split("?")[0])
                        if len(links) >= limit:
                            break
        # unique and preserve order
        seen = set()
        filtered = []
        for l in links:
            if l not in seen:
                seen.add(l)
                filtered.append(l)
        logger.info("Collected %d links (limit=%d)", len(filtered), limit)
        if len(filtered) >= min(limit, 1):
            return filtered[:limit]

        # Fallback: some IMDB pages (chart) may render via JS or be tricky in headless mode.
        # Use the IMDB search endpoint (server-side HTML) to extract title links reliably.
        try:
            search_url = "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating"
            logger.info("Falling back to requests to fetch links from %s", search_url)
            # Try paginating the search results to collect up to `limit` links.
            full = []
            per_page = 50
            start = 1
            attempts = 0
            while len(full) < limit and attempts < 10:
                paged = f"{search_url}&start={start}"
                try:
                    resp = requests.get(paged, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                except Exception:
                    attempts += 1
                    wait = 1 + attempts * 2
                    logger.info("Request failed, retrying after %ds", wait)
                    sleep(wait)
                    continue
                if resp.status_code != 200:
                    attempts += 1
                    wait = 1 + attempts * 2
                    logger.info("Non-200 status %d, retry after %ds", resp.status_code, wait)
                    sleep(wait)
                    continue
                ids = re.findall(r'/title/tt\d+', resp.text)
                for item in ids:
                    base = item.split('?')[0]
                    url = "https://www.imdb.com" + base + '/'
                    if url not in seen:
                        seen.add(url)
                        full.append(url)
                    if len(full) >= limit:
                        break
                # move to next page
                start += per_page
                attempts = 0
                # be polite and randomize
                sleep(0.5 + random.random())
            logger.info("Fallback collected %d links via pagination", len(full))
            return full[:limit]
        except Exception:
            logger.exception("Fallback requests-based scraping failed")

        return filtered[:limit]

    def scrape_movie(self, url) -> Dict:
        logger.debug("Scraping movie page: %s", url)
        data = {
            "url": url,
            "title": None,
            "year": None,
            "rating": None,
            "metascore": None,
            "duration": None,
            "duration_min": None,
            "genres": [],
            "votes": None,
        }
        try:
            self.driver.get(url)
            # wait for main content
            try:
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            except Exception:
                time.sleep(1)
            # title
            try:
                h1 = self.driver.find_element(By.TAG_NAME, "h1")
                data["title"] = safe_text(h1)
            except Exception:
                pass
            page_text = self.driver.page_source

            # year - try various selectors
            year = None
            selectors = [
                "span#titleYear a",  # older structure
                "a[href^='/releaseinfo']",
                "span.sc-8c396aa2-2[href]"  # attempt (may not match)
            ]
            for sel in selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = safe_text(el)
                    if txt and re.search(r'\d{4}', txt):
                        year = re.search(r'(\d{4})', txt).group(1)
                        break
                except Exception:
                    continue
            # fallback: extract 4-digit year near title from page text
            if not year:
                m = re.search(r'\b(19|20)\d{2}\b', page_text)
                if m:
                    year = m.group(0)
            if year:
                data["year"] = int(year)

            # rating
            rating = None
            rating_selectors = [
                "span[itemprop='ratingValue']",
                "div[data-testid='hero-rating-bar__aggregate-rating__score'] span:first-child",
                "span.sc-7ab21ed2-1.jGRxWM"  # old class example
            ]
            for sel in rating_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = safe_text(el)
                    if txt:
                        rating = txt
                        break
                except Exception:
                    continue
            # fallback searching numbers like 9.2 in the page near 'IMDb'
            if not rating:
                m = re.search(r'(\d\.\d)\s*\/\s*10', page_text)
                if m:
                    rating = m.group(1)
            data["rating"] = safe_float(rating)

            # votes
            votes = None
            vote_selectors = [
                "span[itemprop='ratingCount']",
                "div[data-testid='hero-rating-bar__aggregate-rating__score'] + div a > div",
                "div.imdbRating span small",
                "div[data-testid='title-pc-principal-credit']"
            ]
            for sel in vote_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = safe_text(el)
                    v = parse_int_votes(txt)
                    if v:
                        votes = v
                        break
                except Exception:
                    continue
            # fallback from page_text
            if not votes:
                m = re.search(r'([0-9][0-9,\.] {2,})\s+user', page_text)
                if not m:
                    m = re.search(r'([\d,]+)\s*ratings', page_text, re.I)
                if m:
                    votes = parse_int_votes(m.group(1))
            data["votes"] = votes

            # metascore
            metascore = None
            meta_selectors = [
                "div.metacriticScore span",
                "span.score-meta",
                "span[class*='metascore']"
            ]
            for sel in meta_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = safe_text(el)
                    if txt and re.search(r'\d+', txt):
                        metascore = int(re.search(r'(\d+)', txt).group(1))
                        break
                except Exception:
                    continue
            # fallback via regex
            if metascore is None:
                m = re.search(r'Metascore</span>\s*<span[^>]*>(\d+)</span>', page_text, re.I)
                if m:
                    metascore = int(m.group(1))
            data["metascore"] = metascore

            # duration - try selectors else regex
            duration = None
            dur_selectors = [
                "time",
                "ul[data-testid='hero-title-block__metadata'] li",
                "li[data-testid='title-techspec_runtime']"
            ]
            for sel in dur_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = safe_text(el)
                    if txt and re.search(r'(\d+\s*h)|(\d+\s*min)|(\d+\s*m)', txt, re.I):
                        duration = txt
                        break
                except Exception:
                    continue
            if not duration:
                m = re.search(r'(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)', page_text, re.I)
                if m:
                    duration = m.group(1)
                else:
                    m = re.search(r'(\d{2,3})\s*min', page_text, re.I)
                    if m:
                        duration = m.group(0)
            data["duration"] = duration
            data["duration_min"] = parse_duration_to_minutes(duration)

            # genres - try data-testid or genre links
            genres = []
            try:
                # 1) Try modern data-testid container
                try:
                    els = self.driver.find_elements(By.CSS_SELECTOR, "div[data-testid='genres'] a, div[data-testid='title-genres'] a")
                    for e in els:
                        t = safe_text(e)
                        if t:
                            genres.append(t)
                except Exception:
                    pass

                # 2) generic anchor href search
                if not genres:
                    anchors = self.driver.find_elements(By.XPATH, "//a[contains(@href,'/search/title?genres=') or contains(@href,'/search/title/?genres=')]")
                    for a in anchors:
                        txt = safe_text(a)
                        if txt:
                            genres.append(txt)

                # 3) JSON-LD fallback: parse script[type='application/ld+json'] for genre
                if not genres:
                    try:
                        scripts = self.driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
                        for s in scripts:
                            try:
                                j = s.get_attribute('innerText')
                                if '"@type"' in j and 'Movie' in j:
                                    import json as _json
                                    doc = _json.loads(j)
                                    g = doc.get('genre')
                                    if isinstance(g, list):
                                        genres.extend([str(x) for x in g if x])
                                    elif isinstance(g, str):
                                        genres.append(g)
                                    break
                            except Exception:
                                continue
                    except Exception:
                        pass

                # 4) regex fallback on page source: <a href="/search/title?genres=...">Genre</a>
                if not genres:
                    try:
                        page = page_text
                        m = re.findall(r'<a[^>]+href="(/search/title\?genres=[^\"]+)"[^>]*>([^<]+)</a>', page)
                        for href, label in m:
                            label = label.strip()
                            if label:
                                genres.append(label)
                    except Exception:
                        pass
            except Exception:
                pass
            data["genres"] = list(dict.fromkeys([g for g in genres if g]))

        except Exception as e:
            logger.exception("Error scraping %s: %s", url, e)
        return data

    def scrape_many(self, links: List[str], sleep=SLEEP_BETWEEN_REQUESTS):
        results = []
        for link in tqdm(links):
            try:
                data = self.scrape_movie(link)
                results.append(data)
            except Exception:
                logger.exception("Failed on %s", link)
            time.sleep(sleep)
        return results

    def scrape_many_requests(self, links: List[str], max_workers: int = 8):
        """
        Faster scraper using requests + BeautifulSoup in parallel. This avoids starting a browser per page
        and is suitable when IMDB pages are server-rendered sufficiently for our regex/bs4 parsing.
        """
        headers = {"User-Agent": "Mozilla/5.0"}
        results = []

        def _scrape_req(url):
            try:
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200:
                    logger.warning("requests scrape non-200 %d for %s", r.status_code, url)
                    return {"url": url}
                page = r.text
                soup = BeautifulSoup(page, "html.parser")
                data = {"url": url, "title": None, "year": None, "rating": None, "metascore": None,
                        "duration": None, "duration_min": None, "genres": [], "votes": None}
                # title
                h1 = soup.find('h1')
                if h1:
                    data['title'] = h1.get_text(strip=True)
                # year
                m = re.search(r'\b(19|20)\d{2}\b', page)
                if m:
                    data['year'] = int(m.group(0))
                # rating
                m = re.search(r'([0-9]\.[0-9])\s*/\s*10', page)
                if m:
                    data['rating'] = safe_float(m.group(1))
                else:
                    # look for aggregate rating meta
                    meta = soup.find('span', attrs={'itemprop': 'ratingValue'})
                    if meta:
                        data['rating'] = safe_float(meta.get_text(strip=True))
                # votes
                m = re.search(r'([\d,]+)\s*user', page, re.I)
                if not m:
                    m = re.search(r'([\d,]+)\s*ratings', page, re.I)
                if m:
                    data['votes'] = parse_int_votes(m.group(1))
                # metascore
                m = re.search(r'(?:Metascore|metascore)[^\d]*(\d{2,3})', page, re.I)
                if m:
                    data['metascore'] = int(m.group(1))
                # duration
                m = re.search(r'(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)', page, re.I)
                if m:
                    data['duration'] = m.group(1)
                    data['duration_min'] = parse_duration_to_minutes(data['duration'])
                else:
                    m = re.search(r'(\d{2,3})\s*min', page, re.I)
                    if m:
                        data['duration'] = m.group(0)
                        data['duration_min'] = parse_duration_to_minutes(data['duration'])
                # genres from anchors
                genres = []
                for a in soup.find_all('a', href=True):
                    if '/search/title?genres=' in a['href']:
                        txt = a.get_text(strip=True)
                        if txt:
                            genres.append(txt)
                # JSON-LD
                if not genres:
                    for s in soup.find_all('script', type='application/ld+json'):
                        try:
                            j = json.loads(s.string)
                            g = j.get('genre')
                            if isinstance(g, list):
                                genres.extend(g)
                            elif isinstance(g, str):
                                genres.append(g)
                        except Exception:
                            continue
                data['genres'] = list(dict.fromkeys([g for g in genres if g]))
                return data
            except Exception:
                logger.exception('requests-based scrape failed for %s', url)
                return {"url": url}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_scrape_req, url) for url in links]
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
        return results

# ---- Data cleaning and analysis ----
def build_dataframe(records: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    # normalize columns
    for col in ["rating", "metascore", "duration_min", "votes", "year"]:
        if col not in df.columns:
            df[col] = np.nan
    # convert types
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["metascore"] = pd.to_numeric(df["metascore"], errors="coerce")
    df["duration_min"] = pd.to_numeric(df["duration_min"], errors="coerce")
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce", downcast="integer")
    df["year"] = pd.to_numeric(df["year"], errors="coerce", downcast="integer")
    return df

def impute_numeric_with_median(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c in df.columns:
            median_val = df[c].median(skipna=True)
            # If median cannot be computed (all values are NaN), skip imputation
            if pd.isna(median_val):
                logger.info("Median for column %s is NaN; skipping imputation", c)
                continue
            df[c] = df[c].fillna(median_val)
            logger.info("Imputed column %s with median=%s", c, median_val)
    return df

def compute_iqr_stats(df: pd.DataFrame, column: str):
    series = df[column].dropna()
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return {"q1": q1, "q3": q3, "iqr": iqr, "lower": lower, "upper": upper}

def detect_anomalies(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    results = {}
    df_num = df.copy()
    df_num["rating"] = pd.to_numeric(df_num["rating"], errors="coerce")
    df_num["metascore"] = pd.to_numeric(df_num["metascore"], errors="coerce")
    df_num["votes"] = pd.to_numeric(df_num["votes"], errors="coerce")

    r_stats = compute_iqr_stats(df_num, "rating")
    m_stats = compute_iqr_stats(df_num, "metascore")
    logger.info("Rating stats: %s", r_stats)
    logger.info("Metascore stats: %s", m_stats)

    cond = (
        (df_num["rating"] > r_stats["upper"])
        & (df_num["metascore"].notna())
        & (df_num["metascore"] < m_stats["lower"])
    )
    results["rating_high_meta_low"] = df_num[cond].copy()

    dur_stats = compute_iqr_stats(df_num, "duration_min")
    cond_dur = (df_num["duration_min"] < dur_stats["lower"]) | (df_num["duration_min"] > dur_stats["upper"])
    results["duration_outliers"] = df_num[cond_dur].copy()

    reg_df = df_num[(df_num["votes"] > 0) & df_num["rating"].notna()].copy()
    anomalies_resid = pd.DataFrame()
    if len(reg_df) >= 5:
        X = np.log(reg_df["votes"].astype(float))
        Y = reg_df["rating"].astype(float)
        try:
            slope, intercept, r_value, p_value, std_err = stats.linregress(X, Y)
            reg_df["pred_rating"] = intercept + slope * X
            reg_df["residual"] = reg_df["rating"] - reg_df["pred_rating"]
            resid_std = reg_df["residual"].std(ddof=0)
            threshold = max(3 * resid_std, 1.5)
            anomalies_resid = reg_df[np.abs(reg_df["residual"]) > threshold].copy()
            anomalies_resid["residual_z"] = (anomalies_resid["residual"] - anomalies_resid["residual"].mean()) / (resid_std if resid_std else 1.0)
            logger.info("Regression slope=%.4f intercept=%.4f r=%.4f resid_std=%.4f threshold=%.4f",
                        slope, intercept, r_value, resid_std, threshold)
        except Exception:
            logger.exception("Regression-based anomaly detection failed")
    results["rating_votes_inconsistent"] = anomalies_resid

    return results

def prepare_react_json(df: pd.DataFrame, out_path="movies_cleaned.json", charts_out="movies_charts.json"):
    df_out = df.copy()
    df_out["title"] = df_out["title"].astype(str)
    df_out["year"] = df_out["year"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_out["rating"] = df_out["rating"].apply(lambda x: float(x) if not pd.isna(x) else None)
    df_out["metascore"] = df_out["metascore"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_out["duration_min"] = df_out["duration_min"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_out["votes"] = df_out["votes"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_out["genres"] = df_out["genres"].apply(lambda g: g if isinstance(g, list) else ([] if pd.isna(g) else [g]))

    records = df_out.to_dict(orient="records")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    logger.info("Wrote cleaned records to %s (n=%d)", out_path, len(records))

    histogram_rating = df_out["rating"].dropna().tolist()
    histogram_metascore = df_out["metascore"].dropna().tolist()
    # Replace NaN with None for JSON serialization
    scatter_df = df_out[["title", "rating", "metascore", "duration_min", "votes"]].copy()
    scatter_df = scatter_df.where(pd.notna(scatter_df), None)
    scatter = scatter_df.to_dict(orient="records")
    charts_payload = {
        "rating_hist": histogram_rating,
        "metascore_hist": histogram_metascore,
        "scatter": scatter
    }
    with open(charts_out, "w", encoding="utf-8") as f:
        json.dump(charts_payload, f, ensure_ascii=False, indent=2)
    logger.info("Wrote charts payload to %s", charts_out)


def save_boxplot(df: pd.DataFrame, column: str, out_file: str):
    """Save a simple boxplot for `column` to `out_file`."""
    series = df[column].dropna().astype(float)
    if series.empty:
        logger.info("No data for boxplot %s", column)
        return
    plt.figure(figsize=(6, 4))
    sns.boxplot(x=series)
    plt.title(f"Boxplot of {column}")
    plt.xlabel(column)
    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()
    logger.info("Saved boxplot %s", out_file)


def prepare_final_json(df: pd.DataFrame, anomalies: Dict[str, pd.DataFrame], analysis_summary: Dict, out_file="movies_final.json"):
    """Create a final JSON containing cleaned rows, anomaly flags and analysis summary.

        Each record contains the cleaned fields plus boolean flags:
            - anomaly_rating_high_meta_low
            - anomaly_duration_outlier
            - anomaly_rating_votes_inconsistent
            - is_anomaly (union of the above)
    """
    df2 = df.copy()
    # build sets for quick lookup (use titles + year)
    def key(row):
        return (str(row.get('title')), int(row.get('year')) if not pd.isna(row.get('year')) else None)

    s1 = set()
    for ix, r in anomalies.get('rating_high_meta_low', pd.DataFrame()).iterrows():
        s1.add((str(r.get('title')), int(r.get('year')) if not pd.isna(r.get('year')) else None))
    s2 = set()
    for ix, r in anomalies.get('duration_outliers', pd.DataFrame()).iterrows():
        s2.add((str(r.get('title')), int(r.get('year')) if not pd.isna(r.get('year')) else None))
    s3 = set()
    for ix, r in anomalies.get('rating_votes_inconsistent', pd.DataFrame()).iterrows():
        s3.add((str(r.get('title')), int(r.get('year')) if not pd.isna(r.get('year')) else None))

    records = []
    for _, row in df2.iterrows():
        rec = {
            'title': row.get('title'),
            'url': row.get('url'),
            'year': int(row.get('year')) if not pd.isna(row.get('year')) else None,
            'rating': float(row.get('rating')) if not pd.isna(row.get('rating')) else None,
            'metascore': int(row.get('metascore')) if not pd.isna(row.get('metascore')) else None,
            'duration_min': int(row.get('duration_min')) if not pd.isna(row.get('duration_min')) else None,
            'genres': row.get('genres') if isinstance(row.get('genres'), list) else [],
            'votes': int(row.get('votes')) if not pd.isna(row.get('votes')) else None,
        }
        k = (str(rec['title']), rec['year'])
        rec['anomaly_rating_high_meta_low'] = k in s1
        rec['anomaly_duration_outlier'] = k in s2
        rec['anomaly_rating_votes_inconsistent'] = k in s3
        rec['is_anomaly'] = bool(
            rec['anomaly_rating_high_meta_low']
            or rec['anomaly_duration_outlier']
            or rec['anomaly_rating_votes_inconsistent']
        )
        records.append(rec)

    out = {
        'summary': analysis_summary,
        'records': records
    }
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    logger.info("Wrote final JSON to %s (n=%d)", out_file, len(records))

def run_pipeline(limit=DEFAULT_LIMIT, chart_url="https://www.imdb.com/chart/top", fast: bool = False, threads: int = 8):
    """
    Orchestrate scraping -> cleaning -> analysis -> export.
    If fast=True, use the requests-based parallel scraper (faster but may miss JS-only content).
    """
    # Fast path: avoid launching Chrome entirely.
    if fast and FAST_LINKS_VIA_REQUESTS:
        links = collect_top_links_via_requests(limit=limit)

        # Reuse the existing request-based scraping logic without constructing a Selenium driver.
        def scrape_many_requests_only(links: List[str], max_workers: int = 8):
            headers = {"User-Agent": "Mozilla/5.0"}
            results = []

            def _scrape_req(url):
                try:
                    r = requests.get(url, headers=headers, timeout=10)
                    if r.status_code != 200:
                        logger.warning("requests scrape non-200 %d for %s", r.status_code, url)
                        return {"url": url}
                    page = r.text
                    soup = BeautifulSoup(page, "html.parser")
                    data = {"url": url, "title": None, "year": None, "rating": None, "metascore": None,
                            "duration": None, "duration_min": None, "genres": [], "votes": None}
                    h1 = soup.find('h1')
                    if h1:
                        data['title'] = h1.get_text(strip=True)
                    m = re.search(r'\b(19|20)\d{2}\b', page)
                    if m:
                        data['year'] = int(m.group(0))
                    m = re.search(r'([0-9]\.[0-9])\s*/\s*10', page)
                    if m:
                        data['rating'] = safe_float(m.group(1))
                    else:
                        meta = soup.find('span', attrs={'itemprop': 'ratingValue'})
                        if meta:
                            data['rating'] = safe_float(meta.get_text(strip=True))
                    m = re.search(r'([\d,]+)\s*user', page, re.I)
                    if not m:
                        m = re.search(r'([\d,]+)\s*ratings', page, re.I)
                    if m:
                        data['votes'] = parse_int_votes(m.group(1))
                    m = re.search(r'(?:Metascore|metascore)[^\d]*(\d{2,3})', page, re.I)
                    if m:
                        data['metascore'] = int(m.group(1))
                    m = re.search(r'(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)', page, re.I)
                    if m:
                        data['duration'] = m.group(1)
                        data['duration_min'] = parse_duration_to_minutes(data['duration'])
                    else:
                        m = re.search(r'(\d{2,3})\s*min', page, re.I)
                        if m:
                            data['duration'] = m.group(0)
                            data['duration_min'] = parse_duration_to_minutes(data['duration'])
                    genres = []
                    for a in soup.find_all('a', href=True):
                        if '/search/title?genres=' in a['href']:
                            txt = a.get_text(strip=True)
                            if txt:
                                genres.append(txt)
                    if not genres:
                        for s in soup.find_all('script', type='application/ld+json'):
                            try:
                                j = json.loads(s.string)
                                g = j.get('genre')
                                if isinstance(g, list):
                                    genres.extend(g)
                                elif isinstance(g, str):
                                    genres.append(g)
                            except Exception:
                                continue
                    data['genres'] = list(dict.fromkeys([g for g in genres if g]))
                    return data
                except Exception:
                    logger.exception('requests-based scrape failed for %s', url)
                    return {"url": url}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = [ex.submit(_scrape_req, url) for url in links]
                for f in concurrent.futures.as_completed(futures):
                    results.append(f.result())
            return results

        raw_records = scrape_many_requests_only(links, max_workers=threads)
    else:
        scraper = IMDbAdvancedScraper()
        try:
            links = scraper.get_top_movie_links(chart_url=chart_url, limit=limit)
            if fast:
                raw_records = scraper.scrape_many_requests(links, max_workers=threads)
            else:
                raw_records = scraper.scrape_many(links)
        finally:
            scraper.close()

    df = build_dataframe(raw_records)

    # If no records were scraped, write empty output files and return early
    if df.empty or len(raw_records) == 0:
        logger.warning("No records scraped (n=%d). Writing empty output files.", len(raw_records))
        with open("movies_cleaned.json", "w", encoding="utf-8") as f:
            json.dump([], f)
        with open("movies_charts.json", "w", encoding="utf-8") as f:
            json.dump({"rating_hist": [], "metascore_hist": [], "scatter": []}, f)
        with open("movies_analysis.json", "w", encoding="utf-8") as f:
            json.dump({"summary": {"n_records": 0, "anomalies_counts": {}}, "anomalies": {}}, f)
        return pd.DataFrame(), {}, {"n_records": 0, "anomalies_counts": {}}

    df_clean = df.copy()
    df_clean["votes"] = df_clean["votes"].apply(lambda v: parse_int_votes(v) if not pd.isna(v) else None).astype('float')

    df_clean = impute_numeric_with_median(df_clean, ["metascore", "rating"])

    if "duration" in df_clean.columns:
        df_clean["duration_min"] = df_clean.apply(
            lambda row: row["duration_min"] if not pd.isna(row["duration_min"]) else parse_duration_to_minutes(row.get("duration")), axis=1
        )

    df_clean = impute_numeric_with_median(df_clean, ["votes", "duration_min", "year"])

    df_clean["votes"] = df_clean["votes"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_clean["metascore"] = df_clean["metascore"].apply(lambda x: int(x) if not pd.isna(x) else None)
    df_clean["rating"] = df_clean["rating"].apply(lambda x: float(x) if not pd.isna(x) else None)
    df_clean["duration_min"] = df_clean["duration_min"].apply(lambda x: int(x) if not pd.isna(x) else None)

    anomalies = detect_anomalies(df_clean)

    prepare_react_json(df_clean, out_path="movies_cleaned.json", charts_out="movies_charts.json")

    analysis_summary = {
        "n_records": len(df_clean),
        "rating_stats": compute_iqr_stats(df_clean, "rating"),
        "metascore_stats": compute_iqr_stats(df_clean, "metascore"),
        "duration_stats": compute_iqr_stats(df_clean, "duration_min"),
        "anomalies_counts": {k: len(v) for k, v in anomalies.items()}
    }
    with open("movies_analysis.json", "w", encoding="utf-8") as f:
        anomalies_small = {}
        for k, df_anom in anomalies.items():
            if df_anom is None or df_anom.empty:
                anomalies_small[k] = []
                continue
            desired_cols = ["title", "year", "rating", "metascore", "votes", "duration_min"]
            present = [c for c in desired_cols if c in df_anom.columns]
            if not present:
                anomalies_small[k] = []
                continue
            sel = df_anom[present].copy()
            sel = sel.where(pd.notna(sel), None)
            anomalies_small[k] = sel.to_dict(orient="records")
        json.dump({"summary": analysis_summary, "anomalies": anomalies_small}, f, ensure_ascii=False, indent=2)
    logger.info("Wrote analysis summary to movies_analysis.json")

    # Save final combined JSON (cleaned records + anomaly flags + analysis summary)
    try:
        prepare_final_json(df_clean, anomalies, analysis_summary, out_file="movies_final.json")
    except Exception:
        logger.exception("Failed to write movies_final.json")

    # Save boxplots for instructor review
    try:
        save_boxplot(df_clean, 'rating', 'boxplot_rating.png')
        save_boxplot(df_clean, 'metascore', 'boxplot_metascore.png')
    except Exception:
        logger.exception("Failed to save boxplots")

    return df_clean, anomalies, analysis_summary

if __name__ == "__main__":
    df_cleaned, anomalies_found, summary = run_pipeline(limit=100)
    logger.info("Pipeline finished. Records=%d", summary["n_records"])
    for k, v in summary["anomalies_counts"].items():
        logger.info("Anomalies %s: %d", k, v)
