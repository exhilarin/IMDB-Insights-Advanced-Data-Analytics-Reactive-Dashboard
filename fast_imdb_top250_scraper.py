from __future__ import annotations

import json
import math
import os
import re
from typing import Dict, List, Optional, Tuple

import argparse
import concurrent.futures
import random
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

TOP_250_URL = "https://www.imdb.com/chart/top/"

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9"
}

def parse_duration_to_minutes(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    text = str(s).strip().lower()

    m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', text, re.I)
    if m_iso:
        h = int(m_iso.group(1)) if m_iso.group(1) else 0
        mm = int(m_iso.group(2)) if m_iso.group(2) else 0
        if h or mm:
            return h * 60 + mm

    m = re.search(r"(\d+)\s*min", text)
    if m:
        return int(m.group(1))

    m = re.search(r"(?:(\d+)\s*h)\s*(?:(\d+)\s*m)?", text)
    if m:
        h = int(m.group(1)) if m.group(1) else 0
        mm = int(m.group(2)) if m.group(2) else 0
        return h * 60 + mm

    m = re.search(r"(\d{2,3})", text)
    return int(m.group(1)) if m else None


def parse_votes(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    s = str(text).replace(",", "").replace(".", "").strip()
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def safe_float(x: Optional[str]) -> Optional[float]:
    if x is None: return None
    try: return float(str(x).strip())
    except Exception: return None


def safe_int(x: Optional[str]) -> Optional[int]:
    if x is None: return None
    try: return int(str(x).strip())
    except Exception: return None


def build_fast_chrome(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    
    # Critical: Force English for Selenium
    options.add_argument("--lang=en-US")
    
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={UA}")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
    }
    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def extract_top250_from_dom(driver: webdriver.Chrome) -> List[Dict]:
    driver.get(TOP_250_URL)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.ipc-metadata-list")))

    items = driver.find_elements(By.CSS_SELECTOR, "ul.ipc-metadata-list > li")
    records: List[Dict] = []

    for li in items:
        title, url, year, duration_text, rating = None, None, None, None, None
        try:
            a = li.find_element(By.CSS_SELECTOR, "a.ipc-title-link-wrapper")
            title = re.sub(r"^\d+\.\s*", "", a.text.strip())
            href = a.get_attribute("href")
            url = href.split("?")[0] if href else None
        except Exception: pass

        try:
            meta_spans = li.find_elements(By.CSS_SELECTOR, "span.cli-title-metadata-item")
            if len(meta_spans) >= 1: year = safe_int(meta_spans[0].text)
            if len(meta_spans) >= 2: duration_text = meta_spans[1].text.strip()
        except Exception: pass

        try:
            m = re.search(r"\b([0-9]\.[0-9])\b", li.text)
            rating = safe_float(m.group(1)) if m else None
        except Exception: rating = None

        if title and url:
            records.append({
                "url": url, "title": title, "year": year, "rating": rating,
                "metascore": None, "duration": duration_text,
                "duration_min": parse_duration_to_minutes(duration_text),
                "votes": None, "genres": [],
            })
    return records


def fetch_details_requests(url: str, session: requests.Session) -> Tuple:
    try:
        resp = session.get(url, timeout=8)
        if resp.status_code != 200: return None, None, None, None, [], None, None
        html, soup = resp.text, BeautifulSoup(resp.text, "html.parser")
        
        metascore = None
        m = re.search(r"(?:Metascore|metascore)[^\d]*(\d{2,3})", html, re.I)
        if m: metascore = int(m.group(1))

        votes = None
        m = re.search(r"([\d,]+)\s*ratings", html, re.I) or re.search(r"([\d,]+)\s*user", html, re.I)
        if m: votes = parse_votes(m.group(1))

        duration_text, duration_min, episodes = None, None, None
        m = re.search(r"(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)", html, re.I)
        if m:
            duration_text = m.group(1)
            duration_min = parse_duration_to_minutes(duration_text)

        genres, year = [], None
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                j = json.loads(s.string)
                objs = j if isinstance(j, list) else [j]
                for obj in objs:
                    if not isinstance(obj, dict): continue
                    g = obj.get("genre")
                    if isinstance(g, list): genres.extend([x for x in g if isinstance(x, str)])
                    elif isinstance(g, str): genres.append(g)
                    
                    if year is None:
                        dp = obj.get('datePublished') or obj.get('startDate')
                        if dp: year = int(str(dp)[:4])
            except Exception: continue

        return metascore, votes, duration_min, duration_text, list(dict.fromkeys(genres)), year, episodes
    except Exception: return None, None, None, None, [], None, None


def fetch_details_with_retry(url: str, session: requests.Session, retries: int = 2, base_backoff: float = 0.4) -> Tuple:
    last = (None, None, None, None, [], None, None)
    for attempt in range(retries + 1):
        last = fetch_details_requests(url, session)
        if any(x is not None for x in last[:4]) or (last[4] and len(last[4]) > 0):
            return url, last
        if attempt < retries:
            time.sleep(base_backoff * (2 ** attempt) + random.random() * 0.2)
    return url, last


def iqr_outliers(series: pd.Series) -> Tuple[Dict[str, float], pd.Series]:
    s = pd.to_numeric(series, errors="coerce")
    valid = s.dropna()
    if valid.empty:
        return {"q1": 0, "q3": 0, "iqr": 0, "lower": 0, "upper": 0}, pd.Series([False]*len(series))
    q1, q3 = float(valid.quantile(0.25)), float(valid.quantile(0.75))
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return {"q1": q1, "q3": q3, "iqr": iqr, "lower": lower, "upper": upper}, (s < lower) | (s > upper)


def write_react_json(payload: Dict):
    repo_root = os.path.dirname(os.path.abspath(__file__))
    paths = [
        os.path.join(repo_root, "imdb-dashboard", "src", "movies_final.json"),
        os.path.join(repo_root, "imdb-dashboard", "public", "movies_final.json")
    ]
    
    def _sanitize(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
        if isinstance(obj, dict): return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list): return [_sanitize(v) for v in obj]
        return obj

    payload = _sanitize(payload)
    for p in paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=250)
    p.add_argument("--threads", type=int, default=24)
    p.add_argument("--autosave-every", type=int, default=25)
    args = p.parse_args(argv)

    driver = build_fast_chrome(headless=True)
    try:
        base_records = extract_top250_from_dom(driver)
    finally:
        driver.quit()

    base_records = base_records[: args.limit]
    session = requests.Session()
    session.headers.update(HEADERS)

    url_to_record = {r["url"]: r for r in base_records if r.get("url")}
    urls = list(url_to_record.keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        futures = [ex.submit(fetch_details_with_retry, url, session) for url in urls]
        done = 0
        for f in concurrent.futures.as_completed(futures):
            url, (metascore, votes, dur_min, dur_text, genres, year, episodes) = f.result()
            r = url_to_record.get(url)
            if not r: continue
            r["metascore"], r["votes"] = metascore, votes
            if dur_min: r["duration_min"], r["duration"] = dur_min, dur_text
            if genres: r["genres"] = genres
            if year: r["year"] = int(year)
            
            done += 1
            if done % 10 == 0 or done == len(urls):
                print(f"Details: {done}/{len(urls)} completed")

    df = pd.DataFrame(base_records)
    for col in ["rating", "metascore", "votes", "year", "duration_min"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    median_meta = df["metascore"].median()
    df["metascore"] = df["metascore"].fillna(median_meta if pd.notna(median_meta) else 0)

    rating_stats, rating_out = iqr_outliers(df["rating"])
    meta_stats, meta_out = iqr_outliers(df["metascore"])
    df["is_anomaly"] = (rating_out | meta_out)

    payload = {
        "summary": {
            "n_records": len(df),
            "rating_stats": rating_stats,
            "metascore_stats": meta_stats,
        },
        "records": df.where(pd.notna(df), None).to_dict(orient="records"),
    }
    write_react_json(payload)
    print(f"Saved {len(df)} records.")

if __name__ == "__main__":
    main()