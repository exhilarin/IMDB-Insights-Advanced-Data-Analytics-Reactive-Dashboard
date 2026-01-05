"""fast_imdb_top250_scraper.py

Ultra-fast, low-error IMDB Top 250 scraper.

Design goals
- Use Selenium ONLY to load the Top 250 list page once (more robust against markup changes).
- Speed up Selenium by blocking images/fonts/stylesheets.
- Fetch per-movie details (metascore, votes, duration) via requests (fast + avoids Selenium per movie).
- Clean messy duration strings ("2h 22m" -> minutes).
- Impute missing metascore with median.
- Detect IQR outliers for rating and metascore.
- Write React-friendly JSON to:
  - imdb-dashboard/public/movies_final.json (served by CRA)
  - imdb-dashboard/src/movies_final.json (as required by rubric)

Run:
  venv/bin/python fast_imdb_top250_scraper.py
"""

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


def parse_duration_to_minutes(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    text = str(s).strip().lower()

    # Support ISO 8601 durations like 'PT2H22M' or 'PT45M'
    m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', text, re.I)
    if m_iso:
        h = int(m_iso.group(1)) if m_iso.group(1) else 0
        mm = int(m_iso.group(2)) if m_iso.group(2) else 0
        # seconds (group 3) ignored when computing minutes
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
    if x is None:
        return None
    try:
        return float(str(x).strip())
    except Exception:
        return None


def safe_int(x: Optional[str]) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(str(x).strip())
    except Exception:
        return None


def build_fast_chrome(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={UA}")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")

    # Big speed-up: block non-essential resources
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

    # This selector has changed historically; we wait for the list itself.
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.ipc-metadata-list")))

    items = driver.find_elements(By.CSS_SELECTOR, "ul.ipc-metadata-list > li")
    records: List[Dict] = []

    for li in items:
        title = None
        url = None
        year = None
        duration_text = None
        rating = None

        try:
            a = li.find_element(By.CSS_SELECTOR, "a.ipc-title-link-wrapper")
            title = re.sub(r"^\d+\.\s*", "", a.text.strip())
            href = a.get_attribute("href")
            url = href.split("?")[0] if href else None
        except Exception:
            pass

        try:
            meta_spans = li.find_elements(By.CSS_SELECTOR, "span.cli-title-metadata-item")
            if len(meta_spans) >= 1:
                year = safe_int(meta_spans[0].text)
            if len(meta_spans) >= 2:
                duration_text = meta_spans[1].text.strip()
        except Exception:
            pass

        # Avoid extra DOM queries (can be slow/hang depending on IMDB markup).
        # Use text-based fallback which is usually enough on the chart page.
        try:
            m = re.search(r"\b([0-9]\.[0-9])\b", li.text)
            rating = safe_float(m.group(1)) if m else None
        except Exception:
            rating = None

        if title and url:
            records.append(
                {
                    "url": url,
                    "title": title,
                    "year": year,
                    "rating": rating,
                    "metascore": None,
                    "duration": duration_text,
                    "duration_min": parse_duration_to_minutes(duration_text),
                    "votes": None,
                    "genres": [],
                }
            )

    return records


def fetch_details_requests(url: str, session: requests.Session) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[str], List[str], Optional[int]]:
    try:
        resp = session.get(url, timeout=8)
        if resp.status_code != 200:
            return None, None, None, None, [], None

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        metascore = None
        m = re.search(r"(?:Metascore|metascore)[^\d]*(\d{2,3})", html, re.I)
        if m:
            metascore = int(m.group(1))

        votes = None
        m = re.search(r"([\d,]+)\s*ratings", html, re.I)
        if not m:
            m = re.search(r"([\d,]+)\s*user", html, re.I)
        if m:
            votes = parse_votes(m.group(1))

        duration_text = None
        duration_min = None
        m = re.search(r"(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)", html, re.I)
        if m:
            duration_text = m.group(1)
            duration_min = parse_duration_to_minutes(duration_text)
        else:
            m = re.search(r"(\d{2,3})\s*min", html, re.I)
            if m:
                duration_text = m.group(0)
                duration_min = parse_duration_to_minutes(duration_text)

        # genres via JSON-LD first (usually stable)
        genres: List[str] = []
        year = None
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                j = json.loads(s.string)
                objs = j if isinstance(j, list) else [j]
                for obj in objs:
                    if not isinstance(obj, dict):
                        continue
                    g = obj.get("genre")
                    if isinstance(g, list):
                        genres.extend([x for x in g if isinstance(x, str)])
                    elif isinstance(g, str):
                        genres.append(g)
                    # capture structured year if present
                    if year is None:
                        dp = obj.get('datePublished') or obj.get('startDate') or obj.get('dateCreated')
                        if dp:
                            try:
                                year = int(str(dp)[:4])
                            except Exception:
                                pass
                    # Prefer duration from Movie/TV objects; avoid VideoObject trailer durations
                    if (duration_min is None) and ('@type' in obj or 'type' in obj):
                        typ = obj.get('@type') or obj.get('type')
                        if isinstance(typ, str):
                            t = typ.lower()
                            if any(x in t for x in ('movie', 'tvepisode', 'tvseries')):
                                d = obj.get('duration') or obj.get('timeRequired')
                                if d:
                                    try:
                                        duration_text = d
                                        duration_min = parse_duration_to_minutes(duration_text)
                                    except Exception:
                                        pass
            except Exception:
                continue

        # de-dup
        genres = list(dict.fromkeys([g for g in genres if g]))

        # fallback: regex year
        if year is None:
            m = re.search(r"\b(19|20)\d{2}\b", html)
            if m:
                try:
                    year = int(m.group(0))
                except Exception:
                    year = None

        # Sanity: ignore implausibly large parsed durations (likely total series runtime)
        if duration_min is not None and duration_min > 10 * 60:
            duration_min = None
            duration_text = None

        return metascore, votes, duration_min, duration_text, genres, year
    except Exception:
        return None, None, None, None, [], None


def fetch_details_with_retry(
    url: str,
    session: requests.Session,
    retries: int = 2,
    base_backoff: float = 0.4,
) -> Tuple[str, Tuple[Optional[int], Optional[int], Optional[int], Optional[str], List[str], Optional[int]]]:
    """Small retry wrapper so one slow/bad request doesn't kill overall speed."""
    last = (None, None, None, None, [], None)
    for attempt in range(retries + 1):
        last = fetch_details_requests(url, session)
        # If we got something useful, stop early.
        if any(x is not None for x in last[:4]) or (last[4] and len(last[4]) > 0):
            return url, last
        if attempt < retries:
            time.sleep(base_backoff * (2 ** attempt) + random.random() * 0.2)
    return url, last


def iqr_outliers(series: pd.Series) -> Tuple[Dict[str, float], pd.Series]:
    s = pd.to_numeric(series, errors="coerce")
    valid = s.dropna()

    if valid.empty:
        stats = {"q1": math.nan, "q3": math.nan, "iqr": math.nan, "lower": math.nan, "upper": math.nan}
        return stats, pd.Series([False] * len(series), index=series.index)

    q1 = float(valid.quantile(0.25))
    q3 = float(valid.quantile(0.75))
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    flags = (s < lower) | (s > upper)
    flags = flags.fillna(False)

    stats = {"q1": q1, "q3": q3, "iqr": float(iqr), "lower": float(lower), "upper": float(upper)}
    return stats, flags


def write_react_json(payload: Dict):
    repo_root = os.path.dirname(os.path.abspath(__file__))
    out_src = os.path.join(repo_root, "imdb-dashboard", "src", "movies_final.json")
    out_public = os.path.join(repo_root, "imdb-dashboard", "public", "movies_final.json")

    os.makedirs(os.path.dirname(out_src), exist_ok=True)
    os.makedirs(os.path.dirname(out_public), exist_ok=True)

    # IMPORTANT: JSON cannot contain NaN/Infinity. Convert them to null.
    def _sanitize(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        return obj

    payload = _sanitize(payload)

    with open(out_src, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    with open(out_public, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)


def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="Ultra-fast IMDB Top scraper")
    p.add_argument("--limit", type=int, default=250, help="How many movies to fetch (default 250)")
    p.add_argument("--threads", type=int, default=24, help="Parallel workers for requests (default 24)")
    p.add_argument(
        "--autosave-every",
        type=int,
        default=25,
        help="Write partial movies_final.json every N completed detail fetches (default 25)",
    )
    args = p.parse_args(argv)

    driver = build_fast_chrome(headless=True)
    try:
        base_records = extract_top250_from_dom(driver)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    # If Selenium couldn't parse anything, fall back to requests pagination (keeps script robust)
    if len(base_records) == 0:
        search_url = "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating"
        session = requests.Session()
        session.headers.update({"User-Agent": UA})
        full: List[str] = []
        seen = set()
        start = 1
        while len(full) < args.limit and start <= 251:
            resp = session.get(f"{search_url}&start={start}", timeout=12)
            if resp.status_code != 200:
                break
            ids = re.findall(r"/title/tt\d+", resp.text)
            for item in ids:
                url = "https://www.imdb.com" + item.split("?")[0] + "/"
                if url not in seen:
                    seen.add(url)
                    full.append(url)
                if len(full) >= args.limit:
                    break
            start += 50

        base_records = [{"url": u, "title": None, "year": None, "rating": None, "metascore": None,
                         "duration": None, "duration_min": None, "votes": None, "genres": []} for u in full]

    # Trim to requested limit (Top 250 page should already be 250)
    base_records = base_records[: args.limit]

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    # Parallelize detail fetches (this is the main speed-up). One bad/slow URL won't block everything.
    url_to_record = {r["url"]: r for r in base_records if r.get("url")}
    urls = list(url_to_record.keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        futures = [ex.submit(fetch_details_with_retry, url, session) for url in urls]
        done = 0
        total = len(futures)
        for f in concurrent.futures.as_completed(futures):
            url, (metascore, votes, dur_min, dur_text, genres, year) = f.result()
            r = url_to_record.get(url)
            if not r:
                continue
            if r.get("metascore") is None:
                r["metascore"] = metascore
            if r.get("votes") is None:
                r["votes"] = votes
            if r.get("duration_min") is None and dur_min is not None:
                r["duration_min"] = dur_min
                r["duration"] = dur_text
            if genres and not r.get("genres"):
                r["genres"] = genres
            # attach discovered year if we didn't have it from the DOM
            if (r.get("year") is None or r.get("year") is False) and year is not None:
                try:
                    r["year"] = int(year)
                except Exception:
                    r["year"] = year

            done += 1
            if done == 1 or done % 10 == 0 or done == total:
                print(f"details: {done}/{total} completed", flush=True)
            if args.autosave_every and (done % args.autosave_every == 0 or done == total):
                # Partial autosave so the dashboard can show something even during long runs.
                df_tmp = pd.DataFrame(base_records)
                df_tmp["rating"] = pd.to_numeric(df_tmp["rating"], errors="coerce")
                df_tmp["metascore"] = pd.to_numeric(df_tmp["metascore"], errors="coerce")
                df_tmp["votes"] = pd.to_numeric(df_tmp["votes"], errors="coerce")
                df_tmp["year"] = pd.to_numeric(df_tmp["year"], errors="coerce")
                df_tmp["duration_min"] = pd.to_numeric(df_tmp["duration_min"], errors="coerce")

                median_meta_tmp = df_tmp["metascore"].median(skipna=True)
                if pd.notna(median_meta_tmp):
                    df_tmp["metascore"] = df_tmp["metascore"].fillna(median_meta_tmp)

                rating_stats_tmp, rating_out_tmp = iqr_outliers(df_tmp["rating"])
                meta_stats_tmp, meta_out_tmp = iqr_outliers(df_tmp["metascore"])

                df_tmp["anomaly_rating_iqr"] = rating_out_tmp.astype(bool)
                df_tmp["anomaly_metascore_iqr"] = meta_out_tmp.astype(bool)
                df_tmp["is_anomaly"] = (df_tmp["anomaly_rating_iqr"] | df_tmp["anomaly_metascore_iqr"]).astype(bool)

                payload_tmp = {
                    "summary": {
                        "n_records": int(len(df_tmp)),
                        "rating_stats": rating_stats_tmp,
                        "metascore_stats": meta_stats_tmp,
                        "metascore_median_used_for_impute": None if pd.isna(median_meta_tmp) else float(median_meta_tmp),
                        "anomalies_counts": {
                            "rating_iqr": int(df_tmp["anomaly_rating_iqr"].sum()),
                            "metascore_iqr": int(df_tmp["anomaly_metascore_iqr"].sum()),
                            "any": int(df_tmp["is_anomaly"].sum()),
                        },
                        "progress": {"details_completed": done, "details_total": total},
                    },
                    "records": df_tmp.where(pd.notna(df_tmp), None).to_dict(orient="records"),
                }
                write_react_json(payload_tmp)
                print(f"autosaved partial JSON ({done}/{total})", flush=True)

    # If base title missing (fallback path), fetch title only for missing ones (few items, keep it short)
    missing_titles = [r for r in base_records if r.get("url") and not r.get("title")]
    for r in missing_titles[:20]:
        try:
            resp = session.get(r["url"], timeout=8)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                h1 = soup.find("h1")
                if h1:
                    r["title"] = h1.get_text(strip=True)
        except Exception:
            continue

    df = pd.DataFrame(base_records)

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["metascore"] = pd.to_numeric(df["metascore"], errors="coerce")
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["duration_min"] = pd.to_numeric(df["duration_min"], errors="coerce")

    median_meta = df["metascore"].median(skipna=True)
    if pd.notna(median_meta):
        df["metascore"] = df["metascore"].fillna(median_meta)

    rating_stats, rating_out = iqr_outliers(df["rating"])
    meta_stats, meta_out = iqr_outliers(df["metascore"])

    df["anomaly_rating_iqr"] = rating_out.astype(bool)
    df["anomaly_metascore_iqr"] = meta_out.astype(bool)
    df["is_anomaly"] = (df["anomaly_rating_iqr"] | df["anomaly_metascore_iqr"]).astype(bool)

    records = df.where(pd.notna(df), None).to_dict(orient="records")

    payload = {
        "summary": {
            "n_records": int(len(df)),
            "rating_stats": rating_stats,
            "metascore_stats": meta_stats,
            "metascore_median_used_for_impute": None if pd.isna(median_meta) else float(median_meta),
            "anomalies_counts": {
                "rating_iqr": int(df["anomaly_rating_iqr"].sum()),
                "metascore_iqr": int(df["anomaly_metascore_iqr"].sum()),
                "any": int(df["is_anomaly"].sum()),
            },
        },
        "records": records,
    }

    write_react_json(payload)

    print(f"Saved {len(df)} records -> imdb-dashboard/public/movies_final.json and imdb-dashboard/src/movies_final.json")


if __name__ == "__main__":
    main()
