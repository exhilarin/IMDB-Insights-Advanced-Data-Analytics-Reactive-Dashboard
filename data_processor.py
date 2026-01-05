"""data_processor.py

Scrape IMDB Top 250 Movies and Top 250 TV Shows, normalize and produce a
single `movies_final.json` used by the React dashboard.

Usage:
  venv/bin/python data_processor.py --limit 250 --threads 16 --autosave-every 25

Design notes:
- Collect lists from the two chart pages (/chart/top/ and /chart/toptv/).
- Label each record with `type`: 'movie' or 'tv_show'.
- Fetch details (metascore, votes, duration, genres) via requests in parallel.
- Clean numeric fields, impute metascore by group median, detect IQR outliers
  separately for movies and tv_shows, and write a single JSON file.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import time
from typing import Dict, List, Optional, Tuple

import concurrent.futures
import requests
import pandas as pd
import warnings
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

TOP_250_MOVIES = "https://www.imdb.com/chart/top/"
TOP_250_TV = "https://www.imdb.com/chart/toptv/"


def parse_duration_to_minutes(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    text = str(s).strip().lower()
    # ISO 8601 durations commonly appear in JSON-LD as 'PT2H22M' or 'PT45M'
    m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', text, re.I)
    if m_iso:
        h = int(m_iso.group(1)) if m_iso.group(1) else 0
        mm = int(m_iso.group(2)) if m_iso.group(2) else 0
        # seconds present in group(3) are ignored for minute precision
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


def collect_top_list(url: str, limit: int) -> List[Dict]:
    """Collect top list URLs and initial metadata from a chart page.

    This function is intentionally tolerant to markup changes: it finds
    /title/ttNNN links and deduplicates while attempting to capture title
    text nearby.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    resp = session.get(url, timeout=12)
    if resp.status_code != 200:
        return []
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # Find anchors that link to titles
    anchors = soup.find_all("a", href=re.compile(r"/title/tt\d+"))
    seen = set()
    records: List[Dict] = []
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        m = re.search(r"(/title/tt\d+)", href)
        if not m:
            continue
        key = m.group(1)
        url_full = "https://www.imdb.com" + key + "/"
        if url_full in seen:
            continue
        seen.add(url_full)

        title = a.get_text(strip=True)
        # try to find year near the anchor
        year = None
        parent = a.parent
        if parent:
            txt = parent.get_text(" ", strip=True)
            m2 = re.search(r"(\d{4})", txt)
            if m2:
                year = int(m2.group(1))

        records.append(
            {
                "url": url_full,
                "title": title or None,
                "year": year,
                "rating": None,
                "metascore": None,
                "duration": None,
                "duration_min": None,
                "votes": None,
                "genres": [],
            }
        )
        if len(records) >= limit:
            break

    return records


def collect_top_list_with_fallback(url: str, limit: int, kind: str = "movie") -> List[Dict]:
    """Call collect_top_list(), and if it returns fewer than `limit`, fall back
    to a paginated search query that extracts /title/tt... links until we have
    the requested number. `kind` can be 'movie' or 'tv' to choose a sensible
    search URL for TV shows.
    """
    # First attempt: a robust Selenium DOM extraction (works when IMDB uses dynamic markup)
    records: List[Dict] = []
    try:
        # build fast chrome that blocks images/styles for speed
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--user-agent={UA}")
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
            "profile.managed_default_content_settings.cookies": 1,
            "profile.managed_default_content_settings.javascript": 1,
        }
        options.add_experimental_option("prefs", prefs)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        try:
            # wait up to 10s for list to appear
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            # try to wait for common list containers (multiple selectors to be robust)
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.chart")))
            except Exception:
                pass

            # Try multiple DOM selectors to be robust across IMDB layout changes
            items = driver.find_elements(By.CSS_SELECTOR, "ul.ipc-metadata-list > li")
            seen = set()
            if items:
                for li in items:
                    try:
                        a = li.find_element(By.CSS_SELECTOR, "a.ipc-title-link-wrapper")
                        title = a.text.strip()
                        href = a.get_attribute("href")
                        url_full = href.split('?')[0] if href else None
                    except Exception:
                        continue

                    if not url_full:
                        continue
                    if url_full in seen:
                        continue
                    seen.add(url_full)

                    year = None
                    rating = None
                    try:
                        meta_spans = li.find_elements(By.CSS_SELECTOR, "span.cli-title-metadata-item")
                        if len(meta_spans) >= 1:
                            y = meta_spans[0].text.strip()
                            if y.isdigit():
                                year = int(y)
                        m = re.search(r"\b([0-9]\.[0-9])\b", li.text)
                        if m:
                            try:
                                rating = float(m.group(1))
                            except Exception:
                                rating = None
                    except Exception:
                        pass

                    records.append(
                        {
                            "url": url_full,
                            "title": title or None,
                            "year": year,
                            "rating": rating,
                            "metascore": None,
                            "duration": None,
                            "duration_min": None,
                            "votes": None,
                            "genres": [],
                        }
                    )
                    if len(records) >= limit:
                        break
            else:
                # fallback to older chart table selectors
                rows = driver.find_elements(By.CSS_SELECTOR, "table.chart tbody tr")
                for tr in rows:
                    try:
                        a = tr.find_element(By.CSS_SELECTOR, "td.titleColumn a")
                        href = a.get_attribute("href")
                        url_full = href.split('?')[0] + "/" if href else None
                        title = a.text.strip() or None
                    except Exception:
                        continue
                    if not url_full or url_full in seen:
                        continue
                    seen.add(url_full)
                    year = None
                    try:
                        y = tr.find_element(By.CSS_SELECTOR, "span.secondaryInfo").text
                        m2 = re.search(r"(\d{4})", y)
                        if m2:
                            year = int(m2.group(1))
                    except Exception:
                        pass
                    records.append(
                        {
                            "url": url_full,
                            "title": title,
                            "year": year,
                            "rating": None,
                            "metascore": None,
                            "duration": None,
                            "duration_min": None,
                            "votes": None,
                            "genres": [],
                        }
                    )
                    if len(records) >= limit:
                        break
        finally:
            try:
                driver.quit()
            except Exception:
                pass
    except Exception:
        records = []

    # If Selenium extraction failed or returned too few, fall back to requests-based collector
    if len(records) >= limit:
        return records[:limit]

    records = collect_top_list(url, limit)
    if len(records) >= limit:
        return records[:limit]

    # Fallback: use search pagination to collect title links
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    if kind == "tv":
        # search for TV series sorted by user rating (broad fallback)
        search_url = "https://www.imdb.com/search/title/?title_type=tv_series&sort=user_rating"
    else:
        # movies top 250 fallback
        search_url = "https://www.imdb.com/search/title/?groups=top_250&sort=user_rating"

    full: List[str] = []
    seen = set()
    start = 1
    while len(full) < limit and start <= 1001:
        resp = session.get(f"{search_url}&start={start}", timeout=12)
        if resp.status_code != 200:
            break
        ids = re.findall(r"/title/tt\d+", resp.text)
        for item in ids:
            url_full = "https://www.imdb.com" + item.split("?")[0] + "/"
            if url_full not in seen:
                seen.add(url_full)
                full.append(url_full)
            if len(full) >= limit:
                break
        start += 50

    # Convert to records with minimal fields if we didn't get full details yet
    fallback_records = [
        {
            "url": u,
            "title": None,
            "year": None,
            "rating": None,
            "metascore": None,
            "duration": None,
            "duration_min": None,
            "votes": None,
            "genres": [],
        }
        for u in full[:limit]
    ]

    # If we had some records from the DOM parse, merge them preserving any
    # extracted details (title/year) and append remaining fallback records.
    urls_have = {r.get("url"): r for r in records}
    merged: List[Dict] = []
    for r in fallback_records:
        if r["url"] in urls_have:
            merged.append(urls_have[r["url"]])
        else:
            merged.append(r)

    return merged


def fetch_details_requests(url: str, session: requests.Session) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[str], List[str], Optional[float], Optional[int]]:
    """Return metascore, votes, duration_min, duration_text, genres, rating, year"""
    try:
        resp = session.get(url, timeout=8)
        if resp.status_code != 200:
            return None, None, None, None, [], None, None
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

        dur_text = None
        dur_min = None
        m = re.search(r"(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)", html, re.I)
        if m:
            dur_text = m.group(1)
            dur_min = parse_duration_to_minutes(dur_text)
        else:
            m = re.search(r"(\d{2,3})\s*min", html, re.I)
            if m:
                dur_text = m.group(0)
                dur_min = parse_duration_to_minutes(dur_text)

        # rating (float)
        rating = None
        m = re.search(r"([0-9]\.[0-9])\b", html)
        if m:
            try:
                rating = float(m.group(1))
            except Exception:
                rating = None

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
                    # Prefer structured year information in JSON-LD
                    if year is None:
                        dp = obj.get('datePublished') or obj.get('startDate') or obj.get('dateCreated')
                        if dp:
                            try:
                                year = int(str(dp)[:4])
                            except Exception:
                                pass
                    # Prefer duration from Movie / TVEpisode / TVSeries objects (avoid VideoObject trailers)
                    if (dur_min is None) and ('@type' in obj or 'type' in obj):
                        typ = obj.get('@type') or obj.get('type')
                        if isinstance(typ, str):
                            t = typ.lower()
                            if any(x in t for x in ('movie', 'tvepisode', 'tvseries')):
                                d = obj.get('duration') or obj.get('timeRequired')
                                if d:
                                    try:
                                        # store raw JSON-LD duration (like 'PT2H22M') and parse
                                        dur_text = d
                                        dur_min = parse_duration_to_minutes(dur_text)
                                    except Exception:
                                        pass
            except Exception:
                continue
        genres = list(dict.fromkeys([g for g in genres if g]))
        # Fallback: simple regex for a 4-digit year anywhere on the page
        if year is None:
            m = re.search(r"\b(19|20)\d{2}\b", html)
            if m:
                try:
                    year = int(m.group(0))
                except Exception:
                    year = None

        # Sanity: if parsed duration is extremely large (e.g. total series runtime), ignore it
        if dur_min is not None and dur_min > 10 * 60:
            dur_min = None
            dur_text = None

        return metascore, votes, dur_min, dur_text, genres, rating, year
    except Exception:
        return None, None, None, None, [], None, None


def fetch_details_with_retry(url: str, session: requests.Session, retries: int = 2, base_backoff: float = 0.4):
    last = (None, None, None, None, [], None, None)
    for attempt in range(retries + 1):
        last = fetch_details_requests(url, session)
        if any(x is not None for x in last[:4]) or (last[4] and len(last[4]) > 0) or last[5] is not None:
            return url, last
        if attempt < retries:
            time.sleep(base_backoff * (2 ** attempt) + random.random() * 0.25)
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


# Suppress noisy numpy runtime warnings about mean of empty slice during
# intermediate median/mean operations. This avoids confusing console spam
# while preserving genuine exceptions.
warnings.filterwarnings("ignore", message="Mean of empty slice")


def write_react_json(payload: Dict, out_src: Optional[str] = None, out_public: Optional[str] = None):
    """Write the given payload into the frontend `src` and `public` folders.

    By default this writes to `frontend/src/movies_final.json` and
    `frontend/public/movies_final.json`. Callers (such as autosave during
    scraping) can pass alternative relative paths (relative to the repo root)
    or absolute paths to avoid clobbering the primary file while the
    pipeline is still running.
    """
    repo_root = os.path.dirname(os.path.abspath(__file__))
    # Default paths inside the repo
    default_src = os.path.join("frontend", "src", "movies_final.json")
    default_public = os.path.join("frontend", "public", "movies_final.json")

    # Resolve provided paths (allow absolute paths as well)
    def _resolve(p, default):
        if p is None:
            return os.path.join(repo_root, default)
        if os.path.isabs(p):
            return p
        return os.path.join(repo_root, p)

    out_src_path = _resolve(out_src, default_src)
    out_public_path = _resolve(out_public, default_public)

    os.makedirs(os.path.dirname(out_src_path), exist_ok=True)
    os.makedirs(os.path.dirname(out_public_path), exist_ok=True)

    def _sanitize(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: _sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v) for v in obj]
        return obj

    payload = _sanitize(payload)
    with open(out_src_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    with open(out_public_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)


def main(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(description="IMDB combined Top-250 movies + tv shows scraper")
    p.add_argument("--limit", type=int, default=250, help="How many items per type to fetch (default 250)")
    p.add_argument("--threads", type=int, default=16, help="Parallel workers for requests")
    p.add_argument("--autosave-every", type=int, default=25, help="Autosave every N details")
    args = p.parse_args(argv)

    # Collect lists (use Selenium-first robust collector with pagination fallback)
    print("Collecting top lists...", flush=True)
    movies = collect_top_list_with_fallback(TOP_250_MOVIES, args.limit, kind="movie")
    tv = collect_top_list_with_fallback(TOP_250_TV, args.limit, kind="tv")

    # Attach type
    for r in movies:
        r["type"] = "movie"
    for r in tv:
        # use concise tag 'tv' for TV shows
        r["type"] = "tv"

    combined = movies + tv
    combined = combined[: args.limit * 2]

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    url_to_record = {r["url"]: r for r in combined if r.get("url")}
    urls = list(url_to_record.keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
        futures = [ex.submit(fetch_details_with_retry, url, session) for url in urls]
        done = 0
        total = len(futures)
        for f in concurrent.futures.as_completed(futures):
            url, (metascore, votes, dur_min, dur_text, genres, rating, year) = f.result()
            r = url_to_record.get(url)
            if not r:
                continue
            if r.get("metascore") is None and metascore is not None:
                r["metascore"] = metascore
            if r.get("votes") is None and votes is not None:
                r["votes"] = votes
            if r.get("duration_min") is None and dur_min is not None:
                r["duration_min"] = dur_min
                r["duration"] = dur_text
            if genres and not r.get("genres"):
                r["genres"] = genres
            if r.get("rating") is None and rating is not None:
                r["rating"] = rating
            # Fill year if we found it while fetching details
            if (r.get("year") is None or r.get("year") is False) and year is not None:
                try:
                    r["year"] = int(year)
                except Exception:
                    r["year"] = year

            done += 1
            if done == 1 or done % 10 == 0 or done == total:
                print(f"details: {done}/{total} completed", flush=True)
            if args.autosave_every and (done % args.autosave_every == 0 or done == total):
                df_tmp = pd.DataFrame(list(url_to_record.values()))
                # numeric cleanup
                df_tmp["rating"] = pd.to_numeric(df_tmp["rating"], errors="coerce")
                df_tmp["metascore"] = pd.to_numeric(df_tmp["metascore"], errors="coerce")
                df_tmp["votes"] = pd.to_numeric(df_tmp["votes"], errors="coerce")
                df_tmp["year"] = pd.to_numeric(df_tmp.get("year"), errors="coerce")
                df_tmp["duration_min"] = pd.to_numeric(df_tmp["duration_min"], errors="coerce")

                # impute metascore per type
                df_tmp["metascore"] = df_tmp.groupby("type")["metascore"].transform(lambda s: s.fillna(s.median()))

                # compute anomalies per type
                df_tmp["anomaly_rating_iqr"] = False
                df_tmp["anomaly_metascore_iqr"] = False
                df_tmp["anomaly_duration_outlier"] = False
                for t, g in df_tmp.groupby("type"):
                    stat_r, out_r = iqr_outliers(g["rating"])
                    stat_m, out_m = iqr_outliers(g["metascore"])
                    stat_d, out_d = iqr_outliers(g["duration_min"])
                    df_tmp.loc[g.index, "anomaly_rating_iqr"] = out_r
                    df_tmp.loc[g.index, "anomaly_metascore_iqr"] = out_m
                    df_tmp.loc[g.index, "anomaly_duration_outlier"] = out_d

                # simple heuristic for rating-votes inconsistency
                # Only flag rating-votes inconsistency when votes are present.
                # Previously missing votes were treated as 0 (df.fillna(0)) which
                # caused many TV shows with missing vote counts to be marked as
                # anomalies. Require votes.notna() so missing votes do not trigger
                # the heuristic.
                df_tmp["anomaly_rating_votes_inconsistent"] = (
                    (df_tmp["rating"] >= 8.5) & df_tmp["votes"].notna() & (df_tmp["votes"] < 1000)
                )

                # high rating + low metascore heuristic
                df_tmp["anomaly_rating_high_meta_low"] = False
                med_meta_by_type = df_tmp.groupby("type")["metascore"].median()
                for t, med in med_meta_by_type.items():
                    mask = (df_tmp["type"] == t) & (df_tmp["rating"] >= 8.5) & (df_tmp["metascore"] < (med - 10))
                    df_tmp.loc[mask, "anomaly_rating_high_meta_low"] = True

                df_tmp["is_anomaly"] = (
                    df_tmp["anomaly_rating_iqr"]
                    | df_tmp["anomaly_metascore_iqr"]
                    | df_tmp["anomaly_duration_outlier"]
                    | df_tmp["anomaly_rating_votes_inconsistent"]
                    | df_tmp["anomaly_rating_high_meta_low"]
                )

                payload_tmp = {
                    "summary": {"n_records": int(len(df_tmp))},
                    "records": df_tmp.where(pd.notna(df_tmp), None).to_dict(orient="records"),
                }
                # Write autosave to a separate file so the frontend dev server
                # doesn't pick up partial results and constantly reload.
                write_react_json(
                    payload_tmp,
                    out_src="frontend/src/movies_final_autosave.json",
                    out_public="frontend/public/movies_final_autosave.json",
                )
                print(f"autosaved partial JSON ({done}/{total})", flush=True)

    # final cleanup and analytics
    df = pd.DataFrame(list(url_to_record.values()))
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["metascore"] = pd.to_numeric(df["metascore"], errors="coerce")
    df["votes"] = pd.to_numeric(df["votes"], errors="coerce")
    df["year"] = pd.to_numeric(df.get("year"), errors="coerce")
    df["duration_min"] = pd.to_numeric(df["duration_min"], errors="coerce")

    # impute metascore per type
    df["metascore"] = df.groupby("type")["metascore"].transform(lambda s: s.fillna(s.median()))

    # IQR per type
    df["anomaly_rating_iqr"] = False
    df["anomaly_metascore_iqr"] = False
    df["anomaly_duration_outlier"] = False
    for t, g in df.groupby("type"):
        stat_r, out_r = iqr_outliers(g["rating"])
        stat_m, out_m = iqr_outliers(g["metascore"])
        stat_d, out_d = iqr_outliers(g["duration_min"])
        df.loc[g.index, "anomaly_rating_iqr"] = out_r
        df.loc[g.index, "anomaly_metascore_iqr"] = out_m
        df.loc[g.index, "anomaly_duration_outlier"] = out_d

    # Only consider the rating-votes inconsistency when we actually have a
    # votes value. Treat missing votes as unknown (do not flag).
    df["anomaly_rating_votes_inconsistent"] = (
        (df["rating"] >= 8.5) & df["votes"].notna() & (df["votes"] < 1000)
    )

    df["anomaly_rating_high_meta_low"] = False
    med_meta_by_type = df.groupby("type")["metascore"].median()
    for t, med in med_meta_by_type.items():
        mask = (df["type"] == t) & (df["rating"] >= 8.5) & (df["metascore"] < (med - 10))
        df.loc[mask, "anomaly_rating_high_meta_low"] = True

    df["is_anomaly"] = (
        df["anomaly_rating_iqr"]
        | df["anomaly_metascore_iqr"]
        | df["anomaly_duration_outlier"]
        | df["anomaly_rating_votes_inconsistent"]
        | df["anomaly_rating_high_meta_low"]
    )

    records = df.where(pd.notna(df), None).to_dict(orient="records")

    # Summary counts
    anomalies_counts = {
        "rating_iqr": int(df["anomaly_rating_iqr"].sum()),
        "metascore_iqr": int(df["anomaly_metascore_iqr"].sum()),
        "any": int(df["is_anomaly"].sum()),
    }

    payload = {
        "summary": {
            "n_records": int(len(df)),
            "anomalies_counts": anomalies_counts,
        },
        "records": records,
    }

    write_react_json(payload)
    print(f"Saved {len(df)} records -> frontend/public/movies_final.json and frontend/src/movies_final.json")


if __name__ == "__main__":
    main()
