#!/usr/bin/env python3
import re
import json
import urllib.request

URLS = [
    "https://www.imdb.com/title/tt0111161/",  # The Shawshank Redemption (movie)
    "https://www.imdb.com/title/tt0944947/",  # Game of Thrones (tv)
    "https://www.imdb.com/title/tt0903747/",  # Breaking Bad (tv)
    "https://www.imdb.com/title/tt4574334/",  # Stranger Things (tv)
]

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def extract_and_parse(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(url)
        print("  fetch error:", e)
        return

    # Prefer JSON-LD duration from Movie / TV objects (avoid VideoObject trailers)
    dur = None
    for s in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.I | re.S):
        try:
            j = json.loads(s)
        except Exception:
            continue
        objs = j if isinstance(j, list) else [j]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            typ = obj.get('@type') or obj.get('type')
            if isinstance(typ, str) and any(x in typ.lower() for x in ('movie', 'tvepisode', 'tvseries')):
                d = obj.get('duration') or obj.get('timeRequired')
                if d:
                    dur = d
                    break
        if dur:
            break
    # fallback: human-readable runtime in page text
    if dur is None:
        m = re.search(r'(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)', html, re.I)
        if m:
            dur = m.group(1)
        else:
            m2 = re.search(r'(\d{2,3})\s*min', html, re.I)
            if m2:
                dur = m2.group(0)

    parsed = None
    if dur:
        s2 = dur.strip().lower()
        m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', s2, re.I)
        if m_iso:
            h = int(m_iso.group(1) or 0)
            mm = int(m_iso.group(2) or 0)
            parsed = h * 60 + mm
        else:
            m_h = re.search(r'(\d+)\s*h', s2)
            if m_h:
                h = int(m_h.group(1))
                m_m = re.search(r'(\d+)\s*m', s2)
                mm = int(m_m.group(1)) if m_m else 0
                parsed = h * 60 + mm
            else:
                m_n = re.search(r'(\d{2,3})', s2)
                if m_n:
                    parsed = int(m_n.group(1))

    # sanity: durations above 10 hours likely represent total series runtime / other aggregate values
    if parsed is not None and parsed > 10 * 60:
        # ignore and fallback to textual runtime on page
        parsed = None
        dur = None
        m = re.search(r'(\d+\s*h(?:ours?)?(?:\s*\d+\s*m(?:in)?)?)', html, re.I)
        if m:
            dur = m.group(1)
        else:
            m2 = re.search(r'(\d{2,3})\s*min', html, re.I)
            if m2:
                dur = m2.group(0)
        if dur:
            s2 = dur.strip().lower()
            m_iso = re.search(r'pt\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?', s2, re.I)
            if m_iso:
                h = int(m_iso.group(1) or 0)
                mm = int(m_iso.group(2) or 0)
                parsed = h * 60 + mm
            else:
                m_h = re.search(r'(\d+)\s*h', s2)
                if m_h:
                    h = int(m_h.group(1))
                    m_m = re.search(r'(\d+)\s*m', s2)
                    mm = int(m_m.group(1)) if m_m else 0
                    parsed = h * 60 + mm
                else:
                    m_n = re.search(r'(\d{2,3})', s2)
                    if m_n:
                        parsed = int(m_n.group(1))

    print(url)
    print("  raw_duration:", dur)
    print("  parsed_min:", parsed)


if __name__ == '__main__':
    for u in URLS:
        extract_and_parse(u)
