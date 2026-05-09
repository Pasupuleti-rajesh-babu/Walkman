"""
C2C Data Engineer job scraper.
Primary:  Dice.com search API (contract jobs)
Fallback: corptocorp.org, usstaffinginc.org, nvoids.com, bighotlist.us
Output:   ../data/jobs.json
"""

import json
import time
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

JOBS_FILE = Path(__file__).parent.parent / "data" / "jobs.json"
TODAY     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
SCRAPED_AT = datetime.now(timezone.utc).isoformat()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

KEYWORDS = [
    "data engineer c2c",
    "data engineer corp to corp",
    "data engineer contract",
]


# ── Helpers ────────────────────────────────────────────────────────────────────────────

def job_id(title: str, link: str) -> str:
    return hashlib.md5(f"{title.strip().lower()}|{link.strip()}".encode()).hexdigest()[:12]


def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date(raw: str) -> str:
    raw = clean(raw)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    lower = raw.lower()
    if "today" in lower or "just now" in lower or "hour" in lower:
        return TODAY
    m = re.search(r"(\d+)\s+day", lower)
    if m:
        from datetime import timedelta
        return (datetime.now(timezone.utc) - timedelta(days=int(m.group(1)))).strftime("%Y-%m-%d")
    return TODAY


def make_job(title: str, link: str, source: str, date: str) -> dict:
    return {
        "id":         job_id(title, link),
        "title":      clean(title),
        "link":       link.strip(),
        "source":     source,
        "date":       date,
        "scraped_at": SCRAPED_AT,
    }


def get_page(url: str, retries: int = 3, json_mode: bool = False):
    session = requests.Session()
    session.headers.update(HEADERS)
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=25, allow_redirects=True)
            if resp.status_code == 403:
                print(f"  [403] {url} — blocked")
                return None
            resp.raise_for_status()
            return resp.json() if json_mode else BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            print(f"  [warn] attempt {attempt+1}/{retries}: {exc}")
            time.sleep(2 ** attempt)
    return None


# ── Source 1: Dice.com API (primary — real links) ──────────────────────────────────────

def scrape_dice() -> list[dict]:
    """Dice.com contract Data Engineer jobs via their search API."""
    jobs = []
    seen = set()

    for kw in KEYWORDS:
        url = (
            "https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search"
            f"?q={requests.utils.quote(kw)}"
            "&countryCode=US&pageSize=50&language=en"
            "&facets=employmentType&filters.employmentType=CONTRACTS"
        )
        print(f"  Dice API: {kw}")
        data = get_page(url, json_mode=True)
        if not data:
            continue

        for hit in data.get("data", []):
            title = clean(hit.get("title", ""))
            link  = hit.get("applyUrl") or hit.get("jobDetailUrl") or ""
            if not title or not link or link in seen:
                continue
            seen.add(link)
            raw_date = hit.get("postedDate") or hit.get("modifiedDate") or TODAY
            date = parse_date(raw_date) if raw_date else TODAY
            jobs.append(make_job(title, link, "Dice.com", date))

        print(f"    → {len(jobs)} total so far")
        time.sleep(1)

    print(f"  Dice.com: {len(jobs)} jobs")
    return jobs


# ── Source 2: corptocorp.org ─────────────────────────────────────────────────────────────────────

def scrape_corptocorp() -> list[dict]:
    jobs = []
    base = "https://www.corptocorp.org"
    for url in [f"{base}/data-engineer-c2c-jobs", f"{base}/c2c-jobs/data-engineer"]:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        for sel in ["div.job-listing", "div.job-post", "li.job-item", "article.job", "div.job"]:
            for item in soup.select(sel):
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean(a.get_text())
                if not title:
                    continue
                href = a["href"] if a["href"].startswith("http") else base + a["href"]
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                jobs.append(make_job(title, href, "CorpToCorp", parse_date(date_el.get_text() if date_el else "")))
        if jobs:
            break
    print(f"  CorpToCorp: {len(jobs)} jobs")
    return jobs


# ── Source 3: usstaffinginc.org ────────────────────────────────────────────────────────────────────

def scrape_usstaffinginc() -> list[dict]:
    jobs = []
    base = "https://www.usstaffinginc.org"
    for url in [f"{base}/c2c-jobs/data-engineer", f"{base}/data-engineer-jobs", base]:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        for sel in ["div.job-listing", "div.job", "li.job", "article", "div.position"]:
            for item in soup.select(sel):
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean(a.get_text())
                if not title:
                    continue
                href = a["href"] if a["href"].startswith("http") else base + a["href"]
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                jobs.append(make_job(title, href, "US Staffing Inc", parse_date(date_el.get_text() if date_el else "")))
        if jobs:
            break
    print(f"  US Staffing Inc: {len(jobs)} jobs")
    return jobs


# ── Source 4: nvoids.com ───────────────────────────────────────────────────────────────────────

def scrape_nvoids() -> list[dict]:
    jobs = []
    base = "https://www.nvoids.com"
    for url in [f"{base}/jobs/data-engineer", f"{base}/c2c-jobs?q=data+engineer", base]:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        for sel in ["div.job-listing", "div.job", "li.job-post", "article", "div.listing"]:
            for item in soup.select(sel):
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean(a.get_text())
                if not title:
                    continue
                href = a["href"] if a["href"].startswith("http") else base + a["href"]
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                jobs.append(make_job(title, href, "nVoids", parse_date(date_el.get_text() if date_el else "")))
        if jobs:
            break
    print(f"  nVoids: {len(jobs)} jobs")
    return jobs


# ── Source 5: bighotlist.us ────────────────────────────────────────────────────────────────────────

def scrape_bighotlist() -> list[dict]:
    jobs = []
    base = "https://www.bighotlist.us"
    for url in [f"{base}/jobs/data-engineer", f"{base}/search?q=data+engineer", base]:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        for sel in ["div.job", "div.job-listing", "li.job", "article.job", "div.posting"]:
            for item in soup.select(sel):
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean(a.get_text())
                if not title:
                    continue
                href = a["href"] if a["href"].startswith("http") else base + a["href"]
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                jobs.append(make_job(title, href, "BigHotList", parse_date(date_el.get_text() if date_el else "")))
        if jobs:
            break
    print(f"  BigHotList: {len(jobs)} jobs")
    return jobs


# ── Dedup ────────────────────────────────────────────────────────────────────────────

def deduplicate(jobs: list[dict]) -> list[dict]:
    seen_ids, seen_links, out = set(), set(), []
    for job in jobs:
        jid  = job.get("id", "")
        link = job.get("link", "").rstrip("/")
        if jid in seen_ids or (link and link in seen_links):
            continue
        seen_ids.add(jid)
        if link:
            seen_links.add(link)
        out.append(job)
    return out


# ── Main ────────────────────────────────────────────────────────────────────────────

def main():
    print("=== C2C Data Engineer Job Scraper ===")
    print(f"Date: {TODAY}\n")

    existing: list[dict] = []
    if JOBS_FILE.exists():
        try:
            existing = json.loads(JOBS_FILE.read_text())
            print(f"Loaded {len(existing)} existing jobs\n")
        except Exception as e:
            print(f"[warn] {e}\n")

    scrapers = [
        ("Dice.com",      scrape_dice),
        ("CorpToCorp",    scrape_corptocorp),
        ("US Staffing",   scrape_usstaffinginc),
        ("nVoids",        scrape_nvoids),
        ("BigHotList",    scrape_bighotlist),
    ]

    new_jobs: list[dict] = []
    for name, fn in scrapers:
        print(f"\n[{name}]")
        try:
            new_jobs.extend(fn())
        except Exception as exc:
            print(f"  ERROR: {exc}")
        time.sleep(1)

    combined = new_jobs + existing
    combined.sort(key=lambda j: j.get("date", ""), reverse=True)
    deduped = deduplicate(combined)

    print(f"\n=== Results ===")
    print(f"  New scraped : {len(new_jobs)}")
    print(f"  After dedup : {len(deduped)}")

    JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    JOBS_FILE.write_text(json.dumps(deduped, indent=2, ensure_ascii=False))
    print(f"  Saved → {JOBS_FILE}")


if __name__ == "__main__":
    main()
