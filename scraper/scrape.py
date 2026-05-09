"""
C2C Data Engineer job scraper.
Sources: corptocorp.org, usstaffinginc.org, nvoids.com, bighotlist.us
Output: ../data/jobs.json
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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
SCRAPED_AT = datetime.now(timezone.utc).isoformat()


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            print(f"  [warn] attempt {attempt+1}/{retries} for {url}: {exc}")
            time.sleep(2 ** attempt)
    return None


def job_id(title: str, link: str) -> str:
    raw = f"{title.strip().lower()}|{link.strip()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_date_string(raw: str) -> str:
    """Best-effort parse of common date strings → YYYY-MM-DD."""
    raw = clean_text(raw)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # relative: "2 days ago", "today", "yesterday"
    lower = raw.lower()
    if "today" in lower or "just now" in lower or "hour" in lower:
        return TODAY
    m = re.search(r"(\d+)\s+day", lower)
    if m:
        from datetime import timedelta
        days = int(m.group(1))
        return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return TODAY  # fallback to today


def make_job(title: str, link: str, source: str, date: str) -> dict:
    return {
        "id": job_id(title, link),
        "title": clean_text(title),
        "link": link.strip(),
        "source": source,
        "date": date,
        "scraped_at": SCRAPED_AT,
    }


# ── Scrapers ─────────────────────────────────────────────────────────────────

def scrape_corptocorp() -> list[dict]:
    """corptocorp.org — Data Engineer C2C jobs"""
    jobs = []
    base = "https://www.corptocorp.org"
    pages = [
        f"{base}/data-engineer-c2c-jobs",
        f"{base}/c2c-jobs/data-engineer",
        f"{base}/corp-to-corp-jobs?q=data+engineer",
    ]

    for url in pages:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue

        # Strategy 1: common job-listing class names
        for sel in ["div.job-listing", "div.job-post", "li.job-item",
                    "article.job", "div.job", "tr.job-row"]:
            items = soup.select(sel)
            if items:
                print(f"    [{sel}] found {len(items)} items")
                for item in items:
                    a = item.find("a", href=True)
                    if not a:
                        continue
                    title = clean_text(a.get_text())
                    if not title or len(title) < 4:
                        continue
                    href = a["href"]
                    if not href.startswith("http"):
                        href = base + href
                    date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                    date = parse_date_string(date_el.get_text() if date_el else "")
                    jobs.append(make_job(title, href, "CorpToCorp", date))
                break

        # Strategy 2: all links that look like job titles
        if not jobs:
            for a in soup.find_all("a", href=True, string=True):
                text = clean_text(a.get_text())
                if re.search(r"data\s+engineer|de\s+|snowflake|spark|databricks", text, re.I):
                    href = a["href"]
                    if not href.startswith("http"):
                        href = base + href
                    jobs.append(make_job(text, href, "CorpToCorp", TODAY))

        if jobs:
            break

    print(f"  CorpToCorp: {len(jobs)} jobs")
    return jobs


def scrape_usstaffinginc() -> list[dict]:
    """usstaffinginc.org — Data Engineer jobs"""
    jobs = []
    base = "https://www.usstaffinginc.org"
    urls = [
        f"{base}/c2c-jobs/data-engineer",
        f"{base}/jobs?q=data+engineer&type=c2c",
        f"{base}/data-engineer-jobs",
        base,
    ]

    for url in urls:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue

        for sel in ["div.job-listing", "div.job", "li.job", "article",
                    "div.position", "table.jobs tr"]:
            items = soup.select(sel)
            if not items:
                continue
            print(f"    [{sel}] found {len(items)} items")
            for item in items:
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                if not title or len(title) < 4:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = base + href
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(title, href, "US Staffing Inc", date))

        if jobs:
            break

    print(f"  US Staffing Inc: {len(jobs)} jobs")
    return jobs


def scrape_nvoids() -> list[dict]:
    """nvoids.com — C2C tech jobs"""
    jobs = []
    base = "https://www.nvoids.com"
    urls = [
        f"{base}/jobs/data-engineer",
        f"{base}/c2c-jobs?q=data+engineer",
        f"{base}/?s=data+engineer",
        base,
    ]

    for url in urls:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue

        for sel in ["div.job-listing", "div.job", "li.job-post",
                    "article", "h2.job-title", "div.listing"]:
            items = soup.select(sel)
            if not items:
                continue
            print(f"    [{sel}] found {len(items)} items")
            for item in items:
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                if not title or len(title) < 4:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = base + href
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(title, href, "nVoids", date))

        if jobs:
            break

    print(f"  nVoids: {len(jobs)} jobs")
    return jobs


def scrape_bighotlist() -> list[dict]:
    """bighotlist.us — IT staffing / C2C jobs"""
    jobs = []
    base = "https://www.bighotlist.us"
    urls = [
        f"{base}/jobs/data-engineer",
        f"{base}/c2c-jobs/data-engineer",
        f"{base}/search?q=data+engineer",
        base,
    ]

    for url in urls:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue

        for sel in ["div.job", "div.job-listing", "li.job",
                    "article.job", "div.posting", "div.result"]:
            items = soup.select(sel)
            if not items:
                continue
            print(f"    [{sel}] found {len(items)} items")
            for item in items:
                a = item.find("a", href=True)
                if not a:
                    continue
                title = clean_text(a.get_text())
                if not title or len(title) < 4:
                    continue
                href = a["href"]
                if not href.startswith("http"):
                    href = base + href
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(title, href, "BigHotList", date))

        if jobs:
            break

    print(f"  BigHotList: {len(jobs)} jobs")
    return jobs


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(jobs: list[dict]) -> list[dict]:
    seen_ids: set[str] = set()
    seen_links: set[str] = set()
    unique = []
    for job in jobs:
        key = job.get("id", "")
        link = job.get("link", "").rstrip("/")
        if key in seen_ids or link in seen_links:
            continue
        seen_ids.add(key)
        if link:
            seen_links.add(link)
        unique.append(job)
    return unique


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== C2C Data Engineer Job Scraper ===")
    print(f"Date: {TODAY}\n")

    # Load existing jobs (preserve history)
    existing: list[dict] = []
    if JOBS_FILE.exists():
        try:
            existing = json.loads(JOBS_FILE.read_text())
            print(f"Loaded {len(existing)} existing jobs from {JOBS_FILE}\n")
        except Exception as e:
            print(f"[warn] Could not load existing jobs: {e}\n")

    # Scrape all sources
    scrapers = [
        ("CorpToCorp",    scrape_corptocorp),
        ("US Staffing",   scrape_usstaffinginc),
        ("nVoids",        scrape_nvoids),
        ("BigHotList",    scrape_bighotlist),
    ]

    new_jobs: list[dict] = []
    for name, fn in scrapers:
        print(f"\n[{name}]")
        try:
            result = fn()
            new_jobs.extend(result)
        except Exception as exc:
            print(f"  ERROR: {exc}")
        time.sleep(1)  # polite delay between sources

    # Merge new + existing, newest first
    combined = new_jobs + existing
    combined.sort(key=lambda j: j.get("date", ""), reverse=True)
    deduped = deduplicate(combined)

    print(f"\n=== Results ===")
    print(f"  New scraped : {len(new_jobs)}")
    print(f"  After dedup : {len(deduped)}")

    JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    JOBS_FILE.write_text(json.dumps(deduped, indent=2, ensure_ascii=False))
    print(f"  Saved to    : {JOBS_FILE}")


if __name__ == "__main__":
    main()
