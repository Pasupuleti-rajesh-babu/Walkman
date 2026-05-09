"""
C2C Data Engineer job scraper.
Sources: nvoids.com, c2cjobs.talenthubsolutions.com, corptocorp.org,
         usstaffinginc.org, bighotlist.us
Output:  ../data/jobs.json  (includes email, company, rate, location fields)

HTML structure reference: backup repo c2cjobs_new_source.html / c2c_induvisualjob.html
  Both sites use the WordPress JobBoard WP plugin (jb-* CSS classes).
  Recruiter email is stored as the "company" name on C2C job boards.
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

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[a-z]{2,}", re.IGNORECASE)
_RATE_RE = re.compile(
    r"\$\s*[\d,]+(?:\.\d+)?(?:\s*/\s*(?:hr|hour|hourly|yr|year|month|week|day))?",
    re.IGNORECASE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def extract_email(text: str) -> str:
    m = _EMAIL_RE.search(text or "")
    return m.group(0).lower() if m else ""


def extract_rate(text: str) -> str:
    m = _RATE_RE.search(text or "")
    return m.group(0).strip() if m else ""


def parse_date_string(raw: str) -> str:
    """Best-effort parse of common date strings → YYYY-MM-DD."""
    raw = clean_text(raw)
    # Strip WordPress JobBoard WP prefixes like "Posted April 2, 2026"
    raw = re.sub(r"^(?:posted|updated|expires?|closing on)\s+", "", raw, flags=re.IGNORECASE)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%Y"):
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
        days = int(m.group(1))
        return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return TODAY


def make_job(title: str, link: str, source: str, date: str, **kw) -> dict:
    return {
        "id": job_id(title, link),
        "title": clean_text(title),
        "link": link.strip(),
        "source": source,
        "date": date,
        "company": kw.get("company", ""),
        "email": kw.get("email", ""),
        "rate": kw.get("rate", ""),
        "location": kw.get("location", ""),
        "scraped_at": SCRAPED_AT,
    }


# ── WordPress JobBoard WP plugin helpers ──────────────────────────────────────
#
# Both nvoids.com and c2cjobs.talenthubsolutions.com use this plugin.
# Listing HTML structure (confirmed from backup-repo HTML reference):
#
#   <div class="jb-job-list-row">
#     <div class="jb-job-title">
#       <a class="jb-job-title-link" href="URL">Job Title</a>
#     </div>
#     <div class="jb-row-info">
#       <div class="company"><span title="">recruiter@email.com</span></div>
#       <div class="location"><a href="...">City, State</a></div>
#       <div class="date" title="Posted">Posted April 2, 2026</div>
#     </div>
#   </div>

def _jb_nonce(soup: BeautifulSoup) -> str:
    """Extract per-page nonce from jb_front_data JS variable."""
    for script in soup.find_all("script"):
        m = re.search(r'"nonce"\s*:\s*"([^"]+)"', script.get_text())
        if m:
            return m.group(1)
    return ""


def _jb_ajax_url(soup: BeautifulSoup, base: str) -> str:
    """Extract admin-ajax.php URL from page scripts."""
    for script in soup.find_all("script"):
        m = re.search(r'"ajaxurl"\s*:\s*"([^"]+)"', script.get_text())
        if m:
            return m.group(1).replace("\\/", "/")
    return f"{base}/wp-admin/admin-ajax.php"


def _jb_parse_rows(soup: BeautifulSoup, source: str, base: str) -> list[dict]:
    """Parse job rows from a WordPress JobBoard WP plugin listing page."""
    jobs = []
    for row in soup.select(".jb-job-list-row"):
        title_el = row.select_one(".jb-job-title-link")
        if not title_el:
            continue
        title = clean_text(title_el.get_text())
        href = title_el.get("href", "").strip()
        if not title or not href:
            continue
        if not href.startswith("http"):
            href = base + href

        # Recruiter email stored as company name on C2C boards
        company_span = row.select_one(".company span")
        raw_company = clean_text(company_span.get_text()) if company_span else ""
        email = extract_email(raw_company)
        company = raw_company

        # Location
        loc_el = row.select_one(".location a, .jb-job-location a")
        location = clean_text(loc_el.get_text()) if loc_el else ""

        # Date — prefer machine-readable datetime attribute; fallback to .date text
        time_el = row.select_one("time[datetime]")
        if time_el:
            dt_raw = time_el.get("datetime", "")[:10]
            date = dt_raw if re.match(r"\d{4}-\d{2}-\d{2}", dt_raw) else parse_date_string(time_el.get_text())
        else:
            date_el = row.select_one(".date")
            date = parse_date_string(date_el.get_text() if date_el else "")

        # Rate is usually only in the job description; try listing text first
        rate = extract_rate(row.get_text(" "))

        jobs.append(make_job(title, href, source, date,
                             company=company, email=email,
                             rate=rate, location=location))
    return jobs


def _jb_load_more(ajax_url: str, nonce: str, search: str,
                  source: str, base: str, extra_pages: int = 4) -> list[dict]:
    """POST to admin-ajax.php to fetch paginated job results."""
    jobs = []
    for page in range(2, extra_pages + 2):
        try:
            resp = requests.post(
                ajax_url,
                data={
                    "action": "jb_job_filter",
                    "nonce": nonce,
                    "search_keywords": search,
                    "search_location": "",
                    "page": str(page),
                },
                headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"},
                timeout=20,
            )
            if not resp.ok:
                break
            try:
                payload = resp.json()
                html_fragment = (
                    payload.get("data", {}).get("html")
                    or payload.get("html", "")
                    or ""
                )
            except Exception:
                html_fragment = resp.text
            if not html_fragment:
                break
            frag_soup = BeautifulSoup(html_fragment, "html.parser")
            page_jobs = _jb_parse_rows(frag_soup, source, base)
            if not page_jobs:
                break
            jobs.extend(page_jobs)
            time.sleep(1)
        except Exception as exc:
            print(f"  [warn] AJAX page {page} failed: {exc}")
            break
    return jobs


def scrape_jobboard_wp(base: str, source: str,
                       search: str = "data engineer",
                       ajax_pages: int = 4) -> list[dict]:
    """
    Generic scraper for sites running the WordPress JobBoard WP plugin.
    1. GET the search results page (initial server-rendered jobs + nonce)
    2. POST to admin-ajax.php for additional pages using the extracted nonce
    """
    # URL patterns the plugin may use for keyword search
    candidate_urls = [
        f"{base}/?search_keywords={search.replace(' ', '+')}",
        f"{base}/jobs/?search_keywords={search.replace(' ', '+')}",
        f"{base}/c2c-jobs/?search_keywords={search.replace(' ', '+')}",
        f"{base}/?s={search.replace(' ', '+')}",
        f"{base}/",
    ]

    for url in candidate_urls:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        rows = _jb_parse_rows(soup, source, base)
        if not rows:
            continue

        print(f"    Found {len(rows)} jobs (initial load)")
        jobs = list(rows)

        nonce = _jb_nonce(soup)
        if nonce and ajax_pages > 0:
            ajax_url = _jb_ajax_url(soup, base)
            more = _jb_load_more(ajax_url, nonce, search, source, base, ajax_pages)
            if more:
                print(f"    AJAX loaded {len(more)} additional jobs")
                jobs.extend(more)
        return jobs

    return []


# ── Site scrapers ─────────────────────────────────────────────────────────────

def scrape_nvoids() -> list[dict]:
    """
    nvoids.com — C2C Data Engineer jobs.
    Uses WordPress JobBoard WP plugin (same jb-* CSS selectors as the
    reference HTML in the backup repo from c2cjobs.talenthubsolutions.com).
    Falls back to generic link/email extraction if the plugin is not detected.
    """
    base = "https://nvoids.com"
    jobs = scrape_jobboard_wp(base, "nVoids")
    if not jobs:
        # Fallback: generic scraping with email regex when plugin not detected
        jobs = _scrape_generic(
            base, "nVoids",
            search_paths=[
                "/?s=data+engineer",
                "/jobs/data-engineer",
                "/c2c-jobs/",
                "/",
            ],
        )
    print(f"  nVoids: {len(jobs)} jobs")
    return jobs


def scrape_c2cjobs_talenthub() -> list[dict]:
    """
    c2cjobs.talenthubsolutions.com — C2C jobs (WordPress JobBoard WP plugin).
    HTML structure confirmed from backup-repo reference files.
    """
    base = "https://c2cjobs.talenthubsolutions.com"
    jobs = scrape_jobboard_wp(base, "C2CJobsTH")
    print(f"  C2CJobsTH: {len(jobs)} jobs")
    return jobs


def _scrape_generic(base: str, source: str, search_paths: list[str]) -> list[dict]:
    """Fallback: try multiple URL paths and extract job links with email regex."""
    jobs = []
    for path in search_paths:
        url = base + path
        soup = get_page(url)
        if not soup:
            continue
        for a in soup.find_all("a", href=True):
            title = clean_text(a.get_text())
            if not title or len(title) < 5:
                continue
            if not re.search(
                r"data\s*engineer|spark|snowflake|databricks|airflow|pipeline|glue|kafka",
                title, re.IGNORECASE,
            ):
                continue
            href = a["href"]
            if not href.startswith("http"):
                href = base + href
            parent_text = clean_text(a.parent.get_text(" ") if a.parent else "")
            jobs.append(make_job(
                title, href, source, TODAY,
                email=extract_email(parent_text),
                rate=extract_rate(parent_text),
            ))
        if jobs:
            break
    return jobs


def scrape_corptocorp() -> list[dict]:
    """corptocorp.org — Data Engineer C2C jobs"""
    jobs = []
    base = "https://www.corptocorp.org"
    urls = [
        f"{base}/data-engineer-c2c-jobs",
        f"{base}/c2c-jobs/data-engineer",
        f"{base}/corp-to-corp-jobs?q=data+engineer",
    ]
    for url in urls:
        print(f"  Trying: {url}")
        soup = get_page(url)
        if not soup:
            continue
        for sel in ["div.job-listing", "div.job-post", "li.job-item", "article.job", "div.job", "tr.job-row"]:
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
                text = item.get_text(" ")
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(
                    title, href, "CorpToCorp", date,
                    email=extract_email(text),
                    rate=extract_rate(text),
                ))
            break
        if jobs:
            break
    print(f"  CorpToCorp: {len(jobs)} jobs")
    return jobs


def scrape_usstaffinginc() -> list[dict]:
    """usstaffinginc.org — Data Engineer C2C jobs"""
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
        for sel in ["div.job-listing", "div.job", "li.job", "article", "div.position", "table.jobs tr"]:
            items = soup.select(sel)
            if not items:
                continue
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
                text = item.get_text(" ")
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(
                    title, href, "US Staffing Inc", date,
                    email=extract_email(text),
                    rate=extract_rate(text),
                ))
            break
        if jobs:
            break
    print(f"  US Staffing Inc: {len(jobs)} jobs")
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
        for sel in ["div.job", "div.job-listing", "li.job", "article.job", "div.posting", "div.result"]:
            items = soup.select(sel)
            if not items:
                continue
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
                text = item.get_text(" ")
                date_el = item.find(class_=re.compile(r"date|time|posted", re.I))
                date = parse_date_string(date_el.get_text() if date_el else "")
                jobs.append(make_job(
                    title, href, "BigHotList", date,
                    email=extract_email(text),
                    rate=extract_rate(text),
                ))
            break
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

    existing: list[dict] = []
    if JOBS_FILE.exists():
        try:
            existing = json.loads(JOBS_FILE.read_text())
            print(f"Loaded {len(existing)} existing jobs from {JOBS_FILE}\n")
        except Exception as e:
            print(f"[warn] Could not load existing jobs: {e}\n")

    scrapers = [
        ("nVoids",          scrape_nvoids),
        ("C2CJobsTH",       scrape_c2cjobs_talenthub),
        ("CorpToCorp",      scrape_corptocorp),
        ("US Staffing",     scrape_usstaffinginc),
        ("BigHotList",      scrape_bighotlist),
    ]

    new_jobs: list[dict] = []
    for name, fn in scrapers:
        print(f"\n[{name}]")
        try:
            result = fn()
            new_jobs.extend(result)
        except Exception as exc:
            print(f"  ERROR: {exc}")
        time.sleep(1)

    combined = new_jobs + existing
    combined.sort(key=lambda j: j.get("date", ""), reverse=True)
    deduped = deduplicate(combined)

    with_email = sum(1 for j in deduped if j.get("email"))
    print(f"\n=== Results ===")
    print(f"  New scraped     : {len(new_jobs)}")
    print(f"  After dedup     : {len(deduped)}")
    print(f"  With email      : {with_email}")

    JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)
    JOBS_FILE.write_text(json.dumps(deduped, indent=2, ensure_ascii=False))
    print(f"  Saved to        : {JOBS_FILE}")


if __name__ == "__main__":
    main()
