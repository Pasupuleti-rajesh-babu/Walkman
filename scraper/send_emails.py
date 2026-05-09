#!/usr/bin/env python3
"""
Auto-email C2C recruiters extracted from data/jobs.json.

Usage:
  python send_emails.py              # send up to 50 emails
  python send_emails.py --dry-run    # preview without sending
  python send_emails.py --limit 10   # send at most 10 emails

Config (.env in scraper/ or repo root):
  SENDER_EMAIL     - your Gmail address           (required)
  SENDER_PASSWORD  - Gmail App Password           (required)
  SENDER_NAME      - your display name            (default: Rajesh Babu Pasupuleti)
  RESUME_PATH      - absolute path to resume PDF  (default: auto-detect *.pdf)
  SMTP_HOST        - SMTP host                    (default: smtp.gmail.com)
  SMTP_PORT        - SMTP port                    (default: 587)

Sent-email deduplication log: data/sent_emails.json
Each recruiter email is only contacted once across runs.
"""

import argparse
import json
import os
import re
import smtplib
import time
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

JOBS_FILE = Path(__file__).parent.parent / "data" / "jobs.json"
SENT_LOG = Path(__file__).parent.parent / "data" / "sent_emails.json"

SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "").strip()
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD", "").strip()
SENDER_NAME = os.environ.get("SENDER_NAME", "Rajesh Babu Pasupuleti").strip()
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))

_EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[a-z]{2,}", re.IGNORECASE)

SUBJECT = "Data Engineer (C2C/Contract) — Resume Attached | {title}"

BODY = """\
Hi,

I came across the {title} position listed on {source} and wanted to reach out.

I’m a Data Engineer with 7+ years of experience building scalable pipelines and data platforms:
  • Apache Spark, Databricks, Delta Lake, Apache Kafka
  • AWS (S3, Glue, EMR, Redshift), Azure (Data Factory, Synapse, ADLS), GCP (BigQuery, Dataflow)
  • Python, SQL, dbt, Apache Airflow, Terraform
  • Snowflake, Lakehouse architecture, real-time streaming

I’m actively looking for C2C / Corp-to-Corp contract engagements. My resume is attached — please
let me know if there’s a fit or if you’d like to schedule a quick call.

Best regards,
{sender_name}
Email: {sender_email}
"""


def _find_resume() -> Path | None:
    resume_env = os.environ.get("RESUME_PATH", "")
    if resume_env and Path(resume_env).exists():
        return Path(resume_env)
    for directory in [Path(__file__).parent, Path(__file__).parent.parent]:
        pdfs = sorted(directory.glob("*.pdf"))
        if pdfs:
            return pdfs[0]
    return None


def _load_sent_log() -> set[str]:
    if SENT_LOG.exists():
        try:
            return set(json.loads(SENT_LOG.read_text()))
        except Exception:
            pass
    return set()


def _save_sent_log(sent: set[str]) -> None:
    SENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    SENT_LOG.write_text(json.dumps(sorted(sent), indent=2))


def _build_message(job: dict, resume_path: Path | None) -> MIMEMultipart:
    msg = MIMEMultipart("mixed")
    msg["From"] = f"{SENDER_NAME} <{SENDER_EMAIL}>"
    msg["To"] = job["email"]
    msg["Subject"] = SUBJECT.format(title=job.get("title", "Data Engineer"))
    msg["Reply-To"] = SENDER_EMAIL

    body = BODY.format(
        title=job.get("title", "Data Engineer"),
        source=job.get("source", "the job board"),
        sender_email=SENDER_EMAIL,
        sender_name=SENDER_NAME,
    )
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if resume_path:
        with open(resume_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=resume_path.name)
        part["Content-Disposition"] = f'attachment; filename="{resume_path.name}"'
        msg.attach(part)

    return msg


def send_emails(dry_run: bool = False, limit: int = 50) -> None:
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print(
            "ERROR: Set SENDER_EMAIL and SENDER_PASSWORD environment variables\n"
            "  - SENDER_EMAIL    : your Gmail address\n"
            "  - SENDER_PASSWORD : Gmail App Password (Settings › Security › 2-step › App passwords)"
        )
        return

    if not JOBS_FILE.exists():
        print(f"ERROR: {JOBS_FILE} not found. Run scrape.py first.")
        return

    jobs = json.loads(JOBS_FILE.read_text())
    candidates = [
        j for j in jobs
        if j.get("email") and _EMAIL_RE.match(j["email"])
    ]

    sent_log = _load_sent_log()
    to_send = [j for j in candidates if j["email"] not in sent_log][:limit]

    resume_path = _find_resume()

    print(f"Jobs with recruiter email : {len(candidates)}")
    print(f"Already emailed           : {len(sent_log)}")
    print(f"To send this run          : {len(to_send)}")
    print(f"Resume                    : {resume_path or 'NOT FOUND (sending without attachment)'}")
    print()

    if not to_send:
        print("Nothing new to send.")
        return

    if dry_run:
        print("[DRY RUN] Would send to:")
        for j in to_send:
            print(f"  {j['email']:<40}  {j['title']} ({j['source']})")
        return

    sent_count = 0
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)

        for i, job in enumerate(to_send, 1):
            try:
                msg = _build_message(job, resume_path)
                smtp.send_message(msg)
                sent_log.add(job["email"])
                sent_count += 1
                print(f"  [{i:>3}/{len(to_send)}] ✓ {job['email']}  ({job['title']})")
                _save_sent_log(sent_log)
                if i < len(to_send):
                    time.sleep(2)  # stay within Gmail sending-rate limits
            except Exception as exc:
                print(f"  [{i:>3}/{len(to_send)}] ✗ {job['email']}: {exc}")

    print(f"\nDone. Sent {sent_count} email(s) this run ({len(sent_log)} total).")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Email C2C recruiters found in data/jobs.json"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview recipients without actually sending",
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Maximum emails to send per run (default: 50)",
    )
    args = parser.parse_args()
    send_emails(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
