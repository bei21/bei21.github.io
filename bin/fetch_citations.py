#!/usr/bin/env python3
"""
fetch_citations.py
Fetch citation counts from Google Scholar and write to _data/citations.yml.
Run via GitHub Actions every week, or manually: python bin/fetch_citations.py
"""

import os
import re
import sys
import time
import yaml
from datetime import date

try:
    from scholarly import scholarly
except ImportError:
    print("ERROR: scholarly not installed. Run: pip install scholarly")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SCHOLAR_USER_ID = "7IZyaZsAAAAJ"
BIB_FILE        = "_bibliography/papers.bib"
OUTPUT_FILE     = "_data/citations.yml"
RETRIES         = 3
RETRY_DELAY     = 10   # seconds between retries
TODAY           = date.today().isoformat()
# ──────────────────────────────────────────────────────────────────────────────


def extract_scholar_ids_from_bib(bib_path):
    """Return dict of {article_id: entry_key} parsed from papers.bib."""
    ids = {}
    if not os.path.exists(bib_path):
        print(f"WARNING: {bib_path} not found, skipping bib parse.")
        return ids
    with open(bib_path, encoding="utf-8") as f:
        content = f.read()
    # match google_scholar_id = {XXXX} or google_scholar_id = "XXXX"
    for m in re.finditer(
        r'google_scholar_id\s*=\s*[{"]([^}"]+)[}"]', content, re.IGNORECASE
    ):
        article_id = m.group(1).strip()
        # find the nearest @entry key above this match
        preceding = content[: m.start()]
        key_match = re.findall(r"@\w+\{([^,]+),", preceding)
        entry_key = key_match[-1].strip() if key_match else "unknown"
        ids[article_id] = entry_key
    return ids


def load_existing_citations(path):
    """Load existing citations.yml so we can fall back if fetch fails."""
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data
        except Exception as e:
            print(f"WARNING: Could not read {path}: {e}")
    return {}


def fetch_author_publications(user_id):
    """Fetch all publications for a Google Scholar user with retry logic."""
    for attempt in range(1, RETRIES + 1):
        try:
            print(f"  Fetching author profile (attempt {attempt}/{RETRIES})...")
            author = scholarly.search_author_id(user_id)
            author = scholarly.fill(author, sections=["publications"])
            return author.get("publications", [])
        except Exception as e:
            print(f"  Attempt {attempt} failed: {e}")
            if attempt < RETRIES:
                print(f"  Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
    print("ERROR: All retries exhausted. Could not fetch author data.")
    return None


def build_citation_lookup(publications):
    """Build {article_id: {citations, title, year}} from raw publication list."""
    lookup = {}
    for pub in publications:
        pub_id = pub.get("author_pub_id") or pub.get("pub_id") or ""
        # author_pub_id format: "USER_ID:ARTICLE_ID"
        article_id = pub_id.split(":")[-1] if ":" in pub_id else pub_id
        if not article_id:
            continue
        bib = pub.get("bib", {})
        lookup[article_id] = {
            "citations": pub.get("num_citations", 0),
            "title":     bib.get("title", "Unknown Title"),
            "year":      str(bib.get("pub_year", "")),
        }
    return lookup


def main():
    print("=" * 60)
    print("Google Scholar Citation Fetcher")
    print(f"User ID : {SCHOLAR_USER_ID}")
    print(f"Date    : {TODAY}")
    print("=" * 60)

    # 1. Parse google_scholar_ids from papers.bib
    scholar_ids = extract_scholar_ids_from_bib(BIB_FILE)
    if not scholar_ids:
        print("No google_scholar_id entries found in papers.bib. Exiting.")
        sys.exit(0)
    print(f"\nFound {len(scholar_ids)} paper(s) with google_scholar_id in {BIB_FILE}:")
    for aid, key in scholar_ids.items():
        print(f"  {key:30s} → {aid}")

    # 2. Load existing data as fallback
    existing = load_existing_citations(OUTPUT_FILE)

    # 3. Fetch from Google Scholar
    print("\nConnecting to Google Scholar...")
    publications = fetch_author_publications(SCHOLAR_USER_ID)

    # 4. Build lookup table
    if publications is not None:
        lookup = build_citation_lookup(publications)
        print(f"\nFetched {len(lookup)} publications from Google Scholar.")
    else:
        lookup = {}
        print("WARNING: Using existing cached data only.")

    # 5. Build output dict
    output = {}
    print("\nCitation results:")
    print(f"  {'Entry Key':<30} {'Article ID':<22} {'Citations':>10}  {'Source'}")
    print("  " + "-" * 75)

    for article_id, entry_key in scholar_ids.items():
        full_key = f"{SCHOLAR_USER_ID}:{article_id}"
        if article_id in lookup:
            info = lookup[article_id]
            source = "Scholar (live)"
        elif full_key in existing:
            # fall back to cached value
            old = existing[full_key]
            info = {
                "citations": old.get("citations", 0),
                "title":     old.get("title", "Unknown"),
                "year":      str(old.get("year", "")),
            }
            source = "cached (fallback)"
        else:
            info = {"citations": 0, "title": "Unknown", "year": ""}
            source = "not found"

        output[full_key] = {
            "title":     info["title"],
            "year":      info["year"],
            "citations": info["citations"],
            "updated":   TODAY,
        }
        print(f"  {entry_key:<30} {article_id:<22} {info['citations']:>10}  ({source})")

    # 6. Write output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        yaml.dump(output, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

    print(f"\n✅ Saved {len(output)} entries to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
