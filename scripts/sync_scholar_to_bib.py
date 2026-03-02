#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sync all publications from a Google Scholar author profile into a BibTeX file.

Target: al-folio / jekyll-scholar expects: _bibliography/papers.bib
- Writes a single BibTeX file with YAML front-matter needed by al-folio.
- Uses 'scholarly' (unofficial) to fetch author + publications and export BibTeX.
  scholarly has bibtex(Publication) API and fill(author, publication_limit=0) for full list.
Refs:
  - scholarly docs: bibtex(), fill(... publication_limit=0)  (see scholarly docs / PyPI)
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, Tuple

from scholarly import scholarly

# Optional: bibtexparser for minor cleanup/dedup
import bibtexparser


SCHOLAR_USER_ID = os.getenv("SCHOLAR_USER_ID", "7IZyaZsAAAAJ")
OUTPUT_BIB = os.getenv("OUTPUT_BIB", "_bibliography/papers.bib")

# Conservative throttling to reduce blocking risk
SLEEP_BETWEEN_PUBS_SEC = float(os.getenv("SLEEP_BETWEEN_PUBS_SEC", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# If you want to preserve custom fields (selected/pdf/code/etc.) from existing bib,
# we will try to carry them over by matching normalized title + year.
PRESERVE_FIELDS = {
    "selected", "abbr", "pdf", "code", "website", "preview", "abstract", "supp",
    "poster", "slides", "blog", "html"
}


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[{}()\\[\\]\"'`]", "", t)
    return t


def load_existing_custom_fields(path: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Parse existing BibTeX file and remember custom fields per (norm_title, year).
    This lets you keep manual annotations like selected={true}, pdf=..., code=...
    """
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

        # Strip al-folio YAML front-matter if present
        raw_wo_fm = re.sub(r"^---\s*---\s*", "", raw, flags=re.M)

        bib_db = bibtexparser.loads(raw_wo_fm)
        mapping = {}
        for e in bib_db.entries:
            title = e.get("title", "")
            year = e.get("year", "")
            key = (normalize_title(title), str(year))
            keep = {k: v for k, v in e.items() if k in PRESERVE_FIELDS}
            if keep:
                mapping[key] = keep
        return mapping
    except Exception:
        return {}


def try_fill_with_retries(obj, **kwargs):
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            return scholarly.fill(obj, **kwargs)
        except Exception as e:
            last_err = e
            # exponential backoff
            time.sleep(2 ** i)
    raise last_err


def main():
    print(f"[INFO] Scholar user id: {SCHOLAR_USER_ID}")
    print(f"[INFO] Output bib file: {OUTPUT_BIB}")

    existing_custom = load_existing_custom_fields(OUTPUT_BIB)
    if existing_custom:
        print(f"[INFO] Loaded custom fields from existing bib: {len(existing_custom)} entries")

    # Fetch author by id and fill all publications
    author = scholarly.search_author_id(SCHOLAR_USER_ID)
    # sections list is supported by scholarly.fill for Author
    author = try_fill_with_retries(
        author,
        sections=["basics", "indices", "counts", "publications"],
        sortby="year",
        publication_limit=0,  # 0 means no limit (full list)
    )

    pubs = author.get("publications", [])
    print(f"[INFO] Publications found: {len(pubs)}")

    bib_entries = []
    for idx, p in enumerate(pubs, start=1):
        # Fill each publication then export bibtex
        pub = try_fill_with_retries(p)

        # scholarly.bibtex(pub) returns a BibTeX entry string
        try:
            bib = scholarly.bibtex(pub)
        except Exception:
            # some entries may not have citation source; fallback to bib in dict if present
            bib = pub.get("bibtex", "")

        if not bib:
            print(f"[WARN] Empty bibtex for pub #{idx}")
            continue

        # Inject preserved custom fields if we can match by (title, year)
        title = pub.get("bib", {}).get("title", "") or ""
        year = str(pub.get("bib", {}).get("pub_year", pub.get("bib", {}).get("year", "")) or "")
        key = (normalize_title(title), year)

        custom = existing_custom.get(key)
        if custom:
            # simple injection before closing brace of entry
            # ensure bib ends with "}\n"
            bib = bib.rstrip()
            if bib.endswith("}"):
                insert_lines = []
                for k, v in custom.items():
                    # keep original value formatting
                    insert_lines.append(f"  {k}={{{v}}},")
                insert_block = "\n" + "\n".join(insert_lines) + "\n"
                bib = bib[:-1] + insert_block + "}"
            bib += "\n"
        else:
            bib += "\n"

        bib_entries.append(bib)

        if idx % 10 == 0:
            print(f"[INFO] Processed {idx}/{len(pubs)}")
        time.sleep(SLEEP_BETWEEN_PUBS_SEC)

    # Combine into al-folio compatible file (needs YAML front-matter)
    header = "---\n---\n"
    header += f"% Auto-synced from Google Scholar (user={SCHOLAR_USER_ID})\n"
    header += f"% Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"

    merged = header + "\n".join(bib_entries).strip() + "\n"

    # Normalize using bibtexparser (best-effort)
    try:
        merged_wo_fm = re.sub(r"^---\s*---\s*", "", merged, flags=re.M)
        db = bibtexparser.loads(merged_wo_fm)
        cleaned = bibtexparser.dumps(db)
        merged = header + cleaned.strip() + "\n"
    except Exception:
        pass

    os.makedirs(os.path.dirname(OUTPUT_BIB), exist_ok=True)
    with open(OUTPUT_BIB, "w", encoding="utf-8") as f:
        f.write(merged)

    print(f"[DONE] Wrote: {OUTPUT_BIB}")


if __name__ == "__main__":
    main()