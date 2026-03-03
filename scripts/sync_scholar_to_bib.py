#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sync all publications from a Google Scholar author profile into a BibTeX file,
and auto-mark selected={true} for top-N papers by a hybrid rule:
  default: most recent first (year desc), then most cited (citations desc).

This is designed for al-folio/jekyll-scholar, which reads _bibliography/papers.bib.
scholarly supports:
  - search_author_id(...)
  - fill(..., publication_limit=0)
  - bibtex(publication)
Refs: scholarly docs / PyPI (bibtex & fill).  al-folio uses papers.bib for publications.
"""

import os
import re
import time
from datetime import datetime
from typing import Dict, Tuple, List

from scholarly import scholarly
import bibtexparser


# ---------------------------
# Config (via env or defaults)
# ---------------------------
SCHOLAR_USER_ID = os.getenv("SCHOLAR_USER_ID", "7IZyaZsAAAAJ")
OUTPUT_BIB = os.getenv("OUTPUT_BIB", "_bibliography/papers.bib")

TOP_SELECTED = int(os.getenv("TOP_SELECTED", "5"))
# SELECT_MODE options:
#   - recent_then_cited (default): year desc, citations desc
#   - top_cited: citations desc, year desc
#   - recent: year desc only (citations tie-break)
SELECT_MODE = os.getenv("SELECT_MODE", "recent_then_cited")

SLEEP_BETWEEN_PUBS_SEC = float(os.getenv("SLEEP_BETWEEN_PUBS_SEC", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Preserve these manual fields from an existing papers.bib (except selected, which we overwrite)
PRESERVE_FIELDS = {
    "abbr", "pdf", "code", "website", "preview", "abstract", "supp",
    "poster", "slides", "blog", "html", "arxiv"
}


def normalize_title(title: str) -> str:
    t = (title or "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"[{}()\[\]\"'`]", "", t)
    return t


def load_existing_custom_fields(path: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    """Keep manual fields (pdf/code/preview/...) by matching (normalized_title, year)."""
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

        # Strip al-folio YAML front-matter if present
        raw_wo_fm = re.sub(r"^---\s*---\s*", "", raw, flags=re.M)
        db = bibtexparser.loads(raw_wo_fm)

        mapping = {}
        for e in db.entries:
            title = e.get("title", "")
            year = str(e.get("year", "") or "")
            key = (normalize_title(title), year)
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
            time.sleep(2 ** i)  # exponential backoff
    raise last_err


def extract_year_and_cites(pub_filled: dict) -> Tuple[int, int]:
    """Return (year, citations). Missing values become 0."""
    bib = pub_filled.get("bib", {}) or {}
    year_str = bib.get("pub_year") or bib.get("year") or ""
    try:
        year = int(str(year_str))
    except Exception:
        year = 0

    cites = pub_filled.get("num_citations", 0) or 0
    try:
        cites = int(cites)
    except Exception:
        cites = 0

    return year, cites


def remove_selected_field(bib: str) -> str:
    """Remove any existing selected=... field to avoid duplicates."""
    # remove lines like: selected={true},
    bib = re.sub(r"(?im)^\s*selected\s*=\s*\{.*?\}\s*,\s*$", "", bib)
    return bib


def inject_fields(bib: str, fields: Dict[str, str]) -> str:
    """Inject fields right before the last closing brace of the entry."""
    bib = bib.rstrip()
    if not bib.endswith("}"):
        return bib + "\n"

    insert_lines = []
    for k, v in fields.items():
        insert_lines.append(f"  {k}={{{v}}},")
    insert_block = "\n" + "\n".join(insert_lines) + "\n"

    bib = bib[:-1] + insert_block + "}"
    return bib + "\n"


def choose_selected_keys(pub_metas: List[dict]) -> set:
    """
    pub_metas: list of dict with keys: key=(norm_title, year_str), year=int, cites=int
    returns: set of keys to mark selected.
    """
    if SELECT_MODE == "top_cited":
        ordered = sorted(pub_metas, key=lambda x: (x["cites"], x["year"]), reverse=True)
    elif SELECT_MODE == "recent":
        ordered = sorted(pub_metas, key=lambda x: (x["year"], x["cites"]), reverse=True)
    else:  # recent_then_cited
        ordered = sorted(pub_metas, key=lambda x: (x["year"], x["cites"]), reverse=True)

    top = ordered[:TOP_SELECTED]
    return {m["key"] for m in top}


def main():
    print(f"[INFO] Scholar user id: {SCHOLAR_USER_ID}")
    print(f"[INFO] Output bib file: {OUTPUT_BIB}")
    print(f"[INFO] SELECT_MODE={SELECT_MODE}, TOP_SELECTED={TOP_SELECTED}")

    existing_custom = load_existing_custom_fields(OUTPUT_BIB)
    if existing_custom:
        print(f"[INFO] Loaded preserved fields from existing bib: {len(existing_custom)} entries")

    # Fetch author and fill all publications
    author = scholarly.search_author_id(SCHOLAR_USER_ID)
    author = try_fill_with_retries(
        author,
        sections=["basics", "indices", "counts", "publications"],
        sortby="year",
        publication_limit=0,  # 0 means no limit (full list)
    )

    pubs = author.get("publications", [])
    print(f"[INFO] Publications found: {len(pubs)}")

    # First pass: collect (title, year, citations) for selection
    pub_metas = []
    filled_cache = []  # keep filled pubs to avoid refetch twice
    for p in pubs:
        pub_filled = try_fill_with_retries(p)
        filled_cache.append(pub_filled)

        title = pub_filled.get("bib", {}).get("title", "") or ""
        year, cites = extract_year_and_cites(pub_filled)
        key = (normalize_title(title), str(year))

        pub_metas.append({"key": key, "year": year, "cites": cites})

        time.sleep(SLEEP_BETWEEN_PUBS_SEC)

    selected_keys = choose_selected_keys(pub_metas)
    print(f"[INFO] Selected keys count: {len(selected_keys)}")

    # Second pass: export bibtex + inject fields
    bib_entries = []
    for idx, pub_filled in enumerate(filled_cache, start=1):
        title = pub_filled.get("bib", {}).get("title", "") or ""
        year, cites = extract_year_and_cites(pub_filled)
        key = (normalize_title(title), str(year))

        try:
            bib = scholarly.bibtex(pub_filled)
        except Exception:
            bib = pub_filled.get("bibtex", "")

        if not bib:
            print(f"[WARN] Empty bibtex for pub #{idx}")
            continue

        bib = remove_selected_field(bib)

        # preserve manual fields like pdf/code/preview from existing papers.bib
        preserved = existing_custom.get(key, {}).copy()

        # overwrite selected based on rule
        auto_fields = {}
        if key in selected_keys:
            auto_fields["selected"] = "true"

        # merge fields (auto selected + preserved others)
        merged_fields = {}
        merged_fields.update(preserved)
        merged_fields.update(auto_fields)

        if merged_fields:
            bib = inject_fields(bib, merged_fields)
        else:
            bib = bib.rstrip() + "\n"

        bib_entries.append(bib)

        if idx % 20 == 0:
            print(f"[INFO] Exported {idx}/{len(filled_cache)} (year={year}, cites={cites})")

    # Write al-folio compatible bib with YAML front-matter
    header = "---\n---\n"
    header += f"% Auto-synced from Google Scholar (user={SCHOLAR_USER_ID})\n"
    header += f"% Generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
    header += f"% SELECT_MODE={SELECT_MODE}, TOP_SELECTED={TOP_SELECTED}\n\n"

    merged = header + "\n".join(bib_entries).strip() + "\n"

    # Best-effort normalize using bibtexparser
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