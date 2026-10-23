#!/usr/bin/env python3
"""Import Scholar records without changing the manually curated homepage.

Existing entries, keys, venue decisions, and presentation fields take precedence.
New records are unselected. An empty or incomplete export never replaces the file.
"""

import os
import re
import tempfile
import time
from pathlib import Path

import bibtexparser
from bibtexparser.bparser import BibTexParser
from bibtexparser.bwriter import BibTexWriter


SCHOLAR_USER_ID = os.getenv("SCHOLAR_USER_ID", "7IZyaZsAAAAJ")
OUTPUT_BIB = os.getenv("OUTPUT_BIB", "_bibliography/papers.bib")
SLEEP_BETWEEN_PUBS_SEC = float(os.getenv("SLEEP_BETWEEN_PUBS_SEC", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


def normalize_title(title):
    return re.sub(r"[\W_]+", "", title.casefold())


def parse_bibliography(raw):
    raw = re.sub(r"\A---\s*\n---\s*\n", "", raw)
    parser = BibTexParser(common_strings=True)
    parser.ignore_nonstandard_types = False
    database = bibtexparser.loads(raw, parser=parser)
    expected = sum(
        kind.lower() not in {"comment", "string", "preamble"}
        for kind in re.findall(r"(?m)^\s*@([a-zA-Z]+)\s*[({]", raw)
    )
    if expected != len(database.entries):
        raise ValueError("Incomplete BibTeX parse; refusing to discard records.")
    keys = [entry["ID"] for entry in database.entries]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate BibTeX keys; resolve them before syncing.")
    return database


def same_publication(left, right):
    for field in ("google_scholar_id", "doi", "arxiv"):
        if left.get(field) and right.get(field):
            if left[field].casefold() == right[field].casefold():
                return True
    title = normalize_title(left.get("title", ""))
    return bool(title and title == normalize_title(right.get("title", "")))


def merge_publications(existing, incoming):
    if not incoming:
        raise ValueError("Scholar returned no publications; existing file is unchanged.")
    merged = [entry.copy() for entry in existing]
    for record in incoming:
        if not record.get("title") or not record.get("author"):
            raise ValueError("Incomplete Scholar record; existing file is unchanged.")
        match = next((i for i, entry in enumerate(merged) if same_publication(entry, record)), None)
        if match is not None:
            # Local decisions are authoritative, including selected=false and venues.
            merged[match] = {**record, **merged[match]}
            continue
        addition = record.copy()
        addition["selected"] = "false"
        used_keys = {entry["ID"] for entry in merged}
        base_key = addition["ID"]
        suffix = 2
        while addition["ID"] in used_keys:
            addition["ID"] = f"{base_key}_{suffix}"
            suffix += 1
        merged.append(addition)
    return merged


def update_bibliography(path, incoming):
    path = Path(path)
    original = path.read_text(encoding="utf-8") if path.exists() else ""
    database = parse_bibliography(original)
    database.entries = merge_publications(database.entries, incoming)
    writer = BibTexWriter()
    writer.order_entries_by = ()  # Retain the curated display order and stable keys.
    writer.indent = "  "
    updated = bibtexparser.dumps(database, writer)
    if updated == original:
        return False

    # Validate the complete output before an atomic replacement.
    parse_bibliography(updated)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", delete=False,
        ) as temporary:
            temporary.write(updated)
            temporary_path = Path(temporary.name)
        os.chmod(temporary_path, path.stat().st_mode & 0o777 if path.exists() else 0o644)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    return True


def fill_with_retries(provider, record, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return provider.fill(record, **kwargs)
        except Exception:
            if attempt + 1 == MAX_RETRIES:
                raise
            time.sleep(2 ** attempt)


def main(provider=None):
    if provider is None:
        from scholarly import scholarly
        provider = scholarly

    author = fill_with_retries(
        provider, provider.search_author_id(SCHOLAR_USER_ID),
        sections=["publications"], sortby="year",
    )
    publications = author.get("publications", [])
    if not publications:
        raise ValueError("Scholar returned no publications; existing file is unchanged.")

    incoming = []
    for publication in publications:
        filled = fill_with_retries(provider, publication)
        exported = parse_bibliography(provider.bibtex(filled))
        if len(exported.entries) != 1:
            raise ValueError("Incomplete Scholar export; existing file is unchanged.")
        record = exported.entries[0]
        scholar_id = filled.get("author_pub_id", "").split(":")[-1]
        if scholar_id:
            record["google_scholar_id"] = scholar_id
        incoming.append(record)
        time.sleep(SLEEP_BETWEEN_PUBS_SEC)

    changed = update_bibliography(OUTPUT_BIB, incoming)
    print(f"Imported {len(incoming)} records. Curated selections preserved. Changed: {changed}")


if __name__ == "__main__":
    main()
