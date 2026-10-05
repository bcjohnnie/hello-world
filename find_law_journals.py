#!/usr/bin/env python3
"""Find law journals in an OpenAlex sources snapshot.

Streams one or more OpenAlex `sources` files (JSON Lines, plain or .gz) and
writes a CSV of sources that look like law journals, based on:
  * law_share: fraction of the source's topic counts in subfield 3308 (Law)
  * name_match: display_name / alternate_titles contain a law keyword

Usage:
  python find_law_journals.py part_000.gz [more files or directories ...] \
      -o law_journals.csv --min-share 0.5

Only standard-library modules are used.
"""

import argparse
import csv
import gzip
import json
import re
import sys
from pathlib import Path

LAW_SUBFIELD = "https://openalex.org/subfields/3308"

# Whole-word-ish keywords for law in several languages.
NAME_PATTERN = re.compile(
    r"\b(law|laws|legal|lawyer|jurist\w*|juris\w*|jurid\w*|justice|"
    r"recht\w*|droit|derecho|diritto|direito|prawo|pravo|court|courts|"
    r"constitutional|legislat\w*|criminolog\w*)\b",
    re.IGNORECASE,
)

FIELDS = [
    "id", "display_name", "issn_l", "issn", "type", "host_organization_name",
    "country_code", "works_count", "cited_by_count", "h_index",
    "first_publication_year", "last_publication_year",
    "law_share", "law_topic_count", "total_topic_count", "name_match",
    "top_law_topics",
]


def iter_files(paths):
    for p in map(Path, paths):
        if p.is_dir():
            yield from sorted(f for f in p.rglob("*") if f.is_file()
                              and (f.suffix == ".gz" or f.suffix in (".json", ".jsonl", "")))
        else:
            yield p


def iter_records(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"warning: {path}:{lineno}: {e}", file=sys.stderr)


def summarize(src):
    topics = src.get("topics") or []
    total = sum(t.get("count") or 0 for t in topics)
    law_topics = [t for t in topics
                  if (t.get("subfield") or {}).get("id") == LAW_SUBFIELD]
    law = sum(t.get("count") or 0 for t in law_topics)
    names = [src.get("display_name") or ""] + (src.get("alternate_titles") or [])
    return {
        "id": src.get("id", "").rsplit("/", 1)[-1],
        "display_name": src.get("display_name"),
        "issn_l": src.get("issn_l"),
        "issn": "|".join(src.get("issn") or []),
        "type": src.get("type"),
        "host_organization_name": src.get("host_organization_name"),
        "country_code": src.get("country_code"),
        "works_count": src.get("works_count"),
        "cited_by_count": src.get("cited_by_count"),
        "h_index": (src.get("summary_stats") or {}).get("h_index"),
        "first_publication_year": src.get("first_publication_year"),
        "last_publication_year": src.get("last_publication_year"),
        "law_share": round(law / total, 4) if total else 0.0,
        "law_topic_count": law,
        "total_topic_count": total,
        "name_match": any(NAME_PATTERN.search(n) for n in names),
        "top_law_topics": "; ".join(t.get("display_name", "") for t in law_topics[:5]),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="+", help="sources files (.gz or plain) or directories")
    ap.add_argument("-o", "--output", default="law_journals.csv")
    ap.add_argument("--min-share", type=float, default=0.5,
                    help="keep sources with law_share >= this (default 0.5)")
    ap.add_argument("--no-name-match", action="store_true",
                    help="don't keep sources that only match by name")
    ap.add_argument("--journals-only", action="store_true",
                    help="keep only type == 'journal'")
    args = ap.parse_args()

    seen = kept = 0
    with open(args.output, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=FIELDS)
        writer.writeheader()
        for path in iter_files(args.inputs):
            for src in iter_records(path):
                seen += 1
                if args.journals_only and src.get("type") != "journal":
                    continue
                row = summarize(src)
                if row["law_share"] >= args.min_share or (
                        row["name_match"] and not args.no_name_match):
                    writer.writerow(row)
                    kept += 1
            print(f"{path}: {seen:,} sources read, {kept:,} kept so far", file=sys.stderr)

    print(f"done: {kept:,} of {seen:,} sources written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
