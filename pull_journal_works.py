#!/usr/bin/env python3
"""Pull works (articles) from a set of journals out of an OpenAlex works snapshot.

Reads the source IDs from the CSV written by find_law_journals.py, streams the
OpenAlex `works` files (JSON Lines, plain or .gz), and writes two CSVs:
  * works.csv       one row per work
  * authorships.csv one row per (work, author), for building author lists

Usage:
  python pull_journal_works.py openalex-snapshot/data/works/ \
      --journals law_journals.csv --min-share 0.5 --year-from 2000 --workers 8

The works snapshot is very large, so files are processed in parallel and each
line is only fully parsed when it mentions one of the wanted source IDs.
Only standard-library modules are used.
"""

import argparse
import csv
import gzip
import json
import re
import sys
from multiprocessing import Pool
from pathlib import Path

SOURCE_ID_RE = re.compile(r'https://openalex\.org/(S\d+)"')

WORK_FIELDS = [
    "work_id", "doi", "title", "publication_year", "publication_date", "type",
    "language", "source_id", "source_name", "volume", "issue", "first_page",
    "last_page", "cited_by_count", "is_oa", "primary_topic", "primary_subfield",
    "n_authors", "authors", "abstract",
]
AUTHORSHIP_FIELDS = [
    "work_id", "publication_year", "source_id", "author_position", "author_id",
    "author_name", "raw_author_name", "orcid", "is_corresponding",
    "institution_ids", "institution_names", "country_codes",
]

# Set in each worker by init_worker().
WANTED = frozenset()
OPTS = None


def short_id(url):
    return (url or "").rsplit("/", 1)[-1]


def load_journal_ids(path, min_share):
    ids = set()
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if min_share is not None and float(row.get("law_share") or 0) < min_share:
                continue
            ids.add(short_id(row["id"]))
    return ids


def iter_files(paths):
    for p in map(Path, paths):
        if p.is_dir():
            yield from sorted(f for f in p.rglob("*") if f.is_file()
                              and (f.suffix == ".gz" or f.suffix in (".json", ".jsonl", "")))
        else:
            yield p


def rebuild_abstract(inverted):
    if not inverted:
        return ""
    words = {}
    for word, positions in inverted.items():
        for pos in positions:
            words[pos] = word
    return " ".join(words[i] for i in sorted(words))


def matched_source(work):
    """Return the wanted source dict this work belongs to, or None."""
    locations = [work.get("primary_location") or {}]
    if OPTS.any_location:
        locations += work.get("locations") or []
    for loc in locations:
        src = loc.get("source") or {}
        if short_id(src.get("id")) in WANTED:
            return src
    return None


def process_file(path):
    works, authorships, seen, errors = [], [], 0, 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            seen += 1
            # Cheap pre-filter: skip lines that don't mention a wanted source.
            if not any(m in WANTED for m in SOURCE_ID_RE.findall(line)):
                continue
            try:
                work = json.loads(line)
            except json.JSONDecodeError:
                errors += 1
                continue
            src = matched_source(work)
            if src is None:
                continue
            year = work.get("publication_year")
            if OPTS.year_from and (year or 0) < OPTS.year_from:
                continue
            if OPTS.year_to and (year or 9999) > OPTS.year_to:
                continue
            if OPTS.types and work.get("type") not in OPTS.types:
                continue

            wid = short_id(work.get("id"))
            biblio = work.get("biblio") or {}
            topic = work.get("primary_topic") or {}
            auths = work.get("authorships") or []
            works.append({
                "work_id": wid,
                "doi": work.get("doi"),
                "title": work.get("display_name") or work.get("title"),
                "publication_year": year,
                "publication_date": work.get("publication_date"),
                "type": work.get("type"),
                "language": work.get("language"),
                "source_id": short_id(src.get("id")),
                "source_name": src.get("display_name"),
                "volume": biblio.get("volume"),
                "issue": biblio.get("issue"),
                "first_page": biblio.get("first_page"),
                "last_page": biblio.get("last_page"),
                "cited_by_count": work.get("cited_by_count"),
                "is_oa": (work.get("open_access") or {}).get("is_oa"),
                "primary_topic": topic.get("display_name"),
                "primary_subfield": (topic.get("subfield") or {}).get("display_name"),
                "n_authors": len(auths),
                "authors": "; ".join((a.get("author") or {}).get("display_name") or ""
                                     for a in auths),
                "abstract": rebuild_abstract(work.get("abstract_inverted_index"))
                            if OPTS.abstracts else "",
            })
            for a in auths:
                author = a.get("author") or {}
                insts = a.get("institutions") or []
                authorships.append({
                    "work_id": wid,
                    "publication_year": year,
                    "source_id": short_id(src.get("id")),
                    "author_position": a.get("author_position"),
                    "author_id": short_id(author.get("id")),
                    "author_name": author.get("display_name"),
                    "raw_author_name": a.get("raw_author_name"),
                    "orcid": author.get("orcid"),
                    "is_corresponding": a.get("is_corresponding"),
                    "institution_ids": "|".join(short_id(i.get("id")) for i in insts),
                    "institution_names": "|".join(i.get("display_name") or "" for i in insts),
                    "country_codes": "|".join(sorted(set(
                        c for c in (a.get("countries") or []) if c))),
                })
    return path, works, authorships, seen, errors


def init_worker(wanted, opts):
    global WANTED, OPTS
    WANTED, OPTS = wanted, opts


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="+", help="works files (.gz or plain) or directories")
    ap.add_argument("--journals", default="law_journals.csv",
                    help="CSV from find_law_journals.py (needs an 'id' column)")
    ap.add_argument("--min-share", type=float, default=None,
                    help="only use journals with law_share >= this")
    ap.add_argument("--year-from", type=int)
    ap.add_argument("--year-to", type=int)
    ap.add_argument("--types", nargs="*",
                    help="keep only these work types, e.g. article review")
    ap.add_argument("--any-location", action="store_true",
                    help="also match works whose non-primary locations are in a journal")
    ap.add_argument("--abstracts", action="store_true",
                    help="rebuild abstracts from abstract_inverted_index (larger output)")
    ap.add_argument("--works-out", default="works.csv")
    ap.add_argument("--authorships-out", default="authorships.csv")
    ap.add_argument("--workers", type=int, default=4)
    opts = ap.parse_args()

    wanted = frozenset(load_journal_ids(opts.journals, opts.min_share))
    if not wanted:
        sys.exit("no journal ids loaded; check --journals / --min-share")
    print(f"looking for works in {len(wanted):,} journals", file=sys.stderr)

    files = list(iter_files(opts.inputs))
    total_seen = total_works = total_errors = 0
    with open(opts.works_out, "w", newline="", encoding="utf-8") as wf, \
         open(opts.authorships_out, "w", newline="", encoding="utf-8") as af, \
         Pool(opts.workers, initializer=init_worker, initargs=(wanted, opts)) as pool:
        wwriter = csv.DictWriter(wf, fieldnames=WORK_FIELDS)
        awriter = csv.DictWriter(af, fieldnames=AUTHORSHIP_FIELDS)
        wwriter.writeheader()
        awriter.writeheader()
        for i, (path, works, auths, seen, errors) in enumerate(
                pool.imap_unordered(process_file, files), 1):
            wwriter.writerows(works)
            awriter.writerows(auths)
            total_seen += seen
            total_works += len(works)
            total_errors += errors
            print(f"[{i}/{len(files)}] {path}: {len(works):,} kept "
                  f"(total {total_works:,} of {total_seen:,})", file=sys.stderr)

    if total_errors:
        print(f"warning: {total_errors:,} unparseable lines skipped", file=sys.stderr)
    print(f"done: {total_works:,} works written to {opts.works_out}, "
          f"authorships to {opts.authorships_out}", file=sys.stderr)


if __name__ == "__main__":
    main()
