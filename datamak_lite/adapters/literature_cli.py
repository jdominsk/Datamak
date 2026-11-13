"""CLI bindings for the local literature catalogue."""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from datamak_lite.core.literature import (
    find_duplicates,
    find_papers,
    list_reviews,
    register_paper,
)
from datamak_lite.adapters.literature_review import discover_reviews, import_review


def _paper_lines(paper: dict) -> list[str]:
    lines = [f"{paper['uid']}  {paper['title']}"]
    identifiers = [paper.get('doi', ''), paper.get('arxiv', '')]
    if any(identifiers):
        lines.append("  identifiers: " + "; ".join(value for value in identifiers if value))
    if not paper.get('doi') and not paper.get('arxiv'):
        lines.append("  identifiers: missing; bibliographic identity incomplete")
    for review in paper.get('reviews', []):
        depth = review.get('depth') or 'not recorded'
        status = review.get('corpus_status') or 'not recorded'
        lines.append(f"  review: {review['name']} [{review['uid']}]; depth: {depth}; corpus: {status}")
        lines.append(f"    folder: {review['path']}")
        for kind in ('note_paths', 'pdf_paths'):
            for path in review.get(kind, []):
                lines.append(f"    {'note' if kind == 'note_paths' else 'PDF'}: {path}")
    return lines


def _run(args: argparse.Namespace) -> None:
    try:
        if args.command == 'literature-discover':
            result = discover_reviews(args.roots)
            if args.json:
                print(json.dumps(result, indent=2))
            elif not result:
                print('No recognizable review folders found under the supplied roots.')
            else:
                for item in result:
                    print(item['path'])
                    print('  evidence: ' + ', '.join(item.get('evidence', [])))
            return
        if args.command == 'literature-import':
            result = import_review(args.root, db_path=args.db, name=args.name,
                                   uid=args.uid, dry_run=args.dry_run)
            print(json.dumps(result, indent=2))
            return
        if args.command == 'literature-reviews':
            result = list_reviews(db_path=args.db)
            if args.json:
                print(json.dumps(result, indent=2))
            elif not result:
                print('No reviews registered.')
            else:
                for item in result:
                    print(f"{item['uid']}  {item['name']}")
                    print(f"  {item['path']}")
            return
        if args.command == 'literature-find':
            result = find_papers(args.query, db_path=args.db)
            if args.json:
                print(json.dumps(result, indent=2))
            elif not result:
                print('No matching papers registered.')
            else:
                for paper in result:
                    print('\n'.join(_paper_lines(paper)))
            return
        if args.command == 'literature-add-paper':
            result = register_paper(
                args.review_uid, title=args.title, authors=args.authors, year=args.year,
                doi=args.doi, arxiv=args.arxiv, uid=args.uid, pdf_path=args.pdf,
                note_path=args.note, depth=args.depth, corpus_status=args.corpus_status,
                db_path=args.db,
            )
            print(json.dumps(result, indent=2) if args.json else '\n'.join(_paper_lines(result)))
            return
        result = find_duplicates(db_path=args.db)
        if args.json:
            print(json.dumps(result, indent=2))
        elif not result:
            print('No possible duplicates found.')
        else:
            print('Possible duplicate records for inspection; no records or files were merged.')
            print(json.dumps(result, indent=2))
    except (ValueError, KeyError, OSError, sqlite3.Error) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        raise SystemExit(1) from None


def add_literature_commands(sub: argparse._SubParsersAction) -> None:
    discover = sub.add_parser('literature-discover', help='Find review folders under explicitly supplied local roots.')
    discover.add_argument('roots', nargs='+', type=Path)
    discover.add_argument('--json', action='store_true')
    discover.set_defaults(func=_run)

    imp = sub.add_parser('literature-import', help='Index an existing review in place; do not modify its files.')
    imp.add_argument('root', type=Path)
    imp.add_argument('--name')
    imp.add_argument('--uid', help='Existing or new stable review UID.')
    imp.add_argument('--dry-run', action='store_true', help='Show extracted records and metadata gaps without writing.')

    reviews = sub.add_parser('literature-reviews', help='List registered review folders.')
    find = sub.add_parser('literature-find', help='Find papers and the reviews, notes, and PDFs that reference them.')
    find.add_argument('query', nargs='?', default='')
    duplicates = sub.add_parser('literature-duplicates', help='Suggest possible duplicate paper records without merging them.')
    paper = sub.add_parser('literature-add-paper', help='Register or update a paper and its membership in a review.')
    paper.add_argument('review_uid')
    paper.add_argument('--title', required=True)
    paper.add_argument('--authors', default='')
    paper.add_argument('--year', default='')
    paper.add_argument('--doi', default='')
    paper.add_argument('--arxiv', default='')
    paper.add_argument('--uid', help='Existing stable paper UID; use to enrich an incomplete record.')
    paper.add_argument('--pdf', type=Path, help='Existing PDF path; the file is not copied.')
    paper.add_argument('--note', type=Path, help='Existing paper note path; the file is not changed.')
    paper.add_argument('--depth', default='', choices=['', 'seed', 'comparative', 'deep'])
    paper.add_argument('--corpus-status', choices=['canonical', 'provisional'],
                       help='Defaults to canonical for a new membership; preserves existing status when omitted.')

    for parser in (imp, reviews, find, duplicates, paper):
        parser.add_argument('--db', type=Path,
                            help='Catalogue path; defaults to $DATAMAK_HOME/literature.sqlite or ~/.datamak/literature.sqlite.')
        if parser is not imp:
            parser.add_argument('--json', action='store_true')
        parser.set_defaults(func=_run)
