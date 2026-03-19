#!/usr/bin/env python3
"""
CastingIQ CLI Ingestion Tool
=============================
Command-line interface for the Adaptive Data Engine.

Usage:
    python3 ingest.py data.xlsx                # auto-detect and clean
    python3 ingest.py data.csv --schema mes    # use saved schema  
    python3 ingest.py data/ --batch            # process all files in directory
    python3 ingest.py data.csv --output clean.csv  # save cleaned output
    python3 ingest.py data.csv --verbose       # detailed logging

Built by Brian Crusoe | github.com/brcrusoe72
"""

import argparse
import sys
from pathlib import Path

from engine import AdaptiveDataEngine


GRADE_COLORS = {"A": "\033[92m", "B": "\033[92m", "C": "\033[93m", "D": "\033[91m", "F": "\033[91m"}
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"


def print_result(result, verbose=False):
    """Pretty-print an IngestResult."""
    q = result.quality_score
    color = GRADE_COLORS.get(q.grade, "")

    print(f"\n{BOLD}══════════════════════════════════════════════════{RESET}")
    print(f"{BOLD}  CastingIQ Data Ingestion Report{RESET}")
    print(f"{BOLD}══════════════════════════════════════════════════{RESET}")

    print(f"\n  Rows:    {result.row_count_raw} raw → {result.row_count_clean} clean")
    print(f"  Dupes:   {result.duplicates_removed} removed")
    print(f"  Schema:  {result.schema_fingerprint}")

    print(f"\n{BOLD}  Data Health Score: {color}{q.grade} ({q.overall:.0f}/100){RESET}")
    print(f"    Completeness:  {q.completeness:5.1f}")
    print(f"    Consistency:   {q.consistency:5.1f}")
    print(f"    Timeliness:    {q.timeliness:5.1f}")
    print(f"    Accuracy:      {q.accuracy:5.1f}")

    print(f"\n{BOLD}  Column Mappings:{RESET}")
    for m in result.column_mappings:
        conf_pct = f"{m.confidence:.0%}"
        if m.mapped_name:
            flag = " ⚠" if m.confidence < 0.70 else " ✓"
            print(f"    {m.raw_name:30s} → {m.mapped_name:20s} [{conf_pct:>4s}]{flag}")
        else:
            print(f"    {m.raw_name:30s} → {DIM}(unmapped){RESET:20s} [{conf_pct:>4s}]")

    if result.outliers:
        print(f"\n{BOLD}  Outliers Flagged:{RESET}")
        for col, indices in result.outliers.items():
            print(f"    {col}: {len(indices)} rows")

    if verbose:
        print(f"\n{BOLD}  Cleaning Log:{RESET}")
        for msg in result.cleaning_log:
            print(f"    {msg}")

    print(f"\n{BOLD}  Cleaned Data Preview:{RESET}")
    print(result.dataframe.head(10).to_string(max_colwidth=30))
    print()


def main():
    parser = argparse.ArgumentParser(
        description="CastingIQ — Adaptive Manufacturing Data Ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("source", help="CSV/Excel file or directory path")
    parser.add_argument("--schema", "-s", help="Named schema to apply")
    parser.add_argument("--batch", "-b", action="store_true", help="Process all files in directory")
    parser.add_argument("--output", "-o", help="Save cleaned data to CSV")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed cleaning log")
    parser.add_argument("--sheet", help="Excel sheet name")

    args = parser.parse_args()
    source = Path(args.source)

    engine = AdaptiveDataEngine()

    if args.batch or source.is_dir():
        if not source.is_dir():
            print(f"Error: {source} is not a directory", file=sys.stderr)
            sys.exit(1)
        results = engine.ingest_batch(source)
        for r in results:
            print_result(r, args.verbose)
        print(f"\nProcessed {len(results)} files.")
    else:
        if not source.exists():
            print(f"Error: {source} not found", file=sys.stderr)
            sys.exit(1)
        result = engine.ingest(source, schema_name=args.schema, sheet_name=args.sheet)
        print_result(result, args.verbose)

        if args.output:
            result.dataframe.to_csv(args.output, index=False)
            print(f"Saved cleaned data to {args.output}")


if __name__ == "__main__":
    main()
