#!/usr/bin/env python3
"""
make_replacements_csv.py

Produce a CSV of replacements suitable for swap_sst_bulk.

Usage examples:
  # 1) Convert an input CSV of hex_key,decimal_value -> replacements.csv
  ./make_replacements_csv.py --map keys_and_values.csv --out replacements.csv

  # 2) Apply the same numeric value to every hex key listed in a file:
  ./make_replacements_csv.py --keys keys.txt --value 100.0 --out replacements.csv

  # 3) Read pairs from stdin (key value per line):
  cat pairs.txt | ./make_replacements_csv.py --stdin --out replacements.csv
  where each line is "1A00... 100.0" or "1A00...,100.0"

Output: CSV with lines "KEY_HEX,NEW_VALUE_HEX" where NEW_VALUE_HEX = struct.pack('>d', value).hex()
"""

import argparse
import csv
import struct
import sys
from typing import Iterable, Tuple


def to_double_hex(value: float) -> str:
    """Return big-endian IEEE754 hex for a Python float."""
    return struct.pack(">d", float(value)).hex()


def clean_hex(h: str) -> str:
    """Normalize a hex string: strip whitespace, optional 0x, uppercase not required."""
    h = h.strip()
    if h.startswith(("0x", "0X")):
        h = h[2:]
    # remove internal whitespace if any
    h = "".join(h.split())
    if len(h) == 0:
        raise ValueError("Empty hex string")
    if len(h) % 2 != 0:
        raise ValueError(f"Hex string has odd length: {h!r}")
    # ensure all chars are hex
    int(h, 16)  # will raise if invalid
    return h.lower()


def read_map_csv(path: str) -> Iterable[Tuple[str, float]]:
    """Read CSV with two columns: key_hex, numeric_value"""
    with open(path, newline="") as f:
        rdr = csv.reader(f)
        for lineno, row in enumerate(rdr, start=1):
            if not row:
                continue
            if len(row) < 2:
                raise ValueError(f"{path}:{lineno}: expected at least 2 columns")
            key_hex = clean_hex(row[0])
            val = row[1].strip()
            yield key_hex, float(val)


def read_keys_file(path: str) -> Iterable[str]:
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            yield clean_hex(line)


def read_stdin_pairs() -> Iterable[Tuple[str, float]]:
    for lineno, raw in enumerate(sys.stdin, start=1):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        # accept "key value" or "key,value"
        if "," in raw:
            parts = [p.strip() for p in raw.split(",", 1)]
        else:
            parts = raw.split(None, 1)
        if len(parts) < 2:
            raise ValueError(f"stdin:{lineno}: expected 'KEY VALUE'")
        yield clean_hex(parts[0]), float(parts[1])


def write_replacements(out_path: str, pairs: Iterable[Tuple[str, float]]):
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        for key_hex, val in pairs:
            hex_val = to_double_hex(val)
            w.writerow([key_hex, hex_val])


def main():
    p = argparse.ArgumentParser(description="Make replacements CSV for swap_sst_bulk")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--map", help="CSV file with: KEY_HEX,NUMERIC_VALUE")
    group.add_argument(
        "--keys", help="File with one KEY_HEX per line; used with --value"
    )
    group.add_argument(
        "--stdin", action="store_true", help="Read KEY and VALUE from stdin"
    )
    p.add_argument(
        "--value", help="Single numeric value to apply to all keys in --keys"
    )
    p.add_argument(
        "--out", required=True, help="Output CSV path (KEY_HEX,NEW_VALUE_HEX)"
    )
    args = p.parse_args()

    if args.map:
        src = list(read_map_csv(args.map))
        if not src:
            print("No pairs found in map file.", file=sys.stderr)
    elif args.keys:
        if args.value is None:
            p.error("--keys requires --value")
        keys = list(read_keys_file(args.keys))
        src = [(k, float(args.value)) for k in keys]
        if not src:
            print("No keys found in keys file.", file=sys.stderr)
    else:  # stdin
        src = list(read_stdin_pairs())
        if not src:
            print("No pairs read from stdin.", file=sys.stderr)

    # write replacements
    write_replacements(args.out, src)

    # print preview
    preview = src[:5]
    print(
        f"Wrote {len(src)} replacement(s) to {args.out}. Preview (first {len(preview)}):"
    )
    for k, v in preview:
        print(f"  {k} -> {to_double_hex(v)}")


if __name__ == "__main__":
    main()
