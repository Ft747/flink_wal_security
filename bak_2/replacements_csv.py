#!/usr/bin/env python3
"""
replacements_csv.py

Generate a replacements CSV from an SST file by replacing ALL values with a
single numeric constant (default: 100.0).

Usage:
    ./replacements_csv.py /path/to/file.sst --out replacements.csv --value 100.0
"""

import argparse
import re
import struct
import subprocess
import csv

LINE_RE = re.compile(
    r"'(?P<key>[0-9A-Fa-f]+)'\s+seq:(?P<seq>\d+),\s+type:(?P<type>\d+)\s+=>\s+(?P<value>[0-9A-Fa-f]+)"
)


def dump_sst(filepath: str) -> str:
    """Run sst_dump --output_hex and return stdout as string."""
    result = subprocess.run(
        ["sst_dump", f"--file={filepath}", "--command=scan", "--output_hex"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def parse_lines(dump_text):
    """Yield key_hex from sst_dump output."""
    for line in dump_text.splitlines():
        m = LINE_RE.search(line)
        if m:
            yield m.group("key")


def to_double_hex(value: float) -> str:
    """Return big-endian IEEE754 hex for a float."""
    return struct.pack(">d", value).hex()


def main():
    parser = argparse.ArgumentParser(
        description="Generate replacements CSV for all keys in an SST file."
    )
    parser.add_argument("sst_file", help="Path to SST file")
    parser.add_argument("--out", required=True, help="Output CSV path")
    parser.add_argument(
        "--value", type=float, default=100.0, help="Replacement value (float)"
    )
    args = parser.parse_args()

    print(f"Running sst_dump on {args.sst_file} ...")
    dump_text = dump_sst(args.sst_file)
    key_list = list(parse_lines(dump_text))
    print(f"Found {len(key_list)} keys.")

    hex_val = to_double_hex(args.value)
    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        for key in key_list:
            writer.writerow([key, hex_val])

    print(f"Wrote replacements CSV to {args.out} with {len(key_list)} entries.")
    print(f"All values replaced with {args.value} -> {hex_val}")


if __name__ == "__main__":
    main()
