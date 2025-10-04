#!/usr/bin/env python3
"""
deserialize_sst_dump.py

Usage:
    python3 deserialize_sst_dump.py /path/to/sst_dump_output.txt

Outputs (in same directory as input):
    - extracted_integers.txt
    - extracted_doubles.txt
    - mapping.csv   (key_hex, value_double, value_int_or_empty)
"""

import sys
import re
import struct
import csv
from pathlib import Path

HEX_AFTER_ARROW = re.compile(r"=>\s*([0-9A-Fa-f]+)\s*$")
KEY_HEX_IN_QUOTES = re.compile(r"'([^']+)'")


def parse_line(line):
    # extract key hex (between single quotes) and value hex (after =>)
    key_match = KEY_HEX_IN_QUOTES.search(line)
    val_match = HEX_AFTER_ARROW.search(line)
    key_hex = key_match.group(1) if key_match else None
    val_hex = val_match.group(1) if val_match else None
    return key_hex, val_hex


def decode_value_double(val_hex):
    # Expect val_hex encodes 12 bytes; last 8 bytes are big-endian double
    if val_hex is None:
        return None
    b = bytes.fromhex(val_hex)
    if len(b) < 8:
        raise ValueError("value bytes too short")
    # use the last 8 bytes as big-endian double
    dbl = struct.unpack(">d", b[-8:])[0]
    return dbl


def main(path):
    p = Path(path)
    if not p.exists():
        print("Input file not found:", path)
        sys.exit(1)

    out_dir = p.parent
    ints_path = out_dir / "extracted_integers.txt"
    dbls_path = out_dir / "extracted_doubles.txt"
    csv_path = out_dir / "mapping.csv"

    entries = []
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln:
                continue
            key_hex, val_hex = parse_line(ln)
            if not val_hex:
                # skip lines without value hex
                continue
            try:
                val_dbl = decode_value_double(val_hex)
            except Exception as e:
                # record error and continue
                val_dbl = None
            # if double is essentially integer, record integer
            val_int = ""
            if val_dbl is not None:
                # tolerance for floating rounding
                rounded = round(val_dbl)
                if abs(val_dbl - rounded) < 1e-9:
                    val_int = int(rounded)
            entries.append((key_hex or "", val_dbl, val_int))

    # write outputs
    with (
        ints_path.open("w", encoding="utf-8") as f_int,
        dbls_path.open("w", encoding="utf-8") as f_dbl,
        csv_path.open("w", newline="", encoding="utf-8") as f_csv,
    ):
        writer = csv.writer(f_csv)
        writer.writerow(["key_hex", "value_double", "value_int_if_integer"])
        for key_hex, val_dbl, val_int in entries:
            # doubles: full repr
            f_dbl.write(f"{val_dbl!r}\n")
            # integers: only write if we inferred an integer
            if val_int != "":
                f_int.write(f"{val_int}\n")
            writer.writerow(
                [
                    key_hex,
                    "" if val_dbl is None else repr(val_dbl),
                    "" if val_int == "" else str(val_int),
                ]
            )

    print("Wrote:", ints_path)
    print("Wrote:", dbls_path)
    print("Wrote:", csv_path)
    print("Entries processed:", len(entries))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 deserialize_sst_dump.py /path/to/sst_dump_output.txt")
        sys.exit(1)
    main(sys.argv[1])
