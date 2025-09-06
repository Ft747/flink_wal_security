#!/usr/bin/env python3
"""
Parse `sst_dump --output_hex` text and produce grouped key/state records.

Usage:
    python3 parse_sst_dump.py sst_dump_output.txt [-o parsed.json]

Output: prints a summarized JSON array of records (one per user_key).
"""

import re
import struct
import json
import sys
from collections import defaultdict
from pathlib import Path

LINE_RE = re.compile(r"'([0-9A-Fa-f]+)'.*=>\s*([0-9A-Fa-f]+)")


def parse_key(key_hex: str):
    kb = bytes.fromhex(key_hex)
    key_group = struct.unpack("<I", kb[0:4])[0] if len(kb) >= 4 else None
    user_key = struct.unpack("<q", kb[-8:])[0] if len(kb) >= 8 else None
    return {"key_group": key_group, "user_key": user_key, "raw_key_hex": key_hex}


def parse_value(val_hex: str):
    vb = bytes.fromhex(val_hex)
    L = len(vb)
    # Heuristics based on observed encodings:
    # - 12 bytes: 4-byte unsigned int (count) + 8-byte double
    # - 4 bytes: 4-byte unsigned int (count)
    # - 8 bytes: 8-byte double
    # Otherwise return raw bytes hex
    if L == 12:
        count = struct.unpack("<I", vb[0:4])[0]
        dbl = struct.unpack("<d", vb[4:12])[0]
        return {"type": "count+double", "count": count, "double": dbl, "raw": val_hex}
    if L == 4:
        count = struct.unpack("<I", vb)[0]
        return {"type": "count", "count": count, "raw": val_hex}
    if L == 8:
        dbl = struct.unpack("<d", vb)[0]
        return {"type": "double", "double": dbl, "raw": val_hex}
    return {"type": "raw", "raw_bytes": val_hex, "len": L}


def group_pairs(pairs):
    # groups[user_key] -> aggregated record
    groups = defaultdict(
        lambda: {
            "key_group": None,
            "user_key": None,
            "count": None,
            "total": None,
            "avg": None,
            "doubles": [],
            "raw_entries": [],
        }
    )

    for key_hex, val_hex in pairs:
        kinfo = parse_key(key_hex)
        vinfo = parse_value(val_hex)
        uk = kinfo["user_key"]

        if uk is None:
            # fallback: use full key hex as key
            uk = key_hex

        rec = groups[uk]
        if rec["key_group"] is None:
            rec["key_group"] = kinfo["key_group"]
        if rec["user_key"] is None:
            rec["user_key"] = uk

        # place parsed value into appropriate field heuristically
        if vinfo["type"] == "count+double":
            # prefer to treat the double as `total` unless total already present
            rec["count"] = vinfo["count"]
            if rec["total"] is None:
                rec["total"] = vinfo["double"]
            else:
                rec["doubles"].append(vinfo["double"])
        elif vinfo["type"] == "count":
            rec["count"] = vinfo["count"]
        elif vinfo["type"] == "double":
            # try to assign as total then avg, otherwise append
            if rec["total"] is None:
                rec["total"] = vinfo["double"]
            elif rec["avg"] is None:
                rec["avg"] = vinfo["double"]
            else:
                rec["doubles"].append(vinfo["double"])
        else:
            rec["raw_entries"].append(vinfo)

    # Post-process: if avg is missing but we can compute avg = total/count
    for uk, r in groups.items():
        if r["avg"] is None and r["total"] is not None and r["count"] not in (None, 0):
            try:
                r["avg"] = float(r["total"]) / float(r["count"])
            except Exception:
                pass

    return groups


def read_sst_dump_file(path):
    pairs = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = LINE_RE.search(line)
            if m:
                pairs.append((m.group(1), m.group(2)))
    return pairs


def main(argv):
    if len(argv) < 2:
        print("Usage: parse_sst_dump.py <sst_dump_output.txt> [-o out.json]")
        sys.exit(2)

    infile = Path(argv[1])
    outfile = None
    if "-o" in argv:
        try:
            outfile = Path(argv[argv.index("-o") + 1])
        except IndexError:
            outfile = None

    pairs = read_sst_dump_file(infile)
    print(f"Extracted {len(pairs)} key-value pairs")

    grouped = group_pairs(pairs)

    # build list sorted by user_key where user_key is numeric when possible
    def key_sort(k):
        try:
            return (0, int(k))
        except Exception:
            return (1, str(k))

    ordered = [grouped[k] for k in sorted(grouped.keys(), key=key_sort)]

    # compact output: remove empty lists if empty
    for r in ordered:
        if not r["doubles"]:
            r.pop("doubles", None)
        if not r["raw_entries"]:
            r.pop("raw_entries", None)

    out_json = json.dumps(ordered, indent=2, default=lambda o: str(o))
    if outfile:
        outfile.write_text(out_json, encoding="utf-8")
        print(f"Wrote {len(ordered)} grouped records to {outfile}")
    else:
        print(out_json)


if __name__ == "__main__":
    main(sys.argv)
