#!/usr/bin/env python3
"""
parse_sst_fixed.py

Parse `sst_dump --output_hex` output and reconstruct per-user-key state.
- Uses BIG-ENDIAN decoding for Flink-serialized primitives (Java DataOutput semantics).
- Heuristically extracts the user key from the RocksDB key bytes by searching
  near the end of the key for an integer within [0, max_user_key].
References:
 - Flink stores RocksDB keys as serialized <KeyGroup, Key, Namespace>. See Ververica/Flink docs.
 - Flink DataOutputViewStreamWrapper/DataOutputStream uses Java DataOutput (big-endian).
   (This is why integers/doubles decode as big-endian).
"""

from pathlib import Path
import re
import struct
import json
import sys
from collections import defaultdict
from typing import List, Tuple, Optional

# Regex to match sst_dump lines like:
# '00000000000580054BD02E00' seq:1254, type:1 => 00000008406A000000000000
LINE_RE = re.compile(r"'([0-9A-Fa-f]+)'.*=>\s*([0-9A-Fa-f]+)")


def read_pairs(path: Path) -> List[Tuple[str, str]]:
    pairs = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = LINE_RE.search(line)
            if m:
                pairs.append((m.group(1), m.group(2)))
    return pairs


def decode_value_big_endian(val_hex: str):
    """Decode value bytes using Flink-style big-endian primitives.
    Heuristics:
      - 12 bytes => (4-byte unsigned int count, 8-byte double)
      - 4 bytes  => 4-byte unsigned int
      - 8 bytes  => 8-byte double
      - otherwise => return raw bytes (hex)
    """
    vb = bytes.fromhex(val_hex)
    L = len(vb)
    if L == 12:
        # count (uint32, big-endian) + double (big-endian)
        count = struct.unpack(">I", vb[0:4])[0]
        dbl = struct.unpack(">d", vb[4:12])[0]
        return {"kind": "count+double", "count": count, "double": dbl, "raw": val_hex}
    if L == 4:
        count = struct.unpack(">I", vb)[0]
        return {"kind": "count", "count": count, "raw": val_hex}
    if L == 8:
        dbl = struct.unpack(">d", vb)[0]
        return {"kind": "double", "double": dbl, "raw": val_hex}
    # fallback: unknown encoding
    return {"kind": "raw", "raw_hex": val_hex, "len": L}


def parse_key_bytes(key_hex: str) -> dict:
    kb = bytes.fromhex(key_hex)
    info = {}
    if len(kb) >= 4:
        # key-group stored in the first 4 bytes (big-endian unsigned int)
        info["key_group"] = struct.unpack(">I", kb[0:4])[0]
    else:
        info["key_group"] = None
    info["raw_key_bytes"] = kb
    info["raw_key_hex"] = key_hex
    return info


def find_user_key_from_tail(kb: bytes, max_user_key: int = 10000) -> Optional[int]:
    """Heuristic: search for a contiguous integer encoded in big-endian near the
    end of the key bytes that falls in [0, max_user_key]. Prefer longer matches
    and matches nearer the end of the key.
    """
    L = len(kb)
    candidates = []
    # check substrings up to 6 bytes ending anywhere; prefer substrings that end near the tail
    max_len = min(6, L)
    for length in range(1, max_len + 1):
        # try substrings that end at positions from L down to 0 (we prioritize tail)
        for end in range(L, 0, -1):
            start = end - length
            if start < 0:
                continue
            fragment = kb[start:end]
            # big-endian unsigned interpretation
            val = int.from_bytes(fragment, byteorder="big", signed=False)
            if 0 <= val <= max_user_key:
                # score: prefer fragments ending later and being longer
                score = (end, length)
                candidates.append((score, start, end, fragment, val))
    if not candidates:
        return None
    # pick best candidate by max score
    best = max(candidates, key=lambda x: x[0])
    return best[4]


def reconstruct_grouped(pairs: List[Tuple[str, str]], max_user_key: int = 10000):
    groups = defaultdict(
        lambda: {
            "key_group": None,
            "user_key": None,
            "count": None,
            "total": None,
            "avg": None,
            "raw": [],
        }
    )
    for key_hex, val_hex in pairs:
        kinfo = parse_key_bytes(key_hex)
        vinfo = decode_value_big_endian(val_hex)

        kb = kinfo["raw_key_bytes"]
        uk = find_user_key_from_tail(kb, max_user_key=max_user_key)
        # fallback: if not found, try last 8 bytes as signed big-endian long
        if uk is None and len(kb) >= 8:
            try:
                uk = struct.unpack(">q", kb[-8:])[0]
            except Exception:
                uk = None
        # fallback to using raw key hex as grouping key
        group_key = uk if uk is not None else key_hex

        rec = groups[group_key]
        if rec["key_group"] is None:
            rec["key_group"] = kinfo.get("key_group")
        if rec["user_key"] is None:
            rec["user_key"] = uk

        # merge decoded value heuristically
        if vinfo["kind"] == "count+double":
            rec["count"] = int(vinfo["count"])
            # often this double is the total or partial aggregate; save into total if empty
            if rec["total"] is None:
                rec["total"] = float(vinfo["double"])
            else:
                # if multiple doubles show up, keep last one in 'avg' or append to raw
                if rec["avg"] is None:
                    rec["avg"] = float(vinfo["double"])
                else:
                    rec["raw"].append(vinfo)
        elif vinfo["kind"] == "count":
            rec["count"] = int(vinfo["count"])
        elif vinfo["kind"] == "double":
            # assign to total or avg heuristically
            if rec["total"] is None:
                rec["total"] = float(vinfo["double"])
            elif rec["avg"] is None:
                rec["avg"] = float(vinfo["double"])
            else:
                rec["raw"].append(vinfo)
        else:
            rec["raw"].append(vinfo)

    # post-process: if avg missing, try compute from total/count
    for k, r in groups.items():
        if (
            r.get("avg") in (None, 0.0)
            and r.get("total") is not None
            and r.get("count") not in (None, 0)
        ):
            try:
                r["avg"] = float(r["total"]) / float(r["count"])
            except Exception:
                pass

    return groups


def main(argv):
    if len(argv) < 2:
        print(
            "Usage: parse_sst_fixed.py <sst_dump_output.txt> [-m max_user_key] [-o out.json]"
        )
        sys.exit(2)
    infile = Path(argv[1])
    max_user_key = 5000
    outfile = None
    if "-m" in argv:
        try:
            max_user_key = int(argv[argv.index("-m") + 1])
        except Exception:
            pass
    if "-o" in argv:
        try:
            outfile = Path(argv[argv.index("-o") + 1])
        except Exception:
            outfile = None

    pairs = read_pairs(infile)
    print(f"Extracted {len(pairs)} key-value pairs")

    grouped = reconstruct_grouped(pairs, max_user_key=max_user_key)

    # sort groups: numeric user_key first, fallback by hex string
    def sort_key(u):
        try:
            return (0, int(u))
        except Exception:
            return (1, str(u))

    ordered = [grouped[k] for k in sorted(grouped.keys(), key=sort_key)]

    # prune empty raw arrays for compactness
    for r in ordered:
        if not r.get("raw"):
            r.pop("raw", None)

    out = json.dumps(ordered, indent=2, default=lambda o: str(o))
    if outfile:
        outfile.write_text(out, encoding="utf-8")
        print(f"Wrote {len(ordered)} grouped records to {outfile}")
    else:
        print(out)


if __name__ == "__main__":
    main(sys.argv)
