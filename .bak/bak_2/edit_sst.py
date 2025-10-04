#!/usr/bin/env python3
"""
Usage:
  python3 edit_sst.py input.sst target_key new_value [--key-hex] [--value-hex] --writer ./make_sst --out patched.sst

Requirements:
  - sst_dump binary (from RocksDB build) in PATH
  - make_sst helper compiled (above)
"""

import sys, subprocess, re, base64, tempfile, os


def run_sst_dump(path):
    p = subprocess.run(
        ["sst_dump", "--file", path, "--command=scan", "--output_hex"],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise SystemExit("sst_dump failed:\n" + p.stderr)
    return p.stdout.splitlines()


# parse lines like:  'Key' @ 5: 1 => Value
line_re = re.compile(r"^'(?P<k>.*)'\s+@\s+\d+:\s+\d+\s+=>\s+(?P<v>.*)$")


def parse_entries(lines):
    out = []
    for L in lines:
        m = line_re.match(L.strip())
        if not m:
            # skip other lines
            continue
        key = m.group("k")
        val = m.group("v")
        # when --output_hex used, keys/vals are hex like 0x6162...
        if key.startswith("0x"):
            key = bytes.fromhex(key[2:])
        else:
            key = key.encode("utf-8", errors="surrogateescape")
        if val.startswith("0x"):
            val = bytes.fromhex(val[2:])
        else:
            val = val.encode("utf-8", errors="surrogateescape")
        out.append((key, val))
    return out


def worker(input_sst, target_key, new_value, key_hex, value_hex, writer, out_sst):
    lines = run_sst_dump(input_sst)
    entries = parse_entries(lines)
    # normalize target_key/new_value
    if key_hex:
        target_key_bytes = bytes.fromhex(target_key)
    else:
        target_key_bytes = target_key.encode()
    if value_hex:
        new_value_bytes = bytes.fromhex(new_value)
    else:
        new_value_bytes = new_value.encode()

    replaced = 0
    b64_lines = []
    for k, v in entries:
        if k == target_key_bytes:
            v = new_value_bytes
            replaced += 1
        b64_lines.append(
            base64.b64encode(k).decode("ascii")
            + "\t"
            + base64.b64encode(v).decode("ascii")
        )

    if replaced == 0:
        print(
            "Warning: key not found in SST (0 replacements). Continuing to write new SST with same content."
        )
    # stream into make_sst
    p = subprocess.Popen([writer, out_sst], stdin=subprocess.PIPE, text=True)
    for ln in b64_lines:
        p.stdin.write(ln + "\n")
    p.stdin.close()
    ret = p.wait()
    if ret != 0:
        raise SystemExit("make_sst failed (exit %d)" % ret)
    # verify
    chk = subprocess.run(
        ["sst_dump", "--file", out_sst, "--command=check", "--verify_checksum"],
        capture_output=True,
        text=True,
    )
    if chk.returncode != 0:
        print("Verification failed:\n", chk.stderr)
    else:
        print("Patched SST written to", out_sst, " (replacements:", replaced, ")")


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            "Usage: edit_sst.py input.sst target_key new_value --writer ./make_sst --out patched.sst [--key-hex] [--value-hex]"
        )
        sys.exit(2)
    input_sst = sys.argv[1]
    target_key = sys.argv[2]
    new_value = sys.argv[3]
    key_hex = "--key-hex" in sys.argv
    value_hex = "--value-hex" in sys.argv
    # find writer arg
    try:
        writer_idx = sys.argv.index("--writer")
        writer = sys.argv[writer_idx + 1]
    except ValueError:
        writer = "./make_sst"
    try:
        out_idx = sys.argv.index("--out")
        out_sst = sys.argv[out_idx + 1]
    except ValueError:
        out_sst = "patched.sst"
    worker(input_sst, target_key, new_value, key_hex, value_hex, writer, out_sst)
