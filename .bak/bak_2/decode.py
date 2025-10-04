#!/usr/bin/env python3
"""
deserialize_sst_dump_with_keys.py

Usage:
    python3 deserialize_sst_dump_with_keys.py /path/to/sst_dump_output.txt

Outputs (in same directory as input):
    - extracted_integers.txt
    - extracted_doubles.txt
    - decoded_keys.txt
    - extracted_counts.txt (count_state values)
    - extracted_totals.txt (total_state values)
    - extracted_averages.txt (avg_state values)
    - mapping.csv   (key_hex, key_decoded, value_double, value_int_or_empty, state_type)
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


def decode_key(key_hex):
    """
    Decode the key from hex string.
    Based on your SST dump, keys appear to follow a pattern.
    This function attempts multiple decoding strategies.
    """
    if key_hex is None:
        return None

    try:
        key_bytes = bytes.fromhex(key_hex)

        # Strategy 1: Try to extract integer from first few bytes
        if len(key_bytes) >= 4:
            # Try big-endian int from first 4 bytes
            int_val = struct.unpack(">I", key_bytes[:4])[0]
            if int_val < 1000000:  # reasonable range check
                return f"int:{int_val}"

        # Strategy 2: Try to extract from specific positions
        # Your keys seem to have a pattern like '01000000000580054B162E00'
        # The first byte might be the actual key value
        if len(key_bytes) >= 1:
            first_byte = key_bytes[0]
            return f"byte:{first_byte}"

        # Strategy 3: Return as hex if other methods fail
        return f"hex:{key_hex}"

    except Exception as e:
        return f"error:{e}"


def decode_value_double(val_hex):
    # RocksDB values appear to be 12 bytes: 4 bytes prefix + 8 bytes double
    if val_hex is None:
        return None
    try:
        b = bytes.fromhex(val_hex)
        if len(b) < 8:
            raise ValueError(f"Value too short: {len(b)} bytes")

        # Try different approaches to decode the value
        if len(b) == 12:
            # Skip first 4 bytes, use last 8 as big-endian double
            dbl = struct.unpack(">d", b[4:])[0]
            return dbl
        elif len(b) == 8:
            # Direct 8-byte double
            dbl = struct.unpack(">d", b)[0]
            return dbl
        else:
            # Try using last 8 bytes
            dbl = struct.unpack(">d", b[-8:])[0]
            return dbl

    except Exception as e:
        # If double decoding fails, try as long integer
        try:
            b = bytes.fromhex(val_hex)
            if len(b) >= 8:
                # Try as big-endian long from last 8 bytes
                int_val = struct.unpack(">Q", b[-8:])[0]
                return float(int_val)
        except Exception:
            pass
        return None


def identify_state_type(key_hex, value):
    """
    Identify state type based on key structure.
    RocksDB uses different key prefixes for different ValueState descriptors.
    The key structure likely encodes the state descriptor name.
    """
    if key_hex is None or value is None:
        return "unknown"

    # In RocksDB, different ValueState descriptors get different key prefixes
    # Let's analyze the key structure to identify the state type

    # The key format appears to be: [user_key][state_descriptor_info][timestamps/metadata]
    # We need to look at the middle part that identifies the state descriptor

    # For now, let's use a different approach based on value patterns and key analysis
    # Since your code processes numbers 0-199, and you're computing rolling averages:

    # Look at the bytes after the first byte (which is the user key)
    try:
        key_bytes = bytes.fromhex(key_hex)
        if len(key_bytes) >= 12:
            # Extract some identifying bytes from the middle of the key
            state_identifier = key_bytes[4:8].hex()  # bytes 4-7

            # Different state descriptors should have different identifiers
            # This is a heuristic - you may need to adjust based on actual patterns
            if state_identifier.startswith("0005800"):
                # All seem to have this pattern, so let's use value analysis
                pass

        # Fallback to value-based analysis with better logic
        if isinstance(value, (int, float)):
            # Count should be small integers (1 to 200 for your data)
            if value == int(value) and 1 <= value <= 200:
                # But we need to distinguish count vs total vs average
                # Count: should be sequential-ish and smaller
                # Total: should be sum of numbers, so larger
                # Average: should be total/count, so between 0 and ~100

                # For numbers 0-199, if we've processed k numbers:
                # - count would be k (1 to 200)
                # - total would be sum of k numbers (could be 0 to sum(0..199) = 19900)
                # - average would be total/k (0 to 99.5)

                # This is tricky with just the value. Let's group by user key first
                user_key = key_bytes[0] if len(key_bytes) > 0 else 0

                # For each user key, we expect 3 states. The values should be:
                # - One small integer (count): 1-200
                # - One larger number (total): sum of numbers
                # - One decimal (average): total/count

                # Since all your values are showing as integers, they might be counts
                # But let's check if some are actually totals stored as integers

                if value <= 200:
                    return "count_or_avg"  # Could be either
                else:
                    return "total_state"  # Likely total

        return "unknown"

    except Exception:
        return "unknown"


def main(path):
    p = Path(path)
    if not p.exists():
        print("Input file not found:", path)
        sys.exit(1)

    out_dir = p.parent
    ints_path = out_dir / "extracted_integers.txt"
    dbls_path = out_dir / "extracted_doubles.txt"
    keys_path = out_dir / "decoded_keys.txt"
    counts_path = out_dir / "extracted_counts.txt"
    totals_path = out_dir / "extracted_totals.txt"
    averages_path = out_dir / "extracted_averages.txt"
    csv_path = out_dir / "mapping.csv"

    entries = []
    debug_count = 0
    with p.open("r", encoding="utf-8", errors="replace") as fh:
        for ln in fh:
            ln = ln.strip()
            if (
                not ln
                or ln.startswith("options.")
                or ln.startswith("Process")
                or ln.startswith("Sst")
            ):
                continue

            key_hex, val_hex = parse_line(ln)
            if not key_hex or not val_hex:
                continue

            # Debug first few entries
            if debug_count < 5:
                print(f"Debug {debug_count}: key_hex='{key_hex}', val_hex='{val_hex}'")
                print(
                    f"  val_hex length: {len(val_hex)} chars, {len(bytes.fromhex(val_hex))} bytes"
                )
                debug_count += 1

            # Decode key and value
            key_decoded = decode_key(key_hex)
            val_dbl = decode_value_double(val_hex)
            state_type = identify_state_type(key_hex, val_dbl)

            if debug_count <= 5:
                print(f"  decoded value: {val_dbl}, state_type: {state_type}")

            # if double is essentially integer, record integer
            val_int = ""
            if val_dbl is not None:
                rounded = round(val_dbl)
                if abs(val_dbl - rounded) < 1e-9:
                    val_int = int(rounded)

            entries.append((key_hex, key_decoded, val_dbl, val_int, state_type))

    # write outputs
    with (
        ints_path.open("w", encoding="utf-8") as f_int,
        dbls_path.open("w", encoding="utf-8") as f_dbl,
        keys_path.open("w", encoding="utf-8") as f_key,
        counts_path.open("w", encoding="utf-8") as f_count,
        totals_path.open("w", encoding="utf-8") as f_total,
        averages_path.open("w", encoding="utf-8") as f_avg,
        csv_path.open("w", newline="", encoding="utf-8") as f_csv,
    ):
        writer = csv.writer(f_csv)
        writer.writerow(
            [
                "key_hex",
                "key_decoded",
                "value_double",
                "value_int_if_integer",
                "state_type",
            ]
        )

        for key_hex, key_decoded, val_dbl, val_int, state_type in entries:
            # decoded keys
            f_key.write(f"{key_decoded}\n")

            # doubles: full repr
            if val_dbl is not None:
                f_dbl.write(f"{val_dbl!r}\n")

            # integers: only write if we inferred an integer
            if val_int != "":
                f_int.write(f"{val_int}\n")

            # separate by state type
            if state_type == "count_state" and val_int != "":
                f_count.write(f"key_{key_decoded}:{val_int}\n")
            elif state_type == "total_state" and val_dbl is not None:
                f_total.write(f"key_{key_decoded}:{val_dbl!r}\n")
            elif state_type == "avg_state" and val_dbl is not None:
                f_avg.write(f"key_{key_decoded}:{val_dbl!r}\n")

            writer.writerow(
                [
                    key_hex,
                    key_decoded or "",
                    "" if val_dbl is None else repr(val_dbl),
                    "" if val_int == "" else str(val_int),
                    state_type,
                ]
            )

    print("Wrote:", ints_path)
    print("Wrote:", dbls_path)
    print("Wrote:", keys_path)
    print("Wrote:", counts_path)
    print("Wrote:", totals_path)
    print("Wrote:", averages_path)
    print("Wrote:", csv_path)
    print("Entries processed:", len(entries))

    # Group entries by user key to analyze patterns
    from collections import defaultdict

    key_groups = defaultdict(list)

    for key_hex, key_decoded, val_dbl, val_int, state_type in entries:
        user_key = key_hex[:2] if key_hex else "unknown"  # First byte as hex
        key_groups[user_key].append(
            (key_hex, key_decoded, val_dbl, val_int, state_type)
        )

    print(f"\nFound {len(key_groups)} unique user keys")

    # Analyze first few user keys to understand the pattern
    print("\nAnalyzing patterns by user key:")
    for i, (user_key, group) in enumerate(list(key_groups.items())[:5]):
        print(f"\nUser key {user_key} has {len(group)} entries:")
        for j, (key_hex, key_decoded, val_dbl, val_int, state_type) in enumerate(group):
            # Show key suffix to identify state types
            key_suffix = key_hex[16:] if len(key_hex) > 16 else key_hex[8:]
            print(f"  Entry {j}: suffix={key_suffix}, value={val_dbl}")

    # Try to re-classify based on patterns per user key
    print("\nRe-analyzing state types...")
    updated_entries = []

    for key_hex, key_decoded, val_dbl, val_int, old_state_type in entries:
        user_key = key_hex[:2] if key_hex else "unknown"
        user_entries = key_groups[user_key]

        if len(user_entries) >= 3:
            # Sort by value to identify count, total, average
            sorted_entries = sorted(
                user_entries, key=lambda x: x[2] if x[2] is not None else 0
            )

            # Find current entry's position in sorted list
            current_pos = -1
            for pos, entry in enumerate(sorted_entries):
                if entry[0] == key_hex:  # Match by key_hex
                    current_pos = pos
                    break

            # Classify based on position (smallest=count, middle=avg, largest=total)
            if current_pos == 0:
                new_state_type = "count_state"
            elif current_pos == len(sorted_entries) - 1:
                new_state_type = "total_state"
            else:
                new_state_type = "avg_state"
        else:
            new_state_type = "single_entry"

        updated_entries.append((key_hex, key_decoded, val_dbl, val_int, new_state_type))

    entries = updated_entries

    # Print summary by state type
    state_counts = {}
    for _, _, _, _, state_type in entries:
        state_counts[state_type] = state_counts.get(state_type, 0) + 1

    print("\nState type distribution:")
    for state_type, count in state_counts.items():
        print(f"  {state_type}: {count}")

    # Print some sample decoded entries for verification
    print("\nSample decoded entries:")
    for i, (key_hex, key_decoded, val_dbl, val_int, state_type) in enumerate(
        entries[:10]
    ):
        print(f"  {key_hex} -> key:{key_decoded}, value:{val_dbl}, type:{state_type}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: python3 deserialize_sst_dump_with_keys.py /path/to/sst_dump_output.txt"
        )
        sys.exit(1)
    main(sys.argv[1])
