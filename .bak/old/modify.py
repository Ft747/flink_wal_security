import struct
import subprocess
import re
import shutil
import os

LINE_RE = re.compile(
    r"'(?P<key>[0-9A-Fa-f]+)'.*?seq:(?P<seq>\d+).*?type:(?P<type>\d+).*?=>\s*(?P<value>[0-9A-Fa-f]+)"
)


def parse_line(line: str):
    """Parse one line of sst_dump --output_hex output."""
    m = LINE_RE.search(line)
    if not m:
        return None
    return {
        "key_hex": m.group("key"),
        "seq": int(m.group("seq")),
        "type": int(m.group("type")),
        "value_hex": m.group("value"),
    }


def convert(fmt, hex_str):
    decoded = bytes.fromhex(hex_str)
    if len(decoded) < 8:
        raise ValueError("Not enough bytes to unpack")
    return struct.unpack(fmt, decoded[-8:])[0]


def reverse_convert(fmt, val):
    # explicit float for >d; caller should pass float for doubles
    return struct.pack(fmt, val).hex()


def dump_sst(filepath):
    return subprocess.run(
        ["sst_dump", f"--file={filepath}", "--command=scan", "--output_hex"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def replace(in_path, out_path, key_hex, new_value_hex):
    """Call the compiled new_swap_sst program to rewrite in_path -> out_path replacing key_hex => new_value_hex."""
    cmd = ["./new_swap_sst", in_path, out_path, key_hex, new_value_hex]
    # raise on error to avoid silent failures
    subprocess.run(cmd, check=True)


def main():
    file = "/home/dtome/Documents/School/Thesis/code/copy/job_5ec66a77154b2967fb59d23ffb69676b_op_ExternalPythonKeyedProcessOperator_90bea66de1c231edf33913ecd54406c1__1_1__uuid_2a360df5-869d-4c8c-9487-bdd0cacdb981/db/000022.sst"

    dump = dump_sst(file)
    parsed = []
    for line in dump.splitlines():
        entry = parse_line(line)
        if entry:
            parsed.append(entry)

    if not parsed:
        print("No parseable entries found in dump.")
        return

    # temp files we will alternate between so we never overwrite the current input.
    tmp_a = "changed_a.sst"
    tmp_b = "changed_b.sst"

    # ensure old tmp files removed
    for p in (tmp_a, tmp_b):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    cur_in = file
    toggle = True
    replaced_count = 0

    # Example: we replace every value with the double 100.0
    for i, e in enumerate(parsed):
        out = tmp_a if toggle else tmp_b
        # build new_value as big-endian double hex
        new_val_hex = reverse_convert(">d", float(100.0))
        try:
            replace(cur_in, out, e["key_hex"], new_val_hex)
        except subprocess.CalledProcessError as err:
            print(f"Replacement failed on iteration {i} for key {e['key_hex']}: {err}")
            return

        # swap so next iteration uses the output we just created
        cur_in = out
        toggle = not toggle
        replaced_count += 1

    print(f"Applied {replaced_count} replacements; final SST is: {cur_in}")

    # show one example decode from the final SST
    final_dump = dump_sst(cur_in)
    parsed_final = []
    for line in final_dump.splitlines():
        entry = parse_line(line)
        if entry:
            parsed_final.append(entry)

    # try to decode the value of the 0-th entry as a double
    if parsed_final:
        try:
            print(
                "decoded example (first entry as >d):",
                convert(">d", parsed_final[0]["value_hex"]),
            )
            print(parsed_final[0]["key_hex"], parsed_final[0]["value_hex"])
        except Exception as ex:
            print("decoding example failed:", ex)


if __name__ == "__main__":
    main()
