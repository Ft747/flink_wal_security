import struct
import subprocess
import re

LINE_RE = re.compile(
    r"'(?P<key>[0-9A-Fa-f]+)'\s+seq:(?P<seq>\d+),\s+type:(?P<type>\d+)\s+=>\s+(?P<value>[0-9A-Fa-f]+)"
)

# LINE_RE = re.compile(r"'([0-9A-Fa-f]+)'.*=>\s*([0-9A-Fa-f]+)")


def parse_line(line: str):
    """
    Parse one line of sst_dump --output_hex output.

    Returns dict with key, seq, type, value (all as strings/ints).
    Returns None if the line doesn't match.
    """
    m = LINE_RE.search(line)
    if not m:
        print("None")
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
    return struct.pack(fmt, val).hex()


def dump_sst(filepath):
    return subprocess.run(
        ["sst_dump", f"--file={filepath}", "--command=scan", "--output_hex"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def replace(filepath, key, new):
    subprocess.run(["./new_swap_sst", f"{filepath}", "changed.sst", f"{key}", f"{new}"])


def main():
    file = "/home/dtome/Documents/School/Thesis/code/copy/job_5ec66a77154b2967fb59d23ffb69676b_op_ExternalPythonKeyedProcessOperator_90bea66de1c231edf33913ecd54406c1__1_1__uuid_2a360df5-869d-4c8c-9487-bdd0cacdb981/db/000022.sst"
    dump = dump_sst(file)
    # print(dump.splitlines())  # print first 200 chars to preview
    parsed = []
    for line in dump.splitlines():
        entry = parse_line(line)
        if entry:
            parsed.append(entry)

    for e in parsed:  # show first 5
        replace(
            file,
            e["key_hex"],
            reverse_convert(">d", 100),
        )
        # print(convert(">d", e["value_hex"]))

        # pass
    print(convert(">d", parsed[188]["value_hex"]))
    dump2 = dump_sst("changed.sst")
    parsed2 = []
    for line in dump2.splitlines():
        entry = parse_line(line)
        if entry:
            parsed2.append(entry)
    for e in parsed2[:5]:
        print(convert(">d", e["value_hex"]))
        print(e["key_hex"], e["value_hex"])


if __name__ == "__main__":
    main()
