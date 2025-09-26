import struct
import os
import subprocess
import codecs

import re


def locate_key(sst_file, dump_lines):
    """
    Parse sst_dump --command=raw output and locate key/value blocks in SST file.
    """
    block_pattern = re.compile(r"offset (\d+) size (\d+)")
    blocks = []

    for line in dump_lines:
        match = block_pattern.search(line)
        if match:
            offset, size = map(int, match.groups())
            blocks.append((offset, size))

    print(f"Found {len(blocks)} data blocks")

    results = []
    with open(sst_file, "rb") as f:
        print(f.read(200))
        for offset, size in blocks:
            f.seek(offset)
            raw_block = f.read(size)
            # Placeholder: actual key/value parsing requires RocksDB block decoding
            results.append((offset, size, raw_block[:32]))  # show first 32 bytes

    return results


def create_raw_dump(sstfile):
    subprocess.run(["sst_dump", f"--file={sstfile}", "--command=raw"])


def dump_to_txt(file):
    with open(file, "rb") as f:
        binary_data = f.readlines()

    # text = []
    text = [
        i.decode("cp1252", errors="replace") for i in binary_data
    ]  # or errors="ignore"
    return text


def dump_sst_to_txt(file):
    with open(file, "rb") as f:
        data = f.read().hex()
    return data


def main():
    file = "./output.sst"
    sst_dump = dump_to_txt(file)

    blocks = locate_key(file, sst_dump)

    for off, size, preview in blocks[:5]:
        print(f"Block at {off} (size {size}), preview: {preview.hex()}")
    # print(sst_dump[-10])
    # print(dump_sst_to_txt("output.sst"))


if __name__ == "__main__":
    main()
