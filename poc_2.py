import re
import subprocess
import sys
import os


def parse_sst_dump(dump_output):
    """
    Parse sst_dump --command=raw output to extract key-value pairs from Data Block section.
    """
    key_value_pairs = []
    # Pattern to match Data Block section
    data_block_pattern = re.compile(
        r"Data Block # \d+ @ \w+\n[-]+\n((?:.*?\n)+?)(?=\n[-]+\n|$)", re.MULTILINE
    )
    # Pattern to match key-value pairs
    kv_pattern = re.compile(
        r"\s*HEX\s+([0-9a-fA-F]+):\s*([0-9a-fA-F]+)\n\s*ASCII\s+([^\n]*)", re.MULTILINE
    )

    # Debugging: Print the raw dump output
    print("Raw sst_dump output:")
    print(dump_output[:500] + "..." if len(dump_output) > 500 else dump_output)
    print("-" * 50)

    # Find Data Block section
    data_block_match = data_block_pattern.search(dump_output)
    if not data_block_match:
        print("No Data Block section found in sst_dump output.")
        return key_value_pairs

    data_block_content = data_block_match.group(1)
    print("Data Block content:")
    print(data_block_content)
    print("-" * 50)

    # Find all key-value pairs within the Data Block
    for match in kv_pattern.finditer(data_block_content):
        hex_key, hex_value, ascii_line = match.groups()
        try:
            # Decode hex to ASCII for key and value
            key = bytes.fromhex(hex_key).decode("ascii", errors="ignore")
            value = bytes.fromhex(hex_value).decode("ascii", errors="ignore")
            key_value_pairs.append((key, value))
        except ValueError as e:
            print(
                f"Error decoding hex at key: {hex_key}, value: {hex_value}. Error: {e}"
            )

    return key_value_pairs


def create_raw_dump(sst_file):
    """
    Run sst_dump --command=raw and read the output from the generated file.
    """
    dump_file = "output_dump.txt"
    try:
        # Run sst_dump, which writes to dump_file
        result = subprocess.run(
            ["sst_dump", f"--file={sst_file}", "--command=raw"],
            capture_output=True,
            text=True,
            check=True,
        )
        # Read the output from the dump file
        if os.path.exists(dump_file):
            with open(dump_file, "r") as f:
                return f.read()
        else:
            print(f"Error: Dump file '{dump_file}' was not created.")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Error running sst_dump: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print(
            "sst_dump tool not found. Ensure RocksDB is installed and sst_dump is in your PATH."
        )
        sys.exit(1)


def main():
    sst_file = "./output.sst"
    # Verify file exists
    if not os.path.exists(sst_file):
        print(f"Error: SST file '{sst_file}' not found.")
        sys.exit(1)

    # Get the raw dump output from the file
    dump_output = create_raw_dump(sst_file)

    # Parse key-value pairs
    key_value_pairs = parse_sst_dump(dump_output)

    # Print results
    print(f"Found {len(key_value_pairs)} key-value pairs:")
    for key, value in key_value_pairs:
        print(f"Key: {key}, Value: {value}")


if __name__ == "__main__":
    main()
