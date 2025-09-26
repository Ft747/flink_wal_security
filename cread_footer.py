import subprocess
import json
import os
import sys


class SSTFooterReader:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_footer_with_sst_dump(self):
        """Use sst_dump to read SST file footer (recommended method)"""
        try:
            # Use the command that works for your sst_dump version
            cmd = ["sst_dump", "--file=" + self.file_path, "--show_properties"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            return self._parse_sst_dump_output(result.stdout)

        except subprocess.CalledProcessError as e:
            return {"error": f"sst_dump failed: {e.stderr}"}
        except FileNotFoundError:
            return {"error": "sst_dump not found. Install RocksDB tools first."}

    def _parse_sst_dump_output(self, output):
        """Parse sst_dump output to extract footer information"""
        footer_info = {}
        lines = output.split("\n")
        current_section = None

        for line in lines:
            line = line.strip()

            # Identify sections
            if "Footer Details:" in line:
                current_section = "footer"
                continue
            elif "Table Properties:" in line:
                current_section = "properties"
                continue
            elif "Metaindex Details:" in line:
                current_section = "metaindex"
                continue
            elif "Index Details:" in line:
                current_section = "index"
                continue

            # Skip section separators
            if line.startswith("---") or not line:
                continue

            # Parse key-value pairs
            if ":" in line and current_section:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()

                # Clean up key names
                clean_key = (
                    key.lower()
                    .replace(" ", "_")
                    .replace("#", "num")
                    .replace("(", "")
                    .replace(")", "")
                )

                if current_section not in footer_info:
                    footer_info[current_section] = {}

                footer_info[current_section][clean_key] = value

        # Extract key footer fields for easy access
        if "footer" in footer_info:
            footer_info["metaindex_offset"] = (
                footer_info["footer"].get("metaindex_handle", "").split()[0]
                if "metaindex_handle" in footer_info["footer"]
                else "N/A"
            )
            footer_info["index_offset"] = (
                footer_info["footer"].get("index_handle", "").split()[0]
                if "index_handle" in footer_info["footer"]
                else "N/A"
            )
            footer_info["magic_number"] = footer_info["footer"].get(
                "table_magic_number", "N/A"
            )
            footer_info["format_version"] = footer_info["footer"].get(
                "format_version", "N/A"
            )

        footer_info["raw_output"] = output
        return footer_info

    def verify_with_scan(self):
        """Verify file integrity by scanning contents"""
        try:
            cmd = ["sst_dump", "--file=" + self.file_path, "--command=verify"]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return {"verification": "PASSED", "output": result.stdout}
        except subprocess.CalledProcessError as e:
            return {"verification": "FAILED", "error": e.stderr}


def check_sst_dump_available():
    """Check if sst_dump is available"""
    try:
        subprocess.run(["sst_dump", "--help"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def main():
    if not check_sst_dump_available():
        print("Error: sst_dump not found!")
        print("Install RocksDB tools:")
        print("  Ubuntu/Debian: sudo apt install rocksdb-tools")
        print("  macOS: brew install rocksdb")
        print("  Or build from source: https://github.com/facebook/rocksdb")
        return

    file_path = input("Enter SST file path: ").strip()

    if not os.path.exists(file_path):
        print("File not found!")
        return

    reader = SSTFooterReader(file_path)

    print("\nReading SST Footer with sst_dump:")
    print("=" * 50)

    # Get footer info
    footer_info = reader.read_footer_with_sst_dump()

    if "error" in footer_info:
        print(footer_info["error"])
        return

    # Display parsed info
    if "error" in footer_info:
        print(footer_info["error"])
        return

    print("\nFooter Information:")
    print(f"Magic Number: {footer_info.get('magic_number', 'N/A')}")
    print(f"Format Version: {footer_info.get('format_version', 'N/A')}")
    print(f"Metaindex Offset: {footer_info.get('metaindex_offset', 'N/A')}")
    print(f"Index Offset: {footer_info.get('index_offset', 'N/A')}")

    if "properties" in footer_info:
        print(f"\nFile Statistics:")
        props = footer_info["properties"]
        print(f"Data Blocks: {props.get('num_data_blocks', 'N/A')}")
        print(f"Entries: {props.get('num_entries', 'N/A')}")
        print(f"Raw Key Size: {props.get('raw_key_size', 'N/A')} bytes")
        print(f"Raw Value Size: {props.get('raw_value_size', 'N/A')} bytes")
        print(f"Compression: {props.get('sst_file_compression_algo', 'N/A')}")

    # Verify file integrity
    print("\nVerifying file integrity:")
    verification = reader.verify_with_scan()
    print(f"Status: {verification.get('verification', 'UNKNOWN')}")
    if "error" in verification:
        print(f"Error: {verification['error']}")

    # Show raw output if requested
    show_raw = input("\nShow raw sst_dump output? (y/N): ").lower().startswith("y")
    if show_raw:
        print("\nRaw sst_dump output:")
        print("-" * 40)
        print(footer_info.get("raw_output", "No output available"))


if __name__ == "__main__":
    main()
