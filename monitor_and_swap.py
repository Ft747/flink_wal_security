#!/usr/bin/env python3
"""
monitor_and_swap.py

Recursively monitor a RocksDB directory for new or modified .sst files.
When an .sst file is detected, run the swap_sst_last5 program on it.
"""

import subprocess
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Path to the compiled C++ program
SWAP_PROGRAM = "./swap_sst_last5"

# Root RocksDB directory to monitor
WATCH_PATH = "/tmp/rocksdb"


def swap_last5(file_path: Path):
    """Run swap_sst_last5 on the given SST file."""
    try:
        print(f"[INFO] Swapping last 5 values in: {file_path}")
        subprocess.run(
            [SWAP_PROGRAM, str(file_path)],
            check=True,
        )
        print(f"[OK] Finished processing {file_path}")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] swap_sst_last5 failed on {file_path}: {e}")
    except FileNotFoundError:
        print(f"[ERROR] swap_sst_last5 not found at {SWAP_PROGRAM}")


class SSTEventHandler(FileSystemEventHandler):
    """Event handler for SST file creation/modification."""

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".sst"):
            swap_last5(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".sst"):
            swap_last5(Path(event.src_path))


def main():
    root = Path(WATCH_PATH)
    if not root.exists():
        print(f"[ERROR] Directory {WATCH_PATH} does not exist.")
        return

    event_handler = SSTEventHandler()
    observer = Observer()
    observer.schedule(event_handler, str(root), recursive=True)

    print(f"[INFO] Monitoring {WATCH_PATH} for SST changes...")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping observer...")
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
