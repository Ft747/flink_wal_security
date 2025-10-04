#!/usr/bin/env python3
# monitor_sst.py - Enhanced to wait until SST files are fully written

import os
import time
import subprocess
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Path to your compiled C++ program
SWAP_PROGRAM = "./replace_last_5_sst"

# Directory to monitor
CHECKPOINT_DIR = "/home/dtome/Code/flink-2.1.0/tmp/rocksdb/job_74455e44c3dbc0a91d785d64e46684a8_op_ExternalPythonKeyedProcessOperator_b84c3b1a0976a0b811acdcb91813802d__1_1__uuid_ff17546a-e7ca-4d9f-803d-b3dedb5390fa/db"

# Wait this many seconds after last modification before processing
STABILITY_DELAY = 2  # seconds


class SSTHandler(FileSystemEventHandler):
    def __init__(self):
        self.pending = {}  # path -> last modified time
        self.processed = set()

    def on_created(self, event):
        self._on_file_event(event)

    def on_modified(self, event):
        self._on_file_event(event)

    def _on_file_event(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith(".sst"):
            return

        # Skip if already processed
        if event.src_path in self.processed:
            return

        try:
            # Get current file size and mtime
            stat = os.stat(event.src_path)
            current_size = stat.st_size
            current_mtime = stat.st_mtime

            # Ignore zero-byte files
            if current_size == 0:
                # Reset timer if file is still empty
                self.pending[event.src_path] = time.time()
                return

            now = time.time()

            # If file is new or was modified recently, update its "pending" timestamp
            if event.src_path not in self.pending:
                print(
                    f"🆕 New non-empty SST detected: {event.src_path} ({current_size} bytes)"
                )
                self.pending[event.src_path] = now
            else:
                # File was modified — reset the stability timer
                if (
                    current_mtime > self.pending[event.src_path] - 1
                ):  # avoid clock skew issues
                    print(f"♻️  SST modified: {event.src_path}, resetting timer")
                    self.pending[event.src_path] = now

        except FileNotFoundError:
            # File may have been deleted already — ignore
            self.pending.pop(event.src_path, None)
            return

    def check_pending_files(self):
        now = time.time()
        to_remove = []

        for sst_path, last_event_time in self.pending.items():
            try:
                # Re-check file size and mtime
                stat = os.stat(sst_path)
                if stat.st_size == 0:
                    continue  # still empty, wait

                # If file hasn’t changed for STABILITY_DELAY seconds, process it
                if now - last_event_time >= STABILITY_DELAY:
                    print(f"✅ File appears stable: {sst_path}")
                    self.process_sst(sst_path)
                    self.processed.add(sst_path)
                    to_remove.append(sst_path)

            except FileNotFoundError:
                print(f"⚠️  File disappeared before processing: {sst_path}")
                to_remove.append(sst_path)
            except Exception as e:
                print(f"❌ Error checking {sst_path}: {e}")
                to_remove.append(sst_path)

        # Clean up
        for path in to_remove:
            self.pending.pop(path, None)

    def process_sst(self, sst_path):
        # Create temp output path
        out_path = sst_path + ".tmp"

        try:
            result = subprocess.run(
                [SWAP_PROGRAM, sst_path, out_path],
                check=True,
                capture_output=True,
                text=True,
            )
            print(f"✅ Successfully processed: {sst_path}")

            # Replace original file
            os.replace(out_path, sst_path)
            print(f"♻️  Replaced original: {sst_path}")

        except subprocess.CalledProcessError as e:
            print(f"❌ Processing failed for {sst_path}: {e}")
            print(e.stderr)
            # Clean up temp file if exists
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception as e:
            print(f"❌ Unexpected error processing {sst_path}: {e}")
            if os.path.exists(out_path):
                os.remove(out_path)


if __name__ == "__main__":
    if not os.path.exists(SWAP_PROGRAM):
        print(f"Error: {SWAP_PROGRAM} not found. Compile replace_last_5_sst.cpp first.")
        sys.exit(1)

    if not os.path.exists(CHECKPOINT_DIR):
        print(f"Error: Checkpoint directory {CHECKPOINT_DIR} does not exist.")
        sys.exit(1)

    event_handler = SSTHandler()
    observer = Observer()
    observer.schedule(event_handler, CHECKPOINT_DIR, recursive=False)
    observer.start()

    print(
        f"📁 Monitoring {CHECKPOINT_DIR} for new .sst files (waiting {STABILITY_DELAY}s after last write)..."
    )
    try:
        while True:
            event_handler.check_pending_files()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping monitor...")
        observer.stop()
    observer.join()
