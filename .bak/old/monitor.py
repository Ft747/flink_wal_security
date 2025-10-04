#!/usr/bin/env python3
"""
Directory Monitor and Sync Tool
Monitors a temporary directory and syncs files to a destination directory
before they are deleted.
"""

import os
import shutil
import time
import threading
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import argparse
import logging
import hashlib
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DirectorySyncHandler(FileSystemEventHandler):
    def __init__(self, temp_dir, sync_dir, sync_delay=0.1, scan_interval=0.1):
        self.temp_dir = Path(temp_dir)
        self.sync_dir = Path(sync_dir)
        self.sync_delay = sync_delay
        self.scan_interval = scan_interval

        # Track files and their states
        self.tracked_files = {}  # file_path -> {'last_modified', 'size', 'synced', 'last_seen'}
        self.file_locks = defaultdict(threading.Lock)
        self.global_lock = threading.Lock()

        # Create sync directory if it doesn't exist
        self.sync_dir.mkdir(parents=True, exist_ok=True)

        # Initial scan of existing files
        self._initial_scan()

        # Start background threads
        self.running = True
        self.scan_thread = threading.Thread(target=self._scan_worker, daemon=True)
        self.sync_thread = threading.Thread(target=self._sync_worker, daemon=True)
        self.scan_thread.start()
        self.sync_thread.start()

    def _get_file_info(self, file_path):
        """Get file modification time and size"""
        try:
            stat = os.stat(file_path)
            return stat.st_mtime, stat.st_size
        except (OSError, FileNotFoundError):
            return None, None

    def _initial_scan(self):
        """Scan existing files in temp directory"""
        logger.info("Performing initial scan...")
        for file_path in self.temp_dir.rglob("*"):
            if file_path.is_file():
                self._track_file(str(file_path))
        logger.info(f"Initial scan complete. Found {len(self.tracked_files)} files.")

    def _track_file(self, file_path):
        """Add or update file in tracking"""
        mtime, size = self._get_file_info(file_path)
        if mtime is None:
            return

        current_time = time.time()
        path_str = str(file_path)

        with self.global_lock:
            if path_str not in self.tracked_files:
                self.tracked_files[path_str] = {
                    "last_modified": mtime,
                    "size": size,
                    "synced": False,
                    "last_seen": current_time,
                    "stable_since": current_time,
                }
                logger.debug(f"Started tracking: {Path(file_path).name}")
            else:
                old_info = self.tracked_files[path_str]
                if old_info["last_modified"] != mtime or old_info["size"] != size:
                    # File changed
                    self.tracked_files[path_str].update(
                        {
                            "last_modified": mtime,
                            "size": size,
                            "synced": False,
                            "stable_since": current_time,
                        }
                    )
                    logger.debug(f"File changed: {Path(file_path).name}")

                self.tracked_files[path_str]["last_seen"] = current_time

    def on_any_event(self, event):
        """Handle all file system events"""
        if not event.is_directory and not event.src_path.startswith("."):
            self._track_file(event.src_path)

            # Also handle moved files
            if hasattr(event, "dest_path") and event.dest_path:
                self._track_file(event.dest_path)

    def _scan_worker(self):
        """Periodically scan directory for missed files"""
        while self.running:
            try:
                current_files = set()
                for file_path in self.temp_dir.rglob("*"):
                    if file_path.is_file():
                        current_files.add(str(file_path))
                        self._track_file(str(file_path))

                # Remove files that no longer exist
                with self.global_lock:
                    to_remove = []
                    for tracked_path in self.tracked_files:
                        if tracked_path not in current_files:
                            to_remove.append(tracked_path)

                    for path in to_remove:
                        del self.tracked_files[path]
                        logger.debug(
                            f"Stopped tracking deleted file: {Path(path).name}"
                        )

            except Exception as e:
                logger.error(f"Error in scan worker: {e}")

            time.sleep(0.1)

    def _sync_worker(self):
        """Process files that need syncing"""
        while self.running:
            try:
                current_time = time.time()
                files_to_sync = []

                with self.global_lock:
                    for file_path, info in self.tracked_files.items():
                        # Sync if file is stable and not yet synced
                        if (
                            not info["synced"]
                            and current_time - info["stable_since"] >= self.sync_delay
                            and os.path.exists(file_path)
                        ):
                            files_to_sync.append(file_path)

                # Sync files outside the global lock
                for file_path in files_to_sync:
                    self._sync_file(file_path)

            except Exception as e:
                logger.error(f"Error in sync worker: {e}")

            time.sleep(0.1)

    def _sync_file(self, src_path):
        """Sync a single file to the destination directory"""
        with self.file_locks[src_path]:
            try:
                src = Path(src_path)
                if not src.exists():
                    return

                # Calculate relative path from temp directory
                rel_path = src.relative_to(self.temp_dir)
                dest_path = self.sync_dir / rel_path

                # Create destination directory if needed
                dest_path.parent.mkdir(parents=True, exist_ok=True)

                # Check if we need to copy (file doesn't exist or is different)
                need_copy = True
                if dest_path.exists():
                    src_stat = src.stat()
                    dest_stat = dest_path.stat()
                    if (
                        src_stat.st_size == dest_stat.st_size
                        and abs(src_stat.st_mtime - dest_stat.st_mtime) < 1
                    ):
                        need_copy = False

                if need_copy:
                    # Copy file with retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            shutil.copy2(src, dest_path)
                            break
                        except (PermissionError, OSError) as e:
                            if attempt < max_retries - 1:
                                time.sleep(0.1 * (attempt + 1))
                                continue
                            raise e

                    logger.info(f"Synced: {rel_path}")
                else:
                    logger.debug(f"Already synced: {rel_path}")

                # Mark as synced
                with self.global_lock:
                    if src_path in self.tracked_files:
                        self.tracked_files[src_path]["synced"] = True

            except Exception as e:
                logger.error(f"Error syncing {src_path}: {e}")

    def force_sync_all(self):
        """Force sync of all tracked files"""
        logger.info("Force syncing all files...")
        with self.global_lock:
            files_to_sync = list(self.tracked_files.keys())

        for file_path in files_to_sync:
            if os.path.exists(file_path):
                self._sync_file(file_path)

        logger.info("Force sync complete.")

    def stop(self):
        """Stop all worker threads"""
        self.running = False


def monitor_directory(temp_dir, sync_dir, sync_delay=0.1, scan_interval=0.1):
    """Monitor temp directory and sync files to destination"""
    temp_path = Path(temp_dir)
    sync_path = Path(sync_dir)

    if not temp_path.exists():
        logger.error(f"Temporary directory doesn't exist: {temp_dir}")
        return

    logger.info(f"Monitoring: {temp_path}")
    logger.info(f"Syncing to: {sync_path}")
    logger.info(f"Sync delay: {sync_delay}s, Scan interval: {scan_interval}s")

    # Set up file system watcher
    event_handler = DirectorySyncHandler(temp_dir, sync_dir, sync_delay, scan_interval)
    observer = Observer()
    observer.schedule(event_handler, str(temp_path), recursive=True)

    try:
        observer.start()
        logger.info("Directory monitoring started. Press Ctrl+C to stop.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Stopping directory monitor...")
        event_handler.force_sync_all()  # Sync remaining files
        event_handler.stop()
        observer.stop()

    observer.join()
    logger.info("Directory monitor stopped.")


def main():
    parser = argparse.ArgumentParser(description="Monitor and sync temporary directory")
    parser.add_argument("temp_dir", help="Temporary directory to monitor")
    parser.add_argument("sync_dir", help="Destination directory for syncing")
    parser.add_argument(
        "--delay", type=float, default=0.1, help="Sync delay in seconds (default: 0.1)"
    )
    parser.add_argument(
        "--scan-interval",
        type=float,
        default=0.1,
        help="Directory scan interval in seconds (default: 0.1)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    monitor_directory(args.temp_dir, args.sync_dir, args.delay, args.scan_interval)


if __name__ == "__main__":
    main()
