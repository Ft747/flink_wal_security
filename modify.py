import rocksdb
import struct
import os
import pyrex

INPUT_SST = "/home/dtome/Code/flink-2.1.0/tmp/copy/81-000023-81.sst"  # Path to your original SST
WORK_DB = "/home/dtome/Code/flink-2.1.0/tmp/copy/"  # Temporary RocksDB folder
OUTPUT_SST = "modified.sst"  # New SST after transformation

pyrex.PyRocksDB(WORK_DB)


# def bytes_to_int(b: bytes) -> int:
#     """Decode 8-byte little-endian signed integer."""
#     return struct.unpack("<q", b)[0]


# def int_to_bytes(i: int) -> bytes:
#     """Encode int to 8-byte little-endian signed integer."""
#     return struct.pack("<q", i)


# def main():
#     # Step 1: Open a temporary RocksDB
#     opts = rocksdb.Options()
#     opts.create_if_missing = True
#     if os.path.exists(WORK_DB):
#         import shutil

#         shutil.rmtree(WORK_DB)
#     db = rocksdb.DB(WORK_DB, opts)

#     # Step 2: Ingest the external SST into RocksDB
#     ingest_opts = rocksdb.IngestExternalFileOptions()
#     db.ingest_external_file([INPUT_SST], ingest_opts)

#     # Step 3: Create a new SST file writer
#     env_opts = rocksdb.EnvOptions()
#     sst_opts = rocksdb.Options()
#     writer = rocksdb.SstFileWriter(env_opts, sst_opts)

#     writer.open(OUTPUT_SST)

#     # Step 4: Iterate over all key-value pairs, modify values, and add to new SST
#     it = db.iterkeys()
#     it = db.iteritems()
#     it.seek_to_first()
#     for key, value in it:
#         try:
#             val_int = bytes_to_int(value)
#             new_val = int_to_bytes(val_int * 2)
#         except Exception:
#             # If not integer, keep as-is
#             new_val = value
#         writer.put(key, new_val)

#     # Step 5: Finalize new SST file
#     writer.finish()
#     print(f"✅ New SST written to {OUTPUT_SST}")


# if __name__ == "__main__":
#     main()
