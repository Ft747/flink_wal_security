import re
import struct

# Path to your sst_dump output file
SST_DUMP_FILE = "sst_dump_output.txt"

# Regex to match lines like:
# '00000000000580054BD02E00' seq:1254, type:1 => 00000008406A000000000000
line_pattern = re.compile(r"'([0-9A-Fa-f]+)'.*=>\s*([0-9A-Fa-f]+)")

pairs = []

with open(SST_DUMP_FILE, "r") as f:
    for line in f:
        m = line_pattern.search(line)
        if m:
            key_hex = m.group(1)
            val_hex = m.group(2)
            pairs.append((key_hex, val_hex))

print(f"Extracted {len(pairs)} key-value pairs")


# Example deserialization (you must know your Flink serializer!)
def deserialize_example(key_hex, val_hex):
    key_bytes = bytes.fromhex(key_hex)
    val_bytes = bytes.fromhex(val_hex)

    # Decode key: first 4 bytes = key group, next 8 = user key (long)
    key_group = struct.unpack("<I", key_bytes[0:4])[0]
    user_key = struct.unpack("<Q", key_bytes[4:12])[0]

    # Decode value: assume INT (count) + DOUBLE (average)
    count = struct.unpack("<I", val_bytes[0:4])[0]
    avg = struct.unpack("<d", val_bytes[4:12])[0]

    return {"key_group": key_group, "user_key": user_key, "count": count, "avg": avg}


def smart_deserialize(val_hex):
    val_bytes = bytes.fromhex(val_hex)

    if len(val_bytes) == 4:
        # Likely an INT
        return struct.unpack("<i", val_bytes)[0]
    elif len(val_bytes) == 8:
        # Likely a DOUBLE
        return struct.unpack("<d", val_bytes)[0]
    else:
        return val_bytes  # raw, unknown type


def decode_entry(user_key, val_bytes):
    # user_key is the raw 8-byte chunk, interpret as signed long
    k = struct.unpack("<q", struct.pack("<Q", user_key))[0]

    # split value
    count = struct.unpack("<i", val_bytes[0:4])[0]
    total_or_avg = struct.unpack("<d", val_bytes[4:12])[0]

    return {"user_key": k, "count": count, "value": total_or_avg}


for k, v in pairs[:5]:
    print(decode_entry(k, v))


# Run deserialization on the first 5 pairs
# for k, v in pairs[:]:
#     print(deserialize_example(k, v))
# for k_hex, v_hex in pairs[:10]:
#     key_bytes = bytes.fromhex(k_hex)
#     user_key = struct.unpack("<Q", key_bytes[-8:])[0]  # last 8 bytes = user key
#     value = smart_deserialize(v_hex)
#     print({"user_key": user_key, "value": value})
