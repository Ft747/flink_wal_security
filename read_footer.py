import struct


def decode_varint(buf, start=0):
    result = 0
    shift = 0
    i = start
    while True:
        b = buf[i]
        result |= (b & 0x7F) << shift
        i += 1
        if (b & 0x80) == 0:
            break
        shift += 7
    return result, i


def read_footer(path):
    with open(path, "rb") as f:
        f.seek(0, 2)  # move to end
        filesize = f.tell()
        footer_length = 48  # or kNewVersionsEncodedLength etc
        if filesize < footer_length:
            raise Exception("File too small to be valid SST")
        f.seek(filesize - footer_length)
        footer = f.read(footer_length)

    # Depending on version, first byte might be checksum type
    # Let's assume “new version” format:

    # For example:
    # byte 0: checksum_type (1 byte)
    # next: metaindex handle (varint offset, varint size)
    # next: index handle (varint offset, varint size)
    # then padding (unused) until magic number
    # then magic number (8 bytes)

    pos = 0
    checksum_type = footer[pos]
    pos += 1

    meta_off, pos = decode_varint(footer, pos)
    meta_size, pos = decode_varint(footer, pos)

    idx_off, pos = decode_varint(footer, pos)
    idx_size, pos = decode_varint(footer, pos)

    # magic = last 8 bytes
    magic = footer[-8:]
    # You could check that magic == expected RocksDB table magic constant

    return {
        "checksum_type": checksum_type,
        "metaindex": {"offset": meta_off, "size": meta_size},
        "index": {"offset": idx_off, "size": idx_size},
        "magic": magic,
    }


if __name__ == "__main__":
    print(read_footer("/home/dtome/Documents/School/Thesis/code/000023.sst"))
