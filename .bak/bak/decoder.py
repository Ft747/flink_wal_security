import struct
import sys
import re


def main():
    key = "01000000000580054B162E00"
    seq = 138
    type = "000000084036000000000000"
    fooo = b"\x66\x6f\x6f"
    decoded = bytes.fromhex(type)
    unpacked = struct.unpack(">d", decoded[-8:])
    # .decode("utf-8")
    packed = struct.pack(">d", unpacked[0])
    print(packed, decoded[-8:], struct.unpack(">3s", fooo), fooo.decode("utf-8"))
    print(unpacked)


if __name__ == "__main__":
    main()
