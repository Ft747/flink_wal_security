def diff_files(file1, file2):
    with open(file1, "rb") as f1, open(file2, "rb") as f2:
        b1 = f1.read()
        b2 = f2.read()

    length = max(len(b1), len(b2))
    differences = []

    for i in range(length):
        byte1 = b1[i] if i < len(b1) else None
        byte2 = b2[i] if i < len(b2) else None
        if byte1 != byte2:
            differences.append((i, byte1, byte2))

    return differences


if __name__ == "__main__":
    file1 = "./000023.sst"
    file2 = "./a4208370-e176-40a6-8954-9c353fbb33bd.sst"

    diffs = diff_files(file1, file2)
    if not diffs:
        print("The files are identical.")
    else:
        print(f"Differences found at {len(diffs)} byte positions:")
        for pos, b1, b2 in diffs:
            print(f"Byte {pos}: {b1} != {b2}")
