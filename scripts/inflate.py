"""Build a large SAS7BDAT file for benchmarks by repeating the row pages of a small one.

Only pages that hold rows and no metadata are repeated, so the metadata stays valid;
the page count in the header and the row count in the row-size subheader are patched.

Usage: python scripts/inflate.py <input.sas7bdat> <output.sas7bdat> <copies>
"""

import struct
import sys

ROW_SIZE = 0xF7F7F7F7
METADATA_SIGNATURES = {0xF7F7F7F7, 0xF6F6F6F6, 0xFFFFFC00, 0xFFFFFFFD,
                       0xFFFFFFFF, 0xFFFFFFFC, 0xFFFFFBFE, 0xFFFFFFFE}


def main() -> None:
    src, dst, copies = sys.argv[1], sys.argv[2], int(sys.argv[3])
    data = bytearray(open(src, "rb").read())

    wide = data[32] == 0x33
    align1 = 4 if data[35] == 0x33 else 0
    order = "<" if data[37] == 1 else ">"
    word, wfmt = (8, "Q") if wide else (4, "I")
    u16 = lambda b, o: struct.unpack_from(order + "H", b, o)[0]
    u32 = lambda b, o: struct.unpack_from(order + "I", b, o)[0]
    uw = lambda b, o: struct.unpack_from(order + wfmt, b, o)[0]

    header_len = u32(data, 196 + align1)
    page_len = u32(data, 200 + align1)
    page_count = uw(data, 204 + align1)
    base = 32 if wide else 16
    ptr_len = 24 if wide else 12

    pages = [data[header_len + i * page_len: header_len + (i + 1) * page_len] for i in range(page_count)]
    row_pages, row_size_at = [], None
    for i, page in enumerate(pages):
        ptype, _blocks, nsub = u16(page, base), u16(page, base + 2), u16(page, base + 4)
        if ptype == 0x9000:
            continue  # compression bookkeeping page, no pointer table
        has_meta, rows = False, 0
        for j in range(nsub):
            at = base + 8 + j * ptr_len
            off, length = uw(page, at), uw(page, at + word)
            compression, kind = page[at + 2 * word], page[at + 2 * word + 1]
            if length == 0 or compression == 1:
                continue
            sig = u32(page, off)
            if compression == 4 or (kind == 1 and sig not in METADATA_SIGNATURES):
                rows += 1  # a row stored in a subheader (compressed files)
            elif sig in METADATA_SIGNATURES:
                has_meta = True
                if sig == ROW_SIZE:
                    row_size_at = header_len + i * page_len + off
        if ptype & 0x0F00 == 0x0100:
            rows = _blocks  # data page: rows stored back to back
        if ptype == 0x9000 or has_meta or rows == 0:
            continue
        row_pages.append((page, rows))

    row_count = uw(data, row_size_at + 6 * word)
    new_rows = row_count + copies * sum(rows for _, rows in row_pages)
    new_pages = page_count + copies * len(row_pages)

    struct.pack_into(order + wfmt, data, 204 + align1, new_pages)
    struct.pack_into(order + wfmt, data, row_size_at + 6 * word, new_rows)
    with open(dst, "wb") as out:
        out.write(data)
        for _ in range(copies):
            for page, _ in row_pages:
                out.write(page)
    print(f"{dst}: {new_pages} pages, {new_rows} rows (row pages repeated {copies}x)")


if __name__ == "__main__":
    main()
