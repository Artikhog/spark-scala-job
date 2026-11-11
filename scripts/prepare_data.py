import argparse
import csv
import hashlib
import json
import tarfile
import tempfile
from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path

SOURCE_URL = "https://zenodo.org/records/20289873"
ARCHIVE_MD5 = "7ee5c87798cbb14f93f39e54b01ef0e6"
MEMBER = "oc-2026-01.csv"
COLUMNS = (
    "hashed_ip continent_name country_iso_code country_name request_method "
    "request_host request_path http_response_code user_agent token date referer"
).split()


def csv_records(stream):
    """Parse records while retaining their original bytes, including newlines."""
    pending = []

    def lines():
        for line in stream:
            pending.append(line)
            yield line.decode("utf-8")

    for row in csv.reader(lines(), strict=True):
        raw = b"".join(pending)
        multiline = len(pending) > 1
        pending.clear()
        yield row, raw, multiline


def prepare(archive, output, sizes_gb):
    targets = [int(size * 1_000_000_000) for size in sizes_gb]
    if not targets or targets[0] <= 0 or targets != sorted(set(targets)):
        raise ValueError("Sizes must be positive, distinct and increasing")
    names = [f"opencitations-{size:g}gb.csv" for size in sizes_gb]
    output.mkdir(parents=True, exist_ok=True)
    for name in names + ["manifest.json"]:
        if (output / name).exists():
            raise FileExistsError(f"Output already exists: {output / name}")

    with tempfile.TemporaryDirectory(prefix=".prepare-", dir=output) as staging:
        staging = Path(staging)
        with tarfile.open(archive, "r|gz") as tar, ExitStack() as stack:
            member = next((m for m in tar if m.isfile() and m.name == MEMBER), None)
            if member is None:
                raise ValueError(f"Archive does not contain {MEMBER}")
            source = stack.enter_context(tar.extractfile(member))
            records = csv_records(source)
            header, raw_header, _ = next(records)
            if header != COLUMNS:
                raise ValueError(f"Unexpected CSV header: {header}")
            if len(raw_header) >= targets[0]:
                raise ValueError("Smallest size must exceed the CSV header size")
            files = [stack.enter_context((staging / name).open("wb")) for name in names]
            for file in files:
                file.write(raw_header)

            digest = hashlib.sha256(raw_header)
            total, rows, multiline_rows, active = len(raw_header), 0, 0, 0
            samples = []
            for row, raw, multiline in records:
                if len(row) != len(COLUMNS):
                    raise ValueError(f"Record {rows + 1}: expected 12 fields, got {len(row)}")
                while active < len(targets) and total + len(raw) > targets[active]:
                    if rows == 0:
                        raise ValueError("Size is too small for a complete data record")
                    files[active].close()
                    sample = {
                        "file": names[active],
                        "target_bytes": targets[active],
                        "actual_bytes": total,
                        "records": rows,
                        "multiline_records": multiline_rows,
                        "sha256": digest.hexdigest(),
                    }
                    samples.append(sample)
                    print(f"{names[active]}: {total:,} bytes, {rows:,} records", flush=True)
                    active += 1
                if active == len(targets):
                    break
                for file in files[active:]:
                    file.write(raw)
                digest.update(raw)
                total += len(raw)
                rows += 1
                multiline_rows += int(multiline)
            if active != len(targets):
                raise ValueError("Source CSV is too small to reach all requested cutoffs")

        manifest = {
            "source_url": SOURCE_URL,
            "archive_md5": ARCHIVE_MD5,
            "source_member": MEMBER,
            "method": "nested prefixes, whole CSV records, original bytes, header included",
            "gb_bytes": 1_000_000_000,
            "columns": COLUMNS,
            "samples": samples,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        for name in names + ["manifest.json"]:
            (staging / name).rename(output / name)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("data/samples"))
    parser.add_argument("--sizes-gb", type=Decimal, nargs="+", default=[Decimal(1), Decimal(2), Decimal(4)])
    args = parser.parse_args()
    print("Verifying archive MD5...", flush=True)
    digest = hashlib.md5()
    with args.archive.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    if digest.hexdigest() != ARCHIVE_MD5:
        parser.error("Archive checksum mismatch; download the pinned source archive again")
    print("Archive MD5 OK; preparing CSV samples...", flush=True)
    prepare(args.archive, args.output, args.sizes_gb)


if __name__ == "__main__":
    main()
