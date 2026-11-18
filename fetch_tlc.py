"""Acquire the two pinned NYC TLC Parquet snapshots."""
import argparse
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen

CDN = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def evidence(path):
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            length += len(block)
            digest.update(block)
    return length, digest.hexdigest()


def verify_source(path, item):
    if evidence(path) != (item["bytes"], item["sha256"]):
        raise ValueError(f"Source size or checksum changed for {path.name}")


def fetch(manifest, out):
    out.mkdir(parents=True, exist_ok=True)
    for item in json.loads(manifest.read_text(encoding="utf-8"))["snapshots"]:
        name = item["name"]
        if name != Path(name).name or not name.startswith("yellow_tripdata_"):
            raise ValueError("Unexpected asset name")
        path = out / name
        if path.is_file():
            verify_source(path, item)
            continue
        with TemporaryDirectory(dir=out) as folder:
            staged = Path(folder) / name
            size = 0
            with urlopen(CDN + name, timeout=60) as source, staged.open("wb") as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    size += len(chunk)
                    if size > item["bytes"]:
                        raise ValueError(f"Download exceeds the pinned size for {name}")
                    target.write(chunk)
            verify_source(staged, item)
            staged.rename(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
    parser.add_argument("--out", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    fetch(args.manifest, args.out)
