"""Check the source manifest's byte-and-checksum gate."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from fetch_tlc import evidence, fetch, verify_source


class FetchTest(unittest.TestCase):
    def test_existing_source_is_reused_and_corruption_is_rejected(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            file = root / "yellow_tripdata_2024-12.parquet"
            file.write_bytes(b"fixture")
            length, sha256 = evidence(file)
            item = {"name": file.name, "bytes": length, "sha256": sha256}
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"snapshots": [item]}), encoding="utf-8")
            fetch(manifest, root)
            file.write_bytes(b"damaged")
            with self.assertRaisesRegex(ValueError, "checksum changed"):
                verify_source(file, item)

    def test_manifest_cannot_fetch_outside_the_source_directory(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"snapshots": [{"name": "../yellow_tripdata_2024-12.parquet"}]}))
            with self.assertRaisesRegex(ValueError, "Unexpected asset name"):
                fetch(manifest, root)
