"""Regression checks for distribution boundaries; no real secrets are used."""
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest
from release_support import delivery_files,safe_relative


class ReleaseBoundaryTests(unittest.TestCase):
    def test_credentials_runs_and_hidden_files_cannot_enter_distribution(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve()
            for name in ("README.md","runtime/runner.py","workbench/vendor/manifest.json","evaluation/tests/raw_fixtures.json","evaluation/tests/raw_negative_designs.json","evaluation/tests/fixtures/checkpoint_v1.json","evaluation/tests/fixtures/unrelated.json","evaluation/tests/unrelated.json",".env",".git/config",".venv/credentials.py","runtime/secrets.py","runtime/run.sqlite3","runtime-runs/log.json","reports/.token.json","config/credentials.json"):
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text("synthetic test content")
            self.assertEqual({p.relative_to(root).as_posix() for p in delivery_files(root)},
                             {"README.md","runtime/runner.py","workbench/vendor/manifest.json","evaluation/tests/raw_fixtures.json","evaluation/tests/raw_negative_designs.json","evaluation/tests/fixtures/checkpoint_v1.json"})

    def test_delivery_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();(root/"runtime").mkdir();(root/"outside.txt").write_text("fixture")
            (root/"runtime/runner.py").symlink_to(root/"outside.txt")
            with self.assertRaises(ValueError):delivery_files(root)

    def test_manifest_traversal_and_secret_names_are_rejected(self):
        for name in ("../outside.py","/absolute/file.py",".env","runtime/credentials.py","runtime/private-key.json","runtime/file.sqlite3"):
            with self.subTest(name=name):self.assertFalse(safe_relative(name))
        self.assertTrue(safe_relative("schema/market-event.schema.json"))

    def test_formal_package_requires_actual_soak_gate(self):
        with tempfile.TemporaryDirectory() as d:
            archive=Path(d)/"without-soak.zip"
            result=subprocess.run([sys.executable,str(Path(__file__).with_name("package.py")),"--output",str(archive)],capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn("Formal release requires --soak-report",result.stderr)
            self.assertFalse(archive.exists())


if __name__=="__main__":unittest.main()
