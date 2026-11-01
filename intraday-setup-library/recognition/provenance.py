"""Content fingerprints for reproducible experiments."""
import hashlib
from .specs import ROOT


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def algorithm_sha256():
    digest = hashlib.sha256()
    files = list((ROOT / "recognition").glob("*.py")) + list((ROOT / "evaluation").glob("*.py"))
    for path in sorted(files):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()
