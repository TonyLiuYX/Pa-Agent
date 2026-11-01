"""Compare two clean distributions and deterministic ZIPs from frozen inputs.

This writes only the evidence report in the source project. Rebuild afterward.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from build import ROOT,VERSION,DATE


def command(args):
    result=subprocess.run([sys.executable,*map(str,args)],capture_output=True,text=True)
    if result.returncode:raise RuntimeError(result.stdout[-4000:]+result.stderr[-4000:])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,default=ROOT/"reports/rebuild-results.json")
    args=parser.parse_args()
    result={"status":"failed","version":VERSION,"date":DATE,"different_files":[],
            "scope":"Two clean allowlisted distributions and deterministic ZIPs; frozen inputs only, no market performance validation."}
    try:
        with tempfile.TemporaryDirectory(prefix="intraday-repro-") as temporary:
            base=Path(temporary);stages=[base/"first",base/"second"]
            for stage in stages:command([ROOT/"scripts/build.py","--output",stage])
            manifests=[json.loads((stage/"manifest.json").read_text()) for stage in stages]
            first={r["path"]:r["sha256"] for r in manifests[0]["files"]}
            second={r["path"]:r["sha256"] for r in manifests[1]["files"]}
            different=[path for path in sorted(set(first)|set(second)) if first.get(path)!=second.get(path)]
            if manifests[0]!=manifests[1]:different.append("manifest.json")
            result.update(deliverable_files_compared=len(first),different_files=different,manifest_equal=manifests[0]==manifests[1])
            result["input_manifest_sha256"]=hashlib.sha256((stages[0]/"manifest.json").read_bytes()).hexdigest()
            if different:raise ValueError("isolated builds differ")
            archives=[base/"first.zip",base/"second.zip"]
            for stage,archive in zip(stages,archives):command([stage/"scripts/package.py","--candidate","--output",archive])
            hashes=[hashlib.sha256(archive.read_bytes()).hexdigest() for archive in archives]
            result.update(archive_sha256=hashes[0],archive_equal=hashes[0]==hashes[1],zip_bytes=archives[0].stat().st_size)
            result["archive_scope"]="Temporary frozen-input validation snapshots; final published archive identity is in its external release-checks report."
            if hashes[0]!=hashes[1]:raise ValueError("archive bytes differ")
            result["status"]="passed"
        result["temporary_copies_removed"]=True
    except Exception as error:result["error"]=str(error)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 0 if result["status"]=="passed" else 1


if __name__=="__main__":sys.exit(main())
