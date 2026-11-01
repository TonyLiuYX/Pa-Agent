"""Package only manifest-listed deliverables and verify the ZIP and hashes.
Writes the release-checks JSON beside the ZIP, never inside it.
Existing archives are preserved; choose a new --output name for another release.
"""
import argparse,hashlib,json,pathlib,subprocess,sys,zipfile
from build import ROOT
from release_support import delivery_files,release_metadata,safe_relative
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--output',type=pathlib.Path);ap.add_argument('--candidate',action='store_true',help='Package engineering candidate without completed ten-hour acceptance');ap.add_argument('--soak-report',type=pathlib.Path,help='Completed current-core soak report required for formal release');args=ap.parse_args();manifest=json.loads((ROOT/'manifest.json').read_text());release=release_metadata(ROOT);out=args.output or ROOT.parent/('intraday-setup-library-v'+manifest['version']+('-candidate' if args.candidate else '')+'.zip');out=out.resolve()
 if out.exists():raise SystemExit('Archive already exists; choose a new --output path: '+str(out))
 if out.is_relative_to(ROOT):raise SystemExit('Write the archive outside the project directory')
 soak=None
 if not args.candidate:
  if not args.soak_report:raise SystemExit('Formal release requires --soak-report; use --candidate for an engineering candidate')
  from verify_soak import verify_soak
  soak=verify_soak(args.soak_report)
 check=subprocess.run([sys.executable,str(ROOT/'scripts/validate.py')],capture_output=True,text=True)
 if check.returncode:print(check.stdout);print(check.stderr);return 1
 allowed={p.relative_to(ROOT).as_posix() for p in delivery_files(ROOT)}
 paths=sorted([f['path'] for f in manifest['files']]+['manifest.json'])
 if len(paths)!=len(set(paths)) or set(paths)-{'manifest.json'}!=allowed:raise ValueError('manifest differs from release allowlist')
 prefix=release.get('artifact_root','intraday-setup-library')
 if not safe_relative(prefix) or '/' in prefix:raise ValueError('invalid archive root')
 y,m,d=map(int,release['content_date'].split('-'));timestamp=(max(1980,y),m,d,0,0,0)
 out.parent.mkdir(parents=True,exist_ok=True)
 with zipfile.ZipFile(out,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
  for rel in paths:
   p=(ROOT/rel).resolve()
   if not p.is_relative_to(ROOT) or not p.is_file():raise ValueError('Invalid manifest path: '+rel)
   info=zipfile.ZipInfo(prefix+'/'+rel,date_time=timestamp);info.create_system=3;info.external_attr=0o100644<<16;info.compress_type=zipfile.ZIP_DEFLATED
   z.writestr(info,p.read_bytes(),compress_type=zipfile.ZIP_DEFLATED,compresslevel=9)
 with zipfile.ZipFile(out) as z:
  if z.testzip() is not None:raise ValueError('ZIP CRC failure')
  if len(z.namelist())!=len(paths):raise ValueError('ZIP entry count differs from manifest')
  loaded=json.loads(z.read(prefix+'/manifest.json'))
  if loaded!=manifest:raise ValueError('Embedded manifest differs from source')
  for f in loaded['files']:
   raw=z.read(prefix+'/'+f['path'])
   if len(raw)!=f['bytes'] or hashlib.sha256(raw).hexdigest()!=f['sha256']:raise ValueError('ZIP content differs from manifest: '+f['path'])
 result={'status':'passed','release_kind':'candidate' if args.candidate else 'formal','ten_hour_acceptance':'not_claimed' if args.candidate else 'verified','soak_validation':soak,'version':manifest['version'],'content_date':manifest['date'],'archive':out.name,'bytes':out.stat().st_size,'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'entries':len(paths),'crc_check':'passed','manifest_hashes':'all passed','reviewed_count':manifest['counts'].get('reviewed',0),'scope':'Only manifest deliverables, no temporary public-source originals or external chart copies'}
 report=out.with_name(out.stem+'-release-checks.json');report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps(result,ensure_ascii=False,indent=2));return 0
if __name__=='__main__':sys.exit(main())
