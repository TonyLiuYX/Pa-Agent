import { unzipSync } from 'fflate';
import { CloudError } from './cloud';

const required = ['bundle.json','source.pdf','manifest.json','chunks.json','pa_records.json'];
const decoder = new TextDecoder('utf-8',{fatal:true});
const hex = (buffer:ArrayBuffer) => [...new Uint8Array(buffer)].map(byte=>byte.toString(16).padStart(2,'0')).join('');
export async function sha256(bytes: Uint8Array) { return hex(await crypto.subtle.digest('SHA-256',bytes as BufferSource)); }
function json(bytes: Uint8Array,name:string) { try { const value=JSON.parse(decoder.decode(bytes)); if (!value || typeof value!=='object' || Array.isArray(value)) throw new Error(); return value as Record<string,unknown>; } catch { throw new CloudError(400,'bundle_json_invalid',`${name} must be a valid UTF-8 JSON object.`); } }

export async function validateBundle(archive: Uint8Array) {
  if (!archive.length || archive.length > 50*1024*1024) throw new CloudError(413,'bundle_too_large','Bundle must be 50 MB or smaller.');
  let files: Record<string,Uint8Array>;
  try { files=unzipSync(archive,{filter:file=>!file.name.endsWith('/')}); } catch { throw new CloudError(400,'bundle_zip_invalid','Bundle is not a valid ZIP archive.'); }
  const names=Object.keys(files).sort(); if (JSON.stringify(names)!==JSON.stringify([...required].sort())) throw new CloudError(400,'bundle_files_invalid','Bundle must contain exactly the five required root files.');
  if (Object.values(files).reduce((sum,value)=>sum+value.length,0)>100*1024*1024) throw new CloudError(413,'bundle_expanded_too_large','Expanded Bundle must be 100 MB or smaller.');
  if (decoder.decode(files['source.pdf'].slice(0,5))!=='%PDF-') throw new CloudError(400,'bundle_pdf_invalid','source.pdf is not a PDF file.');
  const bundle=json(files['bundle.json'],'bundle.json'); const records=json(files['pa_records.json'],'pa_records.json');
  json(files['manifest.json'],'manifest.json'); json(files['chunks.json'],'chunks.json');
  if (bundle.schema_version!=='pa-setup-bundle.v1'||bundle.bundle_version!=='1.0.0'||typeof bundle.bundle_id!=='string') throw new CloudError(400,'bundle_schema_invalid','bundle.json does not match Setup Bundle v1.');
  const setupIds=Array.isArray(bundle.setup_ids)?bundle.setup_ids.filter((id):id is string=>typeof id==='string'):[];
  if (!setupIds.length||setupIds.some(id=>!/^pa:user-[0-9a-f]{8,16}-[a-z0-9]+(?:-[a-z0-9]+)*$/.test(id))) throw new CloudError(400,'bundle_setup_ids_invalid','Bundle setup IDs are invalid.');
  const declared=bundle.files as Record<string,string>|undefined;
  for (const name of required.slice(1)) if (!declared||declared[name]!==await sha256(files[name])) throw new CloudError(400,'bundle_hash_mismatch',`${name} hash does not match bundle.json.`);
  const recordList=Array.isArray(records.records)?records.records:[];
  const setups=recordList.filter((item):item is Record<string,unknown>=>!!item&&typeof item==='object'&&!Array.isArray(item)&&item.type==='setup'&&item.active!==false);
  const actual=new Set(setups.map(item=>`pa:${item.id}`)); if (actual.size!==setupIds.length||setupIds.some(id=>!actual.has(id))) throw new CloudError(400,'bundle_setup_ids_invalid','Bundle setup IDs do not match active setup records.');
  return {bundle,setupIds,setups,files,hash:await sha256(archive)};
}
