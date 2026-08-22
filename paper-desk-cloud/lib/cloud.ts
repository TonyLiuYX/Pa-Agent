import { env } from 'cloudflare:workers';
import { STRATEGIES } from './product';

type Env = Cloudflare.Env;
const runtimeEnv = env as Env;
let schemaReady: Promise<void> | null = null;

const schemaStatements = [
  `CREATE TABLE IF NOT EXISTS user_settings (user_id TEXT PRIMARY KEY, locale TEXT NOT NULL DEFAULT 'zh-CN', active_provider TEXT NOT NULL DEFAULT 'deepseek', deepseek_reasoning TEXT NOT NULL DEFAULT 'enabled', openai_speed TEXT NOT NULL DEFAULT 'standard', selected_ids_json TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS credentials (user_id TEXT NOT NULL, provider TEXT NOT NULL, ciphertext TEXT NOT NULL, iv TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (user_id, provider))`,
  `CREATE TABLE IF NOT EXISTS runs (user_id TEXT PRIMARY KEY, lifecycle TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL, execution_policy TEXT NOT NULL, provider_calls INTEGER NOT NULL DEFAULT 0, last_bar_at TEXT, snapshot_json TEXT NOT NULL, updated_at TEXT NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS custom_setups (user_id TEXT NOT NULL, strategy_id TEXT NOT NULL, bundle_hash TEXT NOT NULL, name TEXT NOT NULL, source_id TEXT NOT NULL, metadata_json TEXT NOT NULL, r2_key TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (user_id, strategy_id))`,
  `CREATE TABLE IF NOT EXISTS audit_events (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, occurred_at TEXT NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_events_user_time ON audit_events(user_id, occurred_at DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_custom_setups_user ON custom_setups(user_id)`,
];

export async function ensureSchema() {
  if (!schemaReady) schemaReady = runtimeEnv.DB.batch(schemaStatements.map(sql => runtimeEnv.DB.prepare(sql))).then(() => undefined);
  await schemaReady;
}

export function userId(request: Request) {
  const id = request.headers.get('oai-authenticated-user-id');
  if (id) return id;
  const host = new URL(request.url).hostname;
  if (host === 'localhost' || host === '127.0.0.1') return 'local-development-owner';
  throw new CloudError(401, 'authentication_required', 'Sign in to this private Site to continue.');
}

export class CloudError extends Error {
  constructor(public status: number, public code: string, message: string) { super(message); }
}

export function now() { return new Date().toISOString(); }

export type UserSettings = {
  user_id: string; locale: string; active_provider: string; deepseek_reasoning: string;
  openai_speed: string; selected_ids_json: string; created_at: string; updated_at: string;
};

export async function getUserSettings(user: string): Promise<UserSettings> {
  await ensureSchema();
  const existing = await runtimeEnv.DB.prepare('SELECT * FROM user_settings WHERE user_id = ?').bind(user).first<UserSettings>();
  if (existing) return existing;
  const timestamp = now();
  const selected = JSON.stringify(STRATEGIES.map(item => item.strategy_id));
  await runtimeEnv.DB.prepare(`INSERT INTO user_settings (user_id, locale, active_provider, deepseek_reasoning, openai_speed, selected_ids_json, created_at, updated_at) VALUES (?, 'zh-CN', 'deepseek', 'enabled', 'standard', ?, ?, ?)`).bind(user, selected, timestamp, timestamp).run();
  return {user_id:user,locale:'zh-CN',active_provider:'deepseek',deepseek_reasoning:'enabled',openai_speed:'standard',selected_ids_json:selected,created_at:timestamp,updated_at:timestamp};
}

export function selectedIds(settings: UserSettings) {
  try { const value = JSON.parse(settings.selected_ids_json); return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []; }
  catch { return []; }
}

export async function credentialStatus(user: string) {
  await ensureSchema();
  const result = await runtimeEnv.DB.prepare('SELECT provider FROM credentials WHERE user_id = ?').bind(user).all<{provider:string}>();
  const configured = {alpaca:false,deepseek:false,openai:false};
  for (const row of result.results) if (row.provider in configured) configured[row.provider as keyof typeof configured] = true;
  return configured;
}

function bytesToBase64(bytes: Uint8Array) { let value=''; for (const byte of bytes) value += String.fromCharCode(byte); return btoa(value); }
function base64ToBytes(value: string) { const raw=atob(value); return Uint8Array.from(raw, char => char.charCodeAt(0)); }

async function vaultKey(request?: Request) {
  let bytes: Uint8Array;
  if (runtimeEnv.PA_VAULT_KEY) bytes = base64ToBytes(runtimeEnv.PA_VAULT_KEY);
  else if (request && ['localhost','127.0.0.1'].includes(new URL(request.url).hostname)) bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode('paper-desk-local-development-only')));
  else throw new CloudError(503, 'vault_unavailable', 'Credential vault is not configured.');
  if (bytes.byteLength !== 32) throw new CloudError(503, 'vault_invalid', 'Credential vault configuration is invalid.');
  return crypto.subtle.importKey('raw', bytes as BufferSource, 'AES-GCM', false, ['encrypt','decrypt']);
}

export async function saveCredential(request: Request, user: string, provider: string, payload: Record<string,string>) {
  const key = await vaultKey(request); const iv = crypto.getRandomValues(new Uint8Array(12));
  const plain = new TextEncoder().encode(JSON.stringify(payload));
  const encrypted = new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM',iv}, key, plain));
  await runtimeEnv.DB.prepare(`INSERT INTO credentials (user_id, provider, ciphertext, iv, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id, provider) DO UPDATE SET ciphertext=excluded.ciphertext, iv=excluded.iv, updated_at=excluded.updated_at`).bind(user,provider,bytesToBase64(encrypted),bytesToBase64(iv),now()).run();
}

export async function loadCredential(request: Request, user: string, provider: string): Promise<Record<string,string>> {
  await ensureSchema();
  const row = await runtimeEnv.DB.prepare('SELECT ciphertext, iv FROM credentials WHERE user_id = ? AND provider = ?').bind(user,provider).first<{ciphertext:string,iv:string}>();
  if (!row) throw new CloudError(400,'credentials_missing',`${provider} credentials are not configured.`);
  try {
    const key = await vaultKey(request);
    const plain = await crypto.subtle.decrypt({name:'AES-GCM',iv:base64ToBytes(row.iv)},key,base64ToBytes(row.ciphertext));
    const value = JSON.parse(new TextDecoder().decode(plain));
    if (!value || typeof value !== 'object') throw new Error();
    return value as Record<string,string>;
  } catch (error) {
    if (error instanceof CloudError) throw error;
    throw new CloudError(500,'credential_decryption_failed','Stored credentials could not be decrypted. Clear and save them again.');
  }
}

export async function removeCredential(user: string, provider: string) { await ensureSchema(); await runtimeEnv.DB.prepare('DELETE FROM credentials WHERE user_id = ? AND provider = ?').bind(user,provider).run(); }

export async function audit(user: string, eventType: string, payload: Record<string,unknown> = {}) {
  await ensureSchema();
  await runtimeEnv.DB.prepare('INSERT INTO audit_events (user_id, event_type, payload_json, occurred_at) VALUES (?, ?, ?, ?)').bind(user,eventType,JSON.stringify(payload),now()).run();
}

export { runtimeEnv };
