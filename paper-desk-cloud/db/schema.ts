import { index, primaryKey, sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';

export const userSettings = sqliteTable('user_settings', {
  userId: text('user_id').primaryKey(),
  locale: text('locale').notNull().default('zh-CN'),
  activeProvider: text('active_provider').notNull().default('deepseek'),
  deepseekReasoning: text('deepseek_reasoning').notNull().default('enabled'),
  openaiSpeed: text('openai_speed').notNull().default('standard'),
  selectedIdsJson: text('selected_ids_json').notNull().default('[]'),
  createdAt: text('created_at').notNull(),
  updatedAt: text('updated_at').notNull(),
});

export const credentials = sqliteTable('credentials', {
  userId: text('user_id').notNull(), provider: text('provider').notNull(),
  ciphertext: text('ciphertext').notNull(), iv: text('iv').notNull(), updatedAt: text('updated_at').notNull(),
}, table => [primaryKey({columns:[table.userId, table.provider]})]);

export const runs = sqliteTable('runs', {
  userId: text('user_id').primaryKey(), lifecycle: text('lifecycle').notNull(), symbol: text('symbol').notNull(),
  timeframe: text('timeframe').notNull(), executionPolicy: text('execution_policy').notNull(),
  providerCalls: integer('provider_calls').notNull().default(0), lastBarAt: text('last_bar_at'),
  snapshotJson: text('snapshot_json').notNull(), updatedAt: text('updated_at').notNull(),
});

export const customSetups = sqliteTable('custom_setups', {
  userId: text('user_id').notNull(), strategyId: text('strategy_id').notNull(), bundleHash: text('bundle_hash').notNull(),
  name: text('name').notNull(), sourceId: text('source_id').notNull(), metadataJson: text('metadata_json').notNull(),
  r2Key: text('r2_key').notNull(), createdAt: text('created_at').notNull(),
}, table => [primaryKey({columns:[table.userId, table.strategyId]}), index('idx_custom_setups_user').on(table.userId)]);

export const auditEvents = sqliteTable('audit_events', {
  id: integer('id').primaryKey({autoIncrement:true}), userId: text('user_id').notNull(), eventType: text('event_type').notNull(),
  payloadJson: text('payload_json').notNull(), occurredAt: text('occurred_at').notNull(),
}, table => [index('idx_audit_events_user_time').on(table.userId, table.occurredAt)]);
