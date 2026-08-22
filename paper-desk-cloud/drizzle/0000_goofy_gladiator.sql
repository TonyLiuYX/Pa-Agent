CREATE TABLE `audit_events` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`user_id` text NOT NULL,
	`event_type` text NOT NULL,
	`payload_json` text NOT NULL,
	`occurred_at` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `credentials` (
	`user_id` text NOT NULL,
	`provider` text NOT NULL,
	`ciphertext` text NOT NULL,
	`iv` text NOT NULL,
	`updated_at` text NOT NULL,
	PRIMARY KEY(`user_id`, `provider`)
);
--> statement-breakpoint
CREATE TABLE `custom_setups` (
	`user_id` text NOT NULL,
	`strategy_id` text NOT NULL,
	`bundle_hash` text NOT NULL,
	`name` text NOT NULL,
	`source_id` text NOT NULL,
	`metadata_json` text NOT NULL,
	`r2_key` text NOT NULL,
	`created_at` text NOT NULL,
	PRIMARY KEY(`user_id`, `strategy_id`)
);
--> statement-breakpoint
CREATE TABLE `runs` (
	`user_id` text PRIMARY KEY NOT NULL,
	`lifecycle` text NOT NULL,
	`symbol` text NOT NULL,
	`timeframe` text NOT NULL,
	`execution_policy` text NOT NULL,
	`provider_calls` integer DEFAULT 0 NOT NULL,
	`last_bar_at` text,
	`snapshot_json` text NOT NULL,
	`updated_at` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `user_settings` (
	`user_id` text PRIMARY KEY NOT NULL,
	`locale` text DEFAULT 'zh-CN' NOT NULL,
	`active_provider` text DEFAULT 'deepseek' NOT NULL,
	`deepseek_reasoning` text DEFAULT 'enabled' NOT NULL,
	`openai_speed` text DEFAULT 'standard' NOT NULL,
	`selected_ids_json` text DEFAULT '[]' NOT NULL,
	`created_at` text NOT NULL,
	`updated_at` text NOT NULL
);
