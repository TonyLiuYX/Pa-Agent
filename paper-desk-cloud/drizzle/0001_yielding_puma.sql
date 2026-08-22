CREATE INDEX `idx_audit_events_user_time` ON `audit_events` (`user_id`,`occurred_at`);--> statement-breakpoint
CREATE INDEX `idx_custom_setups_user` ON `custom_setups` (`user_id`);