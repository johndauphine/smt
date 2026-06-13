-- [safe] create table audit_log
CREATE TABLE `tgt`.`audit_log` (
    `id` BIGINT AUTO_INCREMENT NOT NULL,
    `actor` VARCHAR(100) NOT NULL,
    `action` VARCHAR(30) NOT NULL DEFAULT 'created',
    `at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `details` LONGTEXT,
    CONSTRAINT `pk_audit_log` PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- [blocking] create index ix_audit_actor
-- note: index creation can lock or scan the table
CREATE INDEX `ix_audit_actor` ON `tgt`.`audit_log` (`actor`);

-- [blocking] create index uq_audit_actor_at
-- note: index creation can lock or scan the table
CREATE UNIQUE INDEX `uq_audit_actor_at` ON `tgt`.`audit_log` (`actor`, `at`);

-- [blocking] create check constraint ck_audit_action
-- note: check validation can scan existing rows
ALTER TABLE `tgt`.`audit_log` ADD CONSTRAINT `ck_audit_action` CHECK (action IN ('created','updated','deleted'));

-- [blocking] create foreign key fk_audit_actor
-- note: foreign key validation can scan existing rows
ALTER TABLE `tgt`.`audit_log` ADD CONSTRAINT `fk_audit_actor` FOREIGN KEY (`actor`) REFERENCES `tgt`.`users` (`username`) ON DELETE CASCADE;

-- [safe] drop foreign key fk_users_dept
ALTER TABLE `tgt`.`users` DROP FOREIGN KEY `fk_users_dept`;

-- [safe] drop check constraint ck_users_legacy
ALTER TABLE `tgt`.`users` DROP CONSTRAINT `ck_users_legacy`;

-- [safe] drop index ix_users_legacy
DROP INDEX `ix_users_legacy` ON `tgt`.`users`;

-- [safe] add column nickname
ALTER TABLE `tgt`.`users` ADD COLUMN `nickname` VARCHAR(40);

-- [rebuild] change column username type
-- note: type changes may rewrite the table and can fail if existing values cannot be cast
ALTER TABLE `tgt`.`users` MODIFY COLUMN `username` VARCHAR(80) NOT NULL;

-- [safe] change column active default
ALTER TABLE `tgt`.`users` ALTER COLUMN `active` SET DEFAULT 1;

-- [data-loss-risk] drop column legacy_code
-- note: drops column data
ALTER TABLE `tgt`.`users` DROP COLUMN `legacy_code`;

-- [blocking] create index ix_users_nickname
-- note: index creation can lock or scan the table
CREATE INDEX `ix_users_nickname` ON `tgt`.`users` (`nickname`);

-- [blocking] create foreign key fk_users_org
-- note: foreign key validation can scan existing rows
ALTER TABLE `tgt`.`users` ADD CONSTRAINT `fk_users_org` FOREIGN KEY (`org_id`) REFERENCES `tgt`.`orgs` (`id`) ON DELETE SET NULL;

-- [blocking] create check constraint ck_users_username
-- note: check validation can scan existing rows
ALTER TABLE `tgt`.`users` ADD CONSTRAINT `ck_users_username` CHECK (username <> '');

-- [data-loss-risk] drop table line_items
-- note: drops the table and its data
DROP TABLE IF EXISTS `tgt`.`line_items`;

-- [data-loss-risk] drop table orders
-- note: drops the table and its data
DROP TABLE IF EXISTS `tgt`.`orders`;

