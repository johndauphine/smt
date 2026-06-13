-- [safe] create table audit_log
CREATE TABLE [tgt].[audit_log] (
    [id] BIGINT IDENTITY(1,1) NOT NULL,
    [actor] VARCHAR(100) NOT NULL,
    [action] VARCHAR(30) NOT NULL DEFAULT 'created',
    [at] DATETIME2(3) NOT NULL DEFAULT GETUTCDATE(),
    [details] NVARCHAR(MAX),
    CONSTRAINT [pk_audit_log] PRIMARY KEY ([id])
);

-- [blocking] create index ix_audit_actor
-- note: index creation can lock or scan the table
CREATE INDEX [ix_audit_actor] ON [tgt].[audit_log] ([actor]);

-- [blocking] create index uq_audit_actor_at
-- note: index creation can lock or scan the table
CREATE UNIQUE INDEX [uq_audit_actor_at] ON [tgt].[audit_log] ([actor], [at]);

-- [blocking] create check constraint ck_audit_action
-- note: check validation can scan existing rows
ALTER TABLE [tgt].[audit_log] ADD CONSTRAINT [ck_audit_action] CHECK (action IN ('created','updated','deleted'));

-- [blocking] create foreign key fk_audit_actor
-- note: foreign key validation can scan existing rows
ALTER TABLE [tgt].[audit_log] ADD CONSTRAINT [fk_audit_actor] FOREIGN KEY ([actor]) REFERENCES [tgt].[users] ([username]) ON DELETE CASCADE;

-- [safe] drop foreign key fk_users_dept
ALTER TABLE [tgt].[users] DROP CONSTRAINT [fk_users_dept];

-- [safe] drop check constraint ck_users_legacy
ALTER TABLE [tgt].[users] DROP CONSTRAINT [ck_users_legacy];

-- [safe] drop index ix_users_legacy
DROP INDEX [ix_users_legacy] ON [tgt].[users];

-- [safe] add column nickname
ALTER TABLE [tgt].[users] ADD [nickname] VARCHAR(40);

-- [rebuild] change column username type
-- note: type changes may rewrite the table and can fail if existing values cannot be cast
ALTER TABLE [tgt].[users] ALTER COLUMN [username] VARCHAR(80) NOT NULL;

-- [safe] drop column active default
DECLARE @constraintName sysname; SELECT @constraintName = dc.name FROM sys.default_constraints dc JOIN sys.columns c ON c.default_object_id = dc.object_id JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = N'tgt' AND t.name = N'users' AND c.name = N'active'; IF @constraintName IS NOT NULL EXEC(N'ALTER TABLE [tgt].[users] DROP CONSTRAINT ' + QUOTENAME(@constraintName));

-- [safe] change column active default
ALTER TABLE [tgt].[users] ADD CONSTRAINT [df_users_active] DEFAULT 1 FOR [active];

-- [data-loss-risk] drop column legacy_code
-- note: drops column data
ALTER TABLE [tgt].[users] DROP COLUMN [legacy_code];

-- [blocking] create index ix_users_nickname
-- note: index creation can lock or scan the table
CREATE INDEX [ix_users_nickname] ON [tgt].[users] ([nickname]);

-- [blocking] create foreign key fk_users_org
-- note: foreign key validation can scan existing rows
ALTER TABLE [tgt].[users] ADD CONSTRAINT [fk_users_org] FOREIGN KEY ([org_id]) REFERENCES [tgt].[orgs] ([id]) ON DELETE SET NULL;

-- [blocking] create check constraint ck_users_username
-- note: check validation can scan existing rows
ALTER TABLE [tgt].[users] ADD CONSTRAINT [ck_users_username] CHECK (username <> '');

-- [data-loss-risk] drop table line_items
-- note: drops the table and its data
DROP TABLE IF EXISTS [tgt].[line_items];

-- [data-loss-risk] drop table orders
-- note: drops the table and its data
DROP TABLE IF EXISTS [tgt].[orders];

