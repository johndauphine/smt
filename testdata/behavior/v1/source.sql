-- Small independent source fixture. No application data or CRM fixture reload.
CREATE TABLE dbo.behavior_simple (
    id int NOT NULL CONSTRAINT pk_behavior_simple PRIMARY KEY,
    code varchar(12) COLLATE Latin1_General_100_BIN2 NULL,
    tiny tinyint NULL CONSTRAINT df_behavior_tiny DEFAULT (7),
    payload varbinary(50) NULL CONSTRAINT df_behavior_payload DEFAULT (0xAB),
    required int NOT NULL CONSTRAINT df_behavior_required DEFAULT (1),
    amount decimal(5,2) NULL,
    CONSTRAINT uq_behavior_code UNIQUE (code)
);
CREATE INDEX ix_behavior_tiny ON dbo.behavior_simple(tiny);
CREATE TABLE dbo.behavior_edge (
    id int NOT NULL CONSTRAINT pk_behavior_edge PRIMARY KEY,
    fixed binary(16) NULL,
    t0 time(0) NULL,
    t3 time(3) NULL,
    legacy datetime NULL,
    minute smalldatetime NULL,
    unicode nvarchar(2) COLLATE Latin1_General_100_BIN2 NULL
);
