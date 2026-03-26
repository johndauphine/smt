# Setup Guide

Step-by-step instructions for installing and configuring SMT.

## Prerequisites

- **Python 3.12+**
- **Database drivers** (at least one):
  - PostgreSQL: psycopg2-binary (installed automatically with `smt[postgres]`)
  - MSSQL: pyodbc (installed automatically with `smt[mssql]`) + system ODBC driver

## Installation

### From Source (Development)

```bash
git clone https://github.com/johndauphine/smt.git
cd smt

# Using uv (recommended)
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev,postgres,mssql]"

# Or using pip
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,postgres,mssql]"
```

### Verify Installation

```bash
smt --help
```

## MSSQL ODBC Driver Installation

MSSQL support requires Microsoft ODBC Driver 18 installed at the system level.

### macOS

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18
```

### Ubuntu / Debian

```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
sudo add-apt-repository "$(curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list)"
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### RHEL / CentOS

```bash
curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y yum install -y msodbcsql18
```

### Windows

Download and run the installer from [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### Verify ODBC Driver

```bash
python -c "import pyodbc; print([d for d in pyodbc.drivers() if 'SQL Server' in d])"
# Should output: ['ODBC Driver 18 for SQL Server']
```

## Configuration

### 1. Create Config File

```bash
cp config.example.yaml smt.yaml
```

### 2. Edit smt.yaml

```yaml
source:
  dialect: mssql               # or postgresql
  host: your-source-host
  port: 1433                   # 5432 for postgresql
  user: sa
  password: YourPassword
  database: SourceDB
  schema: dbo

target:
  dialect: postgresql          # or mssql
  host: your-target-host
  port: 5432
  user: postgres
  password: YourPassword
  database: TargetDB

tables: all                    # or list specific tables
workspace: ./migration_workspace
```

### 3. Using Environment Variables for Passwords

For CI/CD pipelines, set passwords via environment variables instead of YAML:

```bash
export SMT_SOURCE_PASSWORD="secret_source_pw"
export SMT_TARGET_PASSWORD="secret_target_pw"
smt -c smt.yaml migrate --yes
```

These override whatever is set in the YAML file.

### Configuration Reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `source.dialect` | Yes | - | `postgresql` or `mssql` |
| `source.driver` | No | per dialect | `psycopg2`, `pyodbc` |
| `source.host` | Yes | - | Database hostname |
| `source.port` | No | 5432/1433 | Database port |
| `source.user` | Yes | - | Database username |
| `source.password` | Yes | - | Database password |
| `source.database` | Yes | - | Database name |
| `source.schema` | Yes | - | Schema to reflect |
| `target.dialect` | Yes | - | `postgresql` or `mssql` |
| `target.host` | Yes | - | Target hostname |
| `target.port` | No | 5432/1433 | Target port |
| `target.user` | Yes | - | Target username |
| `target.password` | Yes | - | Target password |
| `target.database` | Yes | - | Target database |
| `tables` | No | `all` | List of tables or `"all"` |
| `workspace` | No | `./migration_workspace` | Artifact directory |

## Docker Testing Environment

Quick setup for testing MSSQL-to-PostgreSQL migrations:

### Start Databases

```bash
# MSSQL 2022
docker run -d --name smt-mssql \
  -e 'ACCEPT_EULA=Y' \
  -e 'MSSQL_SA_PASSWORD=SmtTestPass1' \
  -p 1433:1433 \
  mcr.microsoft.com/mssql/server:2022-latest

# PostgreSQL 15
docker run -d --name smt-postgres \
  -e 'POSTGRES_PASSWORD=SmtTestPass1' \
  -p 5433:5432 \
  postgres:15
```

### Wait for Readiness

```bash
# Wait for MSSQL
until docker exec smt-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'SmtTestPass1' -C -Q "SELECT 1" 2>/dev/null; do
  sleep 2
done

# Wait for PostgreSQL
until docker exec smt-postgres pg_isready -U postgres 2>/dev/null; do
  sleep 2
done
```

### Create Databases

```bash
# Create source database in MSSQL
docker exec smt-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'SmtTestPass1' -C \
  -Q "CREATE DATABASE StackOverflow2010;"

# Create target database in PostgreSQL
docker exec smt-postgres psql -U postgres \
  -c "CREATE DATABASE stackoverflow;"
```

### Create Sample Tables

```bash
docker exec smt-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'SmtTestPass1' -C \
  -d StackOverflow2010 -Q "
CREATE TABLE dbo.Users (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    DisplayName NVARCHAR(40) NOT NULL,
    Reputation INT NOT NULL DEFAULT 1,
    CreationDate DATETIME NOT NULL,
    LastAccessDate DATETIME NOT NULL
);
CREATE TABLE dbo.Posts (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Title NVARCHAR(250) NULL,
    Body NTEXT NOT NULL,
    Score INT NOT NULL DEFAULT 0,
    PostTypeId INT NOT NULL,
    OwnerUserId INT NULL,
    CreationDate DATETIME NOT NULL,
    LastActivityDate DATETIME NOT NULL,
    CONSTRAINT FK_Posts_Users FOREIGN KEY (OwnerUserId) REFERENCES dbo.Users(Id)
);
"
```

### Configure and Migrate

```yaml
# smt.yaml
source:
  dialect: mssql
  host: localhost
  port: 1433
  user: sa
  password: SmtTestPass1
  database: StackOverflow2010
  schema: dbo

target:
  dialect: postgresql
  host: localhost
  port: 5433
  user: postgres
  password: SmtTestPass1
  database: stackoverflow

tables: all
workspace: ./migration_workspace
```

```bash
smt -c smt.yaml migrate --yes
```

### Verify

```bash
# Check tables in target
docker exec smt-postgres psql -U postgres -d stackoverflow \
  -c "\dt dw__stackoverflow2010__dbo.*"

# Check migration status
smt -c smt.yaml status
```

### Cleanup

```bash
docker stop smt-mssql smt-postgres
docker rm smt-mssql smt-postgres
rm -rf migration_workspace
```

## Running Tests

```bash
# All tests (no database required)
.venv/bin/pytest tests/ -v

# Specific module
.venv/bin/pytest tests/test_models.py -v

# With coverage
.venv/bin/pytest tests/ --cov=smt --cov-report=term-missing
```

## Troubleshooting

### Connection Refused

Verify the database is running and accessible:

```bash
# PostgreSQL
psql -h localhost -p 5432 -U postgres -c "SELECT 1"

# MSSQL (via Docker)
docker exec smt-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'YourPassword' -C -Q "SELECT 1"
```

### ODBC Driver Not Found

```
Error: pyodbc.InterfaceError: ('IM002', '[IM002] [unixODBC][Driver Manager]Data source name not found')
```

Install the Microsoft ODBC Driver 18 (see instructions above), then verify:

```bash
python -c "import pyodbc; print(pyodbc.drivers())"
```

### Password Authentication Failed

- Check for special characters in passwords that may need shell escaping
- Use `SMT_SOURCE_PASSWORD` / `SMT_TARGET_PASSWORD` env vars to avoid shell issues
- Verify credentials work directly: `psql -h host -U user -d database`

### Schema Name Too Long

```
ConfigError: Derived target schema 'dw__verylongdatabasename__verylongschema' is 45 chars,
exceeding postgresql identifier limit of 63.
```

Use shorter source database or schema names. The derived name is `dw__` + database + `__` + schema (all lowercase).
