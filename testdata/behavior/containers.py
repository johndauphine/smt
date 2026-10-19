"""Disposable pinned engines. Python orchestration and Go clients run on Linux.

WSLC's Windows CLI is a transport to its Linux ARM64 VM. No existing containers,
bind mounts, host ports, configurable database endpoints, or implicit pulls.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
import uuid

EDGE_IMAGE = 'mcr.microsoft.com/azure-sql-edge@sha256:3544cf1c6847c5cb60e071638c48b1b45d05a64eb2d88d0f83d44b397f40b186'
PG_IMAGE = 'postgres@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826'
SOURCE_SETTINGS = '''SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS ON;
SET ANSI_WARNINGS ON; SET QUOTED_IDENTIFIER ON; SET ANSI_PADDING ON;
SET CONCAT_NULL_YIELDS_NULL ON; SET NUMERIC_ROUNDABORT OFF; SET ARITHABORT ON;'''
TARGET_SETTINGS = "SET TIME ZONE 'UTC'; SET DateStyle = 'ISO, YMD';"
COLLATION = 'Latin1_General_100_BIN2'


class DatabaseSetupFailure(RuntimeError):
    def __init__(self, result):
        super().__init__('database setup failure: ' + json.dumps(result))
        self.result = result


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class Engine:
    def __init__(self, engine, client, executable=None, postgres_loopback=False):
        if os.name != 'posix':
            raise RuntimeError('run this harness in native Linux/WSL')
        self.engine, self.client = engine, Path(client)
        self.image = EDGE_IMAGE if engine == 'mssql' else PG_IMAGE
        self.user = 'mssql' if engine == 'mssql' else 'postgres'
        self.executable = executable or shutil.which('wslc') or shutil.which('wslc.exe') or '/mnt/c/Program Files/WSL/wslc.exe'
        self.token = uuid.uuid4().hex
        self.name = 'smt-behavior-' + self.token
        self.container_id = None
        self.removed = False
        self.metadata = {}
        self.postgres_loopback = postgres_loopback

    def command(self, *args, text=None, timeout=180):
        # Never include command-line env/password values in raised exceptions.
        result = subprocess.run([self.executable, *args], input=text, text=True,
                                encoding='utf-8', capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('container operation failed: ' + str(args[0]) + ': ' + result.stderr[-1500:])
        return result.stdout

    def inspect(self):
        return json.loads(self.command('inspect', '--type', 'container', '--format', 'json', self.container_id))[0]

    def query(self, steps, setup=False, admin=False, settings=True):
        request = {'engine': self.engine, 'steps': steps, 'setup': setup, 'admin': admin,
                   'settings': (SOURCE_SETTINGS if self.engine == 'mssql' else TARGET_SETTINGS) if settings else ''}
        result = self.command('exec', '-i', '--user=' + self.user, self.container_id,
                              '/tmp/smt-behavior-client', text=json.dumps(request))
        return json.loads(result)

    def require(self, sql, admin=False):
        r = self.query([{'sql': sql}], setup=True, admin=admin)
        if not r['ok']:
            raise DatabaseSetupFailure(r)
        return r

    def upload(self, content, path):
        if path not in ('/tmp/smt-behavior-client', '/tmp/smt-behavior-lifecycle', '/tmp/smt', '/tmp/smt-config.yaml'):
            raise ValueError('upload path is not harness owned')
        self.command('exec', '-i', '--user=' + self.user, self.container_id, 'sh', '-c',
                     'umask 077; base64 -d > ' + path + ' && chmod 700 ' + path,
                     text=base64.b64encode(content).decode('ascii'))

    def __enter__(self):
        if not self.client.is_file():
            raise RuntimeError('build the Linux ARM64 client first')
        self.metadata['runtime'] = self.command('version').strip()
        image = json.loads(self.command('image', 'inspect', '--format', 'json', self.image))[0]
        self.metadata.update(image=self.image, image_id=image['Id'], image_digests=image.get('RepoDigests', []),
                             client_sha256=digest(self.client.read_bytes()))
        args = ['create', '--pull=never', '--name', self.name, '--label', 'smt.behavior.owner=' + self.token,
                '--network=none', '--memory=' + ('4g' if self.engine == 'mssql' else '768m'),
                '--cpus=2', '--user=' + self.user]
        if self.engine == 'mssql':
            args += ['--env', 'ACCEPT_EULA=Y', '--env', 'MSSQL_PID=Developer', '--env',
                     'MSSQL_SA_PASSWORD=SMT!' + secrets.token_hex(24), '--env', 'MSSQL_MEMORY_LIMIT_MB=2048']
        else:
            args += ['--env', 'POSTGRES_HOST_AUTH_METHOD=trust', '--env', 'PGDATA=/tmp/smt-behavior-data']
        args += [self.image]
        if self.engine == 'postgres':
            args += ['-c', 'listen_addresses=' + ('127.0.0.1' if self.postgres_loopback else ''), '-c', 'statement_timeout=30000']
        self.container_id = self.command(*args).strip()
        try:
            info = self.inspect()
            host = info['HostConfig']
            if (host['NetworkMode'] != 'none' or host.get('PortBindings') or host.get('Binds')
                    or any(m['Type'] == 'bind' for m in info.get('Mounts', []))
                    or info['Config']['User'] != self.user
                    or info['Config']['Labels'].get('smt.behavior.owner') != self.token):
                raise RuntimeError('container isolation validation failed')
            self.metadata['isolation'] = {'network': 'none', 'user': self.user, 'host_bind_mounts': False, 'published_ports': False}
            if self.engine == 'postgres':
                self.metadata['isolation']['database_listener'] = 'container loopback only' if self.postgres_loopback else 'Unix socket only'
            self.metadata['container_id'] = self.container_id
            self.command('start', self.container_id)
            self.upload(self.client.read_bytes(), '/tmp/smt-behavior-client')
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                ready = ('Recovery is complete.' in self.command('logs', self.container_id)) if self.engine == 'mssql' else (self.command('exec', self.container_id, 'cat', '/proc/1/comm').strip() == 'postgres')
                if ready:
                    r = self.query([{'sql': "SELECT 'ready' AS value"}], admin=True, settings=False)
                    if r['ok']:
                        break
                if not self.inspect()['State'].get('Running'):
                    raise RuntimeError('engine stopped before readiness')
                time.sleep(1)
            else:
                raise RuntimeError('engine readiness timed out')
            if self.engine == 'mssql':
                self.require('CREATE DATABASE smt_behavior COLLATE ' + COLLATION, admin=True)
                self.require('ALTER DATABASE smt_behavior SET COMPATIBILITY_LEVEL=150', admin=True)
                query = "SELECT @@VERSION AS version, CONVERT(varchar(128),SERVERPROPERTY('ProductVersion')) AS product_version, CONVERT(varchar(128),DATABASEPROPERTYEX('smt_behavior','Collation')) AS collation, CONVERT(varchar(20),@@OPTIONS) AS session_options, compatibility_level FROM sys.databases WHERE name='smt_behavior'"
            else:
                self.require('CREATE DATABASE smt_behavior', admin=True)
                query = "SELECT version() AS version, current_setting('server_version') AS product_version, (SELECT datcollate FROM pg_database WHERE datname=current_database()) AS collation, current_setting('TimeZone') AS timezone, current_setting('DateStyle') AS datestyle"
            self.metadata['server'] = self.query([{'sql': query}])
            if not self.metadata['server']['ok']:
                raise RuntimeError('engine metadata failed: ' + json.dumps(self.metadata['server']))
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        if not self.container_id:
            return
        if self.inspect()['Config']['Labels'].get('smt.behavior.owner') != self.token:
            raise RuntimeError('ownership mismatch: refusing cleanup')
        identifier = self.container_id
        try:
            self.command('remove', '--force', '--volumes', identifier)
        except RuntimeError:
            # WSLC can miss a killed container's exit event. Non-force removal
            # safely refuses a live container; retry only the same owned ID.
            if self.inspect()['Config']['Labels'].get('smt.behavior.owner') != self.token:
                raise RuntimeError('ownership mismatch during cleanup retry')
            self.command('remove', '--volumes', identifier)
        remaining = [json.loads(line) for line in self.command('container', 'list', '--all', '--format', 'json').splitlines() if line.strip()]
        if any(c.get('Id', c.get('ID', '')).startswith(identifier[:12]) for c in remaining):
            raise RuntimeError('removed container still listed')
        self.container_id = None
        self.removed = True

    def __exit__(self, *_):
        self.close()
