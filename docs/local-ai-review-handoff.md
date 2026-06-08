# Local AI Review Handoff

This guide captures the repeatable test path for evaluating SMT's optional AI
DDL review on faster local hardware, especially a MacBook Pro with enough unified
memory to run larger local models.

## Goal

SMT should be able to generate migration DDL deterministically from source
metadata, then optionally ask an AI reviewer to inspect that DDL before it is
applied. The deterministic path must work without AI. The AI path is a quality
gate and triage aid, not the source of truth.

For model testing, measure both:

- Correctness: deterministic verification passes after the migration.
- Reviewer usefulness: warnings are valid, parseable, and specific.
- Throughput: wall-clock time for a complete CRM migration.
- Noise: false-positive warnings and malformed reviewer output.

The current CRM fixture mostly tests false positives because the generated DDL is
expected to be correct. To measure recall, add intentionally mutated bad-DDL
cases later.

## Current Windows/WSL Baseline

These runs used the fabricated CRM MSSQL fixture migrated to PostgreSQL.

| Reviewer | Result | Notes |
| --- | --- | --- |
| Anthropic Sonnet | Deep verification passed | Warnings were mostly false positives around equivalent UTC defaults and omitted `NO ACTION` clauses. |
| `openai/gpt-oss-20b` via LM Studio | Deep verification passed in about 5m52s | 14 warning events. Current best local candidate on this laptop. |
| `google/gemma-4-e4b` via LM Studio | Deep verification passed in about 6m25s | 34 warning events. More noisy than GPT-OSS 20B. |
| `google/gemma-4-12b-qat` via LM Studio | Not practical on this laptop | Very slow and repeatedly consumed completion budget on reasoning with no usable output at 2048, 4096, and 8192 max tokens. |

Known GPT-OSS baseline artifact:

- Target DB: `smt_crm_gptoss20b_20260608_041606`
- Log: `/tmp/smt-crm-gptoss20b-20260608-041606.log`
- DDL artifacts: `/tmp/smt-crm-gptoss20b-20260608-041606-data/runs/01222b51-8b62-41f6-adf2-020972df834d/ddl`

## Mac Setup Notes

SQL Server containers can be awkward on Apple Silicon. Before spending time on
model tuning, pick the source database strategy:

- Easiest: keep SQL Server running on Windows/WSL or another x86 host, then point
  the Mac SMT config at that host.
- Good alternative: use Azure SQL or another reachable SQL Server instance.
- Riskier: run SQL Server locally on the Mac. Verify the container/runtime works
  on Apple Silicon before assuming this path.

PostgreSQL should run locally on the Mac without issue.

## Clone And Build

```bash
mkdir -p ~/repos
cd ~/repos
git clone https://github.com/johndauphine/smt.git
cd smt
go test ./...
```

## LM Studio Configuration

Start LM Studio's local server and load the model under test. A typical local URL
is:

```text
http://127.0.0.1:1234
```

Use the LM Studio UI to increase context length before running reviewer tests.
For CRM review prompts, start with at least 32768 tokens. Larger models may need
more context, but max completion tokens also matters because some reasoning
models can spend the whole budget internally.

Do not commit secrets. Put local AI configuration in `~/.secrets/smt-config.yaml`.

Example LM Studio reviewer config:

```yaml
ai:
  default_provider: lmstudio
  providers:
    lmstudio:
      provider_type: openai
      endpoint: http://127.0.0.1:1234/v1
      api_key: local
      model: openai/gpt-oss-20b
      model_temperature: 1
      max_tokens: 8192
      timeout_seconds: 240
```

For Anthropic comparison runs, keep that provider in the same secrets file and
switch `default_provider` to `anthropic`.

## Suggested Model Matrix

| Model | Starting settings | What to watch |
| --- | --- | --- |
| `openai/gpt-oss-20b` | context 32768+, `max_tokens: 8192`, temperature 1, concurrency 2 then 4 | Best Windows baseline so far. Check warning validity and total time. |
| `google/gemma-4-e4b` | context 32768+, `max_tokens: 4096`, temperature 0, concurrency 4 | Fast enough, but noisy on Windows. |
| `google/gemma-4-12b-qat` | context 32768+, `max_tokens: 8192`, concurrency 1 | Re-test only on faster hardware. Watch for no-output reasoning failures. |
| `google/gemma-4-31b-qat` | smoke test one or two tables first, then full CRM if parseable | Use the Mac memory headroom, but do not start with high concurrency. |

After a model is stable, increase reviewer concurrency and compare wall-clock
time. Keep a log of context length, max tokens, GPU/offload settings, and model
quantization.

## CRM Run Protocol

1. Verify the model endpoint:

   ```bash
   curl http://127.0.0.1:1234/v1/models
   ```

2. Create or refresh the CRM MSSQL fixture and an empty PostgreSQL target.

3. Run an SMT CRM migration with AI review enabled. Capture stdout/stderr:

   ```bash
   time smt create path/to/crm-config.yaml 2>&1 | tee /tmp/smt-crm-model.log
   ```

4. Count AI warnings and parser failures:

   ```bash
   grep -c "AI DDL review warning" /tmp/smt-crm-model.log || true
   grep -Ei "parse|json|malformed|no output" /tmp/smt-crm-model.log || true
   ```

5. Run column verification:

   ```bash
   MSSQL_CONTAINER=dmt-wsl-mssql \
   PG_CONTAINER=dmt-wsl-postgres \
   testdata/crm/verify_columns.sh CRM_MSSQL_SMT dbo smt_crm_target public
   ```

6. Run deep verification:

   ```bash
   MSSQL_CONTAINER=dmt-wsl-mssql \
   PG_CONTAINER=dmt-wsl-postgres \
   testdata/crm/verify_deep_mssql_to_postgres.sh CRM_MSSQL_SMT dbo smt_crm_target public
   ```

## Warning Triage

Treat these as likely false positives unless a deterministic check also fails:

- `GETUTCDATE()` mapped to `(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')`.
- Foreign keys that omit explicit `ON UPDATE NO ACTION` or `ON DELETE NO ACTION`.
- System-generated MSSQL primary-key names differing from deterministic
  PostgreSQL primary-key names.
- MSSQL `dbo` names normalized to PostgreSQL `public` and lowercase identifiers.

Treat these as real issues until verified:

- Dropped or changed nullability.
- Missing defaults, especially non-identity defaults.
- Timestamp or timezone class changes.
- Missing primary keys, unique indexes, foreign keys, check constraints, or
  generated columns.
- Reviewer JSON parse failures or empty reviewer output. These are model failures
  even when the deterministic migration succeeds.

## Scorecard Template

| Date | Machine | Model | Quant | Context | Max tokens | Concurrency | Duration | Deep check | Warnings | Valid warnings | Parse failures | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  |  |  |  |  |  |  |  | pass/fail |  |  |  |  |
