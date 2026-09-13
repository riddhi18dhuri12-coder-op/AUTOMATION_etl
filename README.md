# Automated ETL Pipeline

A config-driven, extensible ETL framework in Python. Pulls from mixed
sources (CSV/Excel, SQL databases, REST APIs), applies composable
transformations, and loads into a data warehouse (Snowflake or
BigQuery) — fully automatable on a schedule.

## Why this design

- **Config-driven** (`config.yaml`): sources, transformations, and
  destination are all declared in YAML. Add a new data source or
  change a transform without touching code.
- **Pluggable**: extractors, transform ops, and loaders are each
  registered in a dict-based registry, so adding a new source type
  or warehouse is a ~20 line addition, not a rewrite.
- **Incremental extraction**: SQL sources support cursor-based
  incremental pulls (e.g. `updated_at >= last_run`), tracked in a
  local state file, so re-runs don't reprocess everything.
- **Production concerns built in**: retries with exponential backoff
  on network calls, structured logging to file + console, failure
  notifications via webhook, append/replace/merge (upsert) load
  modes.
- **Automation**: `scheduler.py` runs the pipeline on a cron or
  interval schedule as a long-lived process (Docker/systemd-friendly).

## Project structure

```
etl_pipeline/
├── config/
│   └── config.example.yaml   # copy to config.yaml and edit
├── etl/
│   ├── extractors.py         # CSV / Excel / SQL / API extractors
│   ├── transformers.py       # composable transform operations
│   ├── loaders.py            # Snowflake / BigQuery loaders
│   ├── pipeline.py           # orchestrates extract -> transform -> load
│   ├── scheduler.py          # automated recurring runs (cron/interval)
│   └── utils/
│       ├── config_loader.py  # YAML + env var interpolation
│       ├── logger.py         # rotating file + console logging
│       └── state.py          # incremental-extraction cursor tracking
├── main.py                   # CLI entry point
├── requirements.txt
└── .env.example              # secrets template
```

## Setup

```bash
pip install -r requirements.txt
cp config/config.example.yaml config/config.yaml
cp .env.example .env
# edit config.yaml and .env with your real sources/credentials
```

Only install the warehouse SDK you actually need
(`snowflake-connector-python` or `google-cloud-bigquery`) if you want
to trim the dependency footprint.

## Usage

Run once, on demand:
```bash
python main.py run
```

Run automatically on a schedule (long-running process):
```bash
python main.py schedule
```
Schedule is defined in `config.yaml` under `schedule:` (cron or
interval). Deploy this as a systemd service, Docker container, or
Kubernetes CronJob for real automation.

Use a non-default config path:
```bash
python main.py run --config /path/to/other_config.yaml
```

## Adding a new source

1. If it's a genuinely new *type* (not CSV/SQL/API), add a class to
   `etl/extractors.py` implementing `extract() -> pd.DataFrame` and
   register it in `EXTRACTOR_REGISTRY`.
2. Add an entry under `sources:` in `config.yaml`.
3. Add any transform steps under `transformations:` keyed by the
   source name.
4. Map the source to a destination table under `destination.*.table_map`.

## Adding a new transform operation

Add a function to `etl/transformers.py` with signature
`(df, step, logger) -> df`, then register it in `OP_REGISTRY`. It's
then usable from config as `- op: your_new_op`.

## Testing without live credentials

`test_smoke.py` runs the real extract + transform pipeline against
the sample CSV in `data/incoming/`, substituting a `MockLoader` that
writes to `data/staging/` instead of a real warehouse — useful for
verifying pipeline logic changes before wiring up real infra:

```bash
python test_smoke.py
```

## Running on GitHub (no server needed)

`.github/workflows/etl.yml` runs the pipeline on GitHub's own cron
scheduler via GitHub Actions — no separate server required.

1. Push this repo to GitHub.
2. In repo **Settings → Secrets and variables → Actions**, add each
   value from `.env.example` as a secret (`DB_PASSWORD`,
   `SNOWFLAKE_PASSWORD`, etc.).
3. Edit the `cron:` line in the workflow file to match your desired
   schedule (UTC time).
4. Commit `config/config.yaml` to the repo (secrets stay in GitHub
   Secrets, not the file — the `${VAR_NAME}` placeholders resolve
   from the environment at runtime either way).
5. Push. The workflow also has a manual "Run workflow" button under
   the Actions tab (`workflow_dispatch`) for on-demand runs.

**Trade-offs vs. `python main.py schedule` on a real server:**
- GitHub Actions free tier caps at 2,000 minutes/month (private
  repos) — fine for periodic batch jobs, not for anything running
  every few minutes continuously.
- No state persists between runs unless you commit `data/state.json`
  back to the repo or use `actions/cache` — matters if you're relying
  on incremental SQL extraction.
- Best fit for scheduled batch ETL (hourly/daily). For near-real-time
  or long-running pipelines, a real server/container (Docker + cron,
  ECS, Cloud Run Jobs) is a better fit.

## Notes / production hardening ideas

- Add `pandera` schema validation per source before loading (library
  is already in requirements.txt).
- Swap the JSON state file for a small database table if running
  multiple pipeline instances concurrently.
- Add dead-letter handling for rows that fail transformation instead
  of dropping/erroring the whole batch.
- Wire `notifications.on_failure.webhook_url` to Slack/PagerDuty/etc.
