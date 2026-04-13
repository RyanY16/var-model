# VaR pipeline

This repo can now run as a script pipeline instead of requiring `var.ipynb` and `results.ipynb` manually.

## Flow

1. `scripts/calculate_var.py` generates dated folders under `outputs/YYYYMMDD/`.
2. `scripts/combine_outputs.py` rebuilds the combined CSVs under `outputs/combined/`.
3. `scripts/sync_supabase.py` upserts those CSVs into Supabase.
4. `scripts/run_pipeline.py` gives you one command for either daily or full-history runs.

## Supabase setup

Create the tables in Supabase by running [supabase/schema.sql](/Users/ryany/Documents/Code/var/supabase/schema.sql).

Create a real `.env` file locally:

```bash
cp .env.example .env
```

Then fill in:

```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

`SUPABASE_SERVICE_ROLE_KEY` needs to be the service role key from Supabase, not the publishable key.

## Commands

Run the just-passed business day only.
Example: if the job runs on April 14, 2026 in `Asia/Tokyo`, it will calculate April 13, 2026.

```bash
python3 scripts/run_pipeline.py --mode daily
```

Recalculate one specific date only:

```bash
python3 scripts/calculate_var.py --start-date 2026-04-13 --end-date 2026-04-13
python3 scripts/combine_outputs.py
python3 scripts/sync_supabase.py
```

Run the full history from the configured start date through the previous business day:

```bash
python3 scripts/run_pipeline.py --mode all --start-date 2025-12-01
```

Recalculate all dates again for a fixed range:

```bash
python3 scripts/calculate_var.py --start-date 2025-12-01 --end-date 2026-04-13
python3 scripts/combine_outputs.py
python3 scripts/sync_supabase.py
```

Only combine existing outputs and preview what would upload:

```bash
python3 scripts/run_pipeline.py --mode daily --skip-calc --dry-run-sync
```

Run just the upload step:

```bash
python3 scripts/sync_supabase.py
```

Upload only the summary-style tables first:

```bash
python3 scripts/sync_supabase.py --tables risk_metrics marginal_risk portfolio_breakdown stock_prices
```

## GitHub Actions

- [daily-sync.yml](/Users/ryany/Documents/Code/var/.github/workflows/daily-sync.yml) runs automatically on weekdays and can also be triggered manually.
- [backfill-all.yml](/Users/ryany/Documents/Code/var/.github/workflows/backfill-all.yml) is a manual full-history run for backfills.

Add these GitHub repository secrets:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
```

The scheduled workflow is set to `0 11 * * 1-5` in UTC, which is 8:00 PM JST on weekdays.

## Local scheduling

If you prefer running it on your Mac instead of GitHub Actions, use `crontab -e` and add:

```cron
0 20 * * 1-5 cd /Users/ryany/Documents/Code/var && /Users/ryany/Documents/Code/var/.venv/bin/python scripts/run_pipeline.py --mode daily >> /tmp/var_pipeline.log 2>&1
```

## Notes

- The notebooks are still there, but the scripts are the automation-friendly path now.
- Supabase uploads use REST upserts, so rerunning the pipeline updates existing dates instead of duplicating them.
- The database uses snake_case column names even where the CSV uses names like `Portfolio Value`.
- `pnls` is the largest table by far, so you may want to skip that table initially for a lighter dashboard database.
- `company_lookup` in the calculation code is just an in-memory lookup built from `portfolio.csv` so grouped positions can keep their `company_name`.
