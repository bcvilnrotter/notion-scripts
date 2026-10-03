# personal-lab

Scheduled GitHub Actions jobs that pull personal data from third-party APIs
into Notion databases. No server, no local runtime — every pipeline is a
cron'd workflow calling a root-level Python script.

## Pipelines

| Workflow | Entrypoint | Schedule (UTC) | Source → Notion |
|---|---|---|---|
| iNaturalist Database Update | `update_inaturalist_db.py` | `0 0 * * *` daily | observations, species dictionary |
| MAL Daily Record Push | `add_daily_mal_records.py` | `0 23 * * *` daily | anime entries + daily records |
| Steam Playtime Sync | `add_2week_playtime.py` | `0 23 * * *` daily | 2-week playtime → raw + stats DBs |
| Zotero Authors Verification | `update_zotero_records.py` | `0 0 * * 0` weekly | library items, author reconciliation |
| Perigon Institution Enrichment | `enrich_institutions_perigon.py`, `update_perigon_stories.py` | `0 0 1 * *` monthly | institution metadata + news stories |
| Duolingo Daily Sync | `add_duolingo_daily.py` | **manual only** | streak, courses, calendar skills, dictionary |

## Layout

- `*.py` at root — one entrypoint per pipeline (`add_`, `update_`, `enrich_`)
- `dry_*.py` — read-only counterparts; hit the source API, skip Notion writes
- `utils/` — per-source API clients (`steam_functions.py`, `duolingo_functions.py`, …)
  plus `utils/notion/` for shared Notion helpers
- `.github/workflows/` — one file per pipeline

## Running locally

```bash
pip install -r requirements.txt
python dry_2week_playtime.py
```

Secrets are read from the environment (`python-dotenv` locally, repo secrets in
Actions). Every pipeline needs `NOTION_TOKEN` plus the database IDs listed in
its workflow file.

## CI

- `pytest.yml` — runs on every push and PR against Python 3.10 and 3.11
- `dry_runs_test.yml` — manual dispatch; exercises the dry-run scripts in CI

## Known gaps

Tracked rather than hidden:

- `pytest.yml` doesn't actually run pytest — it executes one dry-run script.
  There is no `tests/` directory yet.
- Dry-run coverage is partial: Steam, Duolingo, and MAL only. iNaturalist,
  Zotero, and Perigon have no read-only path.
- `validate_game_stats.py` and `update_duolingo_dictioanry_database.py` (sic)
  are not wired to any workflow.
- `utils/__pycache__` is committed; the repo has no `.gitignore`.
- `requirements.txt` is a raw `pip freeze`, including Windows-only
  `pywin32-ctypes`.
- Python version is inconsistent across workflows (3.10 vs 3.11).
- GitHub disables scheduled workflows after 60 days with no repo commits.

See #39 for the consolidation work addressing most of these.
