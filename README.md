# Lakebase reverse-sync lab

[![ci](https://github.com/suriyalakshmi-chandrasekar/lakebase-reverse-sync-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/suriyalakshmi-chandrasekar/lakebase-reverse-sync-lab/actions/workflows/ci.yml)
![postgres](https://img.shields.io/badge/Postgres-16-336791) ![spark](https://img.shields.io/badge/Spark-3.5-orange) ![delta](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD4) ![license](https://img.shields.io/badge/license-MIT-green)

**Let users edit data in an app served from Databricks Lakebase, and get those edits back into your Delta Gold tables correctly.**

Serving Gold data to an app through Lakebase (managed Postgres) is easy with **synced tables**. The hard part is the way back: users change values in the app, and those changes have to reach Gold without being overwritten by the next sync, without losing edits, and without the app showing stale data in between. This repo is a working, tested implementation of the pattern.

## The problem

- A synced table is **owned by the sync pipeline**. If the app writes to it directly, the next refresh overwrites the edit.
- Users expect to **see their own change immediately**, but Gold updates asynchronously.
- Two consumers usually need the edits: the **Gold MERGE** and an **audit changelog** for the UI history. Neither should wait on the other.
- Cleaning up processed edits must never **delete an edit that hasn't been consumed**, or one that the synced table doesn't reflect yet.

## The pattern

```mermaid
flowchart LR
    G[(Gold Delta)] -->|synced table| S[store_inventory_synced]
    subgraph Lakebase Postgres
      S --> V[store_inventory_current<br/>unified view]
      C[store_inventory_changes<br/>append-only, REPLICA IDENTITY FULL] --> V
      F[apply_inventory_edit] --> C
    end
    APP[App API] -->|reads| V
    APP -->|writes| F
    C -->|Lakebase Change Data Feed| CDF[(CDF Delta table)]
    CDF --> R1[consumer: gold<br/>MERGE latest per key] --> G
    CDF --> R2[consumer: changelog<br/>one row per changed column] --> L[(ui_changelog)]
    R1 --> P[(progress)]
    R2 --> P
    P -->|min of both + synced refreshed| RET[retire_inventory_changes]
    RET --> C
```

| Piece | What it does |
|---|---|
| **Change table** | Every edit is appended: the full row after the edit, `change_type` (I/U/D), `previous_data` (a JSON snapshot of what the user saw), who and when. `REPLICA IDENTITY FULL` so the change feed carries old values. |
| **Unified view** | What the app reads: synced rows with the latest edit per key on top. Deletes hide the row, and `has_pending_edit` shows what Gold hasn't absorbed yet. Users see their own writes instantly. |
| **Write function** | The only write path. It validates (no insert of an existing row, no update or delete of a missing one), snapshots `previous_data`, and stores the full row so two edits to different columns never lose each other. |
| **Gold consumer** | Takes the latest change per key in each batch, then MERGE: `I` inserts, `U` updates, `D` deletes. Idempotent, so replaying a batch changes nothing. |
| **Changelog consumer** | One audit row per *really changed* column (old → new), rebuilt from the change row itself with **no lookup of Gold**, so it has no timing dependency on the Gold consumer. Idempotent by `change_id`. |
| **Progress + retirement** | Each consumer records the last `change_id` it processed. Change rows are deleted only when **both** consumers passed them **and** the synced table refreshed after them. They're deleted by id range, never `TRUNCATE`, so edits that arrive mid-cleanup survive. |

## Repository

```
sql/postgres/01_change_capture.sql   change table, unified view, write + retire functions
src/reversesync/
  changes.py     pure reference rules: apply edits, changed columns, retire point
  resolve.py     Spark: resolve_to_gold, build/write_changelog, Progress, compare
  app.py         FastAPI app: GET / POST / PATCH / DELETE through the view and write function
databricks/resolve_stream.py         streaming job over the Lakebase CDF table (one per consumer)
tests/
  unit/        pure rules
  postgres/    view, write function, validation, retirement, API (real Postgres)
  e2e/         app edits -> change feed -> Spark -> Gold + changelog -> retirement
```

## Run the tests

```bash
pip install -e ".[app,spark,dev]"
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres   # any Postgres 14+
pytest -v
```

CI runs all three layers on every push, with a Postgres 16 service and local Spark plus Delta. The end-to-end test makes 79 edits (partial updates, repeat edits of the same row, inserts, deletes), resolves them, and checks:

- Gold equals the app view: no missing rows, extra rows or value differences
- Gold also equals the **pure-Python reference rules**
- the changelog has exactly the real changes, and nothing is logged twice on replay
- after the forward sync and retirement, the change table is empty and the app view is unchanged

## Run the app

```bash
DATABASE_URL=... uvicorn reversesync.app:app --reload
curl -X PATCH localhost:8000/inventory/SKU0001/ST01 -H 'content-type: application/json' -d '{"on_hand_qty": 7}'
```

On Lakebase, connect with an OAuth token as the password and refresh it before it expires.

## Design notes

- **Full row in each change.** Storing only the changed columns looks leaner, but then "latest change wins" loses the earlier edit when a row is edited twice. Full rows keep the view, the Gold MERGE and the reference rules in agreement.
- **Two independent consumers.** The changelog never reads Gold, so the two jobs can run on different schedules and fail independently.
- **Two-condition retirement.** "Both streams processed it" isn't enough: until the synced table refreshes, the view still needs the change row, or it would briefly show the old value.

## Timeline

I designed, benchmarked and validated this architecture in **May–Oct 2026**, while moving a multi-tenant search serving layer onto Databricks Lakebase. In that work, all 160 of 160 validation edits reached Gold, in 1 minute 42 seconds on average. Along the way I compared it with the alternatives (WAL2Delta, trigger-based sync and logical replication). This repository is a **generic open-source rebuild (Nov 2026)**, written from scratch with synthetic retail data and no client code.

See also: [lakebase-skills](https://github.com/suriyalakshmi-chandrasekar/lakebase-skills), my cited reference skill for Databricks Lakebase.

## Author

**Suriyalakshmi Chandrasekar**: Technical Lead, Data Engineering · Databricks Certified Data Engineer Professional
[LinkedIn](https://www.linkedin.com/in/suriyalakshmi-chandrasekar) · [GitHub](https://github.com/suriyalakshmi-chandrasekar)

Not an official Databricks project. MIT licensed.
