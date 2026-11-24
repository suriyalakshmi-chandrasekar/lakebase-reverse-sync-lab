-- =====================================================================
-- Lakebase (Postgres) side of the reverse-sync pattern.
--
--   store_inventory_synced   read-only copy of Gold, kept fresh by a Lakebase
--                            synced table (created from Databricks). Here it is
--                            a plain table so the pattern runs on any Postgres.
--   store_inventory_changes  every user edit, append-only. Lakebase Change Data
--                            Feed streams this table back to Delta.
--   store_inventory_current  what the app reads: synced rows with the latest
--                            edit applied on top.
--   apply_inventory_edit()   the only write path the app uses.
--
-- Why not write to the synced table? A synced table is owned by the sync
-- pipeline; any direct write is overwritten on the next refresh. Edits go to
-- a separate change table and the view merges them, so the app sees its own
-- writes immediately while Gold catches up asynchronously.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.store_inventory_synced (
    sku             text        NOT NULL,
    store_id        text        NOT NULL,
    on_hand_qty     integer,
    reorder_point   integer,
    safety_stock    integer,
    supplier_id     text,
    lead_time_days  integer,
    status          text,
    PRIMARY KEY (sku, store_id)
);

CREATE TABLE IF NOT EXISTS app.store_inventory_changes (
    change_id       bigserial   PRIMARY KEY,
    sku             text        NOT NULL,
    store_id        text        NOT NULL,
    -- same business columns as the synced table; each change row holds the FULL row
    -- after the edit, so "latest change per key wins" is always correct
    on_hand_qty     integer,
    reorder_point   integer,
    safety_stock    integer,
    supplier_id     text,
    lead_time_days  integer,
    status          text,
    change_type     char(1)     NOT NULL CHECK (change_type IN ('I', 'U', 'D')),
    previous_data   jsonb,      -- the row as the user saw it before the edit (audit + changelog)
    updated_by      text        NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- CDF / logical replication needs old values for updates and deletes.
ALTER TABLE app.store_inventory_changes REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS store_inventory_changes_key_idx
    ON app.store_inventory_changes (sku, store_id, change_id DESC);

-- ---------------------------------------------------------------------
-- Unified view: latest edit per key wins; partial updates fall back to the
-- synced value column by column; deletes hide the row.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW app.store_inventory_current AS
WITH latest AS (
    SELECT DISTINCT ON (sku, store_id) *
    FROM app.store_inventory_changes
    ORDER BY sku, store_id, change_id DESC
)
SELECT
    COALESCE(l.sku, s.sku)                              AS sku,
    COALESCE(l.store_id, s.store_id)                    AS store_id,
    COALESCE(l.on_hand_qty,    s.on_hand_qty)           AS on_hand_qty,
    COALESCE(l.reorder_point,  s.reorder_point)         AS reorder_point,
    COALESCE(l.safety_stock,   s.safety_stock)          AS safety_stock,
    COALESCE(l.supplier_id,    s.supplier_id)           AS supplier_id,
    COALESCE(l.lead_time_days, s.lead_time_days)        AS lead_time_days,
    COALESCE(l.status,         s.status)                AS status,
    (l.change_id IS NOT NULL)                           AS has_pending_edit
FROM app.store_inventory_synced s
FULL OUTER JOIN latest l
  ON l.sku = s.sku AND l.store_id = s.store_id
WHERE COALESCE(l.change_type, 'U') <> 'D';

-- ---------------------------------------------------------------------
-- Write path. Snapshots the current row into previous_data, then records the
-- full row after the edit: for updates, columns the caller did not send keep
-- their current value. Two edits of different columns therefore never lose
-- each other, in the view or in Gold.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION app.apply_inventory_edit(
    p_sku text, p_store_id text, p_change_type char(1), p_values jsonb, p_user text
) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    v_prev jsonb;
    v_id   bigint;
BEGIN
    SELECT to_jsonb(c) - 'has_pending_edit' INTO v_prev
    FROM app.store_inventory_current c
    WHERE c.sku = p_sku AND c.store_id = p_store_id;

    IF p_change_type = 'I' AND v_prev IS NOT NULL THEN
        RAISE EXCEPTION 'row % / % already exists', p_sku, p_store_id;
    ELSIF p_change_type IN ('U', 'D') AND v_prev IS NULL THEN
        RAISE EXCEPTION 'row % / % does not exist', p_sku, p_store_id;
    END IF;

    INSERT INTO app.store_inventory_changes
        (sku, store_id, on_hand_qty, reorder_point, safety_stock, supplier_id,
         lead_time_days, status, change_type, previous_data, updated_by)
    VALUES
        (p_sku, p_store_id,
         COALESCE((p_values ->> 'on_hand_qty')::integer, CASE WHEN p_change_type = 'U' THEN (v_prev ->> 'on_hand_qty')::integer END),
         COALESCE((p_values ->> 'reorder_point')::integer, CASE WHEN p_change_type = 'U' THEN (v_prev ->> 'reorder_point')::integer END),
         COALESCE((p_values ->> 'safety_stock')::integer, CASE WHEN p_change_type = 'U' THEN (v_prev ->> 'safety_stock')::integer END),
         COALESCE(p_values ->> 'supplier_id', CASE WHEN p_change_type = 'U' THEN v_prev ->> 'supplier_id' END),
         COALESCE((p_values ->> 'lead_time_days')::integer, CASE WHEN p_change_type = 'U' THEN (v_prev ->> 'lead_time_days')::integer END),
         COALESCE(p_values ->> 'status', CASE WHEN p_change_type = 'U' THEN v_prev ->> 'status' END),
         p_change_type, v_prev, p_user)
    RETURNING change_id INTO v_id;
    RETURN v_id;
END;
$$;

-- ---------------------------------------------------------------------
-- Retirement. A change row can go when BOTH are true:
--   1. both downstream streams (Gold MERGE and the changelog) processed it
--      -> change_id <= p_processed_up_to (the MIN of the two streams' progress)
--   2. the forward sync refreshed the synced table AFTER the edit landed in
--      Gold -> updated_at < p_synced_as_of
-- Otherwise the view would briefly fall back to a stale synced value.
-- Delete by range, never TRUNCATE: edits that arrived after the streams last
-- ran must survive.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION app.retire_inventory_changes(p_processed_up_to bigint, p_synced_as_of timestamptz)
RETURNS bigint
LANGUAGE sql AS $$
    WITH gone AS (
        DELETE FROM app.store_inventory_changes
        WHERE change_id <= p_processed_up_to
          AND updated_at < p_synced_as_of
        RETURNING 1
    )
    SELECT count(*) FROM gone;
$$;
