-- Reporting views consumed by the Power BI dashboard (PostgreSQL).
-- Created automatically on API startup.

CREATE OR REPLACE VIEW v_run_facts AS
SELECT
    r.id                                   AS run_id,
    p.name                                 AS pipeline,
    p.owner,
    r.status,
    r.started_at,
    r.finished_at,
    DATE(r.started_at)                     AS run_date,
    r.duration_seconds,
    r.rows_processed,
    r.is_anomaly,
    d.category                             AS issue_category,
    d.root_cause,
    d.confidence                           AS rca_confidence,
    d.source                               AS rca_source,
    r.error_message
FROM pipeline_runs r
JOIN pipelines p ON p.id = r.pipeline_id
LEFT JOIN diagnoses d ON d.run_id = r.id
WHERE r.status <> 'running';

CREATE OR REPLACE VIEW v_daily_pipeline_stats AS
SELECT
    p.name                                                     AS pipeline,
    DATE(r.started_at)                                         AS run_date,
    COUNT(*)                                                   AS total_runs,
    COUNT(*) FILTER (WHERE r.status = 'failed')                AS failed_runs,
    ROUND(AVG(CASE WHEN r.status = 'success' THEN 1.0 ELSE 0 END)::numeric, 3) AS success_rate,
    ROUND(AVG(r.duration_seconds)::numeric, 1)                 AS avg_duration_seconds,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.duration_seconds)::numeric, 1) AS p95_duration_seconds,
    SUM(r.rows_processed)                                      AS rows_processed,
    COUNT(*) FILTER (WHERE r.is_anomaly)                       AS anomalous_runs
FROM pipeline_runs r
JOIN pipelines p ON p.id = r.pipeline_id
WHERE r.status <> 'running'
GROUP BY p.name, DATE(r.started_at);

CREATE OR REPLACE VIEW v_pipeline_freshness AS
SELECT
    p.name                                                     AS pipeline,
    p.freshness_sla_minutes,
    MAX(r.finished_at)                                         AS last_success_at,
    ROUND((EXTRACT(EPOCH FROM (NOW() - MAX(r.finished_at))) / 60)::numeric, 1) AS minutes_since_success,
    COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX(r.finished_at))) / 60 > p.freshness_sla_minutes, TRUE) AS is_stale
FROM pipelines p
LEFT JOIN pipeline_runs r ON r.pipeline_id = p.id AND r.status = 'success'
GROUP BY p.name, p.freshness_sla_minutes;
