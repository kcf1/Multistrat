-- Post-backfill checks for strategies_daily.factor_ls tables (run in psql or GUI).
-- Replace schema if you use a non-default search_path.

-- 1) Primary key uniqueness (no duplicate (bar_ts, symbol))
SELECT 'l1feats_daily dup PK' AS check_name, COUNT(*) AS violation_rows
FROM (
  SELECT bar_ts, symbol, COUNT(*) AS c
  FROM strategies_daily.l1feats_daily
  GROUP BY 1, 2
  HAVING COUNT(*) > 1
) t;

SELECT 'factors_daily dup PK' AS check_name, COUNT(*) FROM (
  SELECT bar_ts, symbol, COUNT(*) AS c FROM strategies_daily.factors_daily GROUP BY 1, 2 HAVING COUNT(*) > 1
) t;

SELECT 'xsecs_daily dup PK' AS check_name, COUNT(*) FROM (
  SELECT bar_ts, symbol, COUNT(*) AS c FROM strategies_daily.xsecs_daily GROUP BY 1, 2 HAVING COUNT(*) > 1
) t;

SELECT 'labels_daily dup PK' AS check_name, COUNT(*) FROM (
  SELECT bar_ts, symbol, COUNT(*) AS c FROM strategies_daily.labels_daily GROUP BY 1, 2 HAVING COUNT(*) > 1
) t;

-- 2) Row counts per bar_ts (spot imbalance)
SELECT bar_ts, COUNT(*) AS n_symbols
FROM strategies_daily.l1feats_daily
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

-- 3) Join sanity: factors vs l1 on same keys (should be 0 orphans each way for same window)
SELECT COUNT(*) AS factors_without_l1
FROM strategies_daily.factors_daily f
LEFT JOIN strategies_daily.l1feats_daily l
  ON l.bar_ts = f.bar_ts AND l.symbol = f.symbol
WHERE l.symbol IS NULL;

SELECT COUNT(*) AS l1_without_factors
FROM strategies_daily.l1feats_daily l
LEFT JOIN strategies_daily.factors_daily f
  ON f.bar_ts = l.bar_ts AND f.symbol = l.symbol
WHERE f.symbol IS NULL;
