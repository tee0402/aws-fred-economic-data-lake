-- Preview table
SELECT * FROM "fred"."economic_indicators" LIMIT 10

-- Get earliest and latest observation date for each series
SELECT
    series_id,
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM "fred"."economic_indicators"
GROUP BY series_id
ORDER BY series_id

-- Number of observations of each series
SELECT
    series_id,
    COUNT(*) AS num_observations
FROM "fred"."economic_indicators"
GROUP BY series_id
ORDER BY series_id

-- Unemployment rate over time
SELECT
    date,
    value AS unemployment_rate
FROM "fred"."economic_indicators"
WHERE series_id = 'UNRATE'
ORDER BY date

-- Get all observations with no value
SELECT * FROM "fred"."economic_indicators" WHERE value IS NULL