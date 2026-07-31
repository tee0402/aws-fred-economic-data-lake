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

-- Unemployment rate over time with recession indicator
SELECT
    U.date,
    U.value AS unemployment_rate,
    CAST(R.value AS BOOLEAN) AS is_recession
FROM "fred"."economic_indicators" U
LEFT JOIN "fred"."economic_indicators" R ON
    U.date = R.date
AND R.series_id = 'USREC'
WHERE U.series_id = 'UNRATE'
ORDER BY U.date

-- Get all observations with no value
SELECT * FROM "fred"."economic_indicators" WHERE value IS NULL