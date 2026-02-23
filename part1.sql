-- Generate date series to ensure all 7 days are shown
WITH date_range AS (
  SELECT generate_series('2023-10-30'::date, '2023-11-05'::date, '1 day')::date AS redemptionDate
),
-- Get the most recent redemption record for each redemption date
-- and count the number of records for each redemption date
mostRecentRedemption AS (
  SELECT rbd.redemptionDate, rbd.redemptionCount, rbd.createDateTime,
         COUNT(*) OVER (PARTITION BY rbd.redemptionDate) AS numRecords,
         ROW_NUMBER() OVER (PARTITION BY rbd.redemptionDate ORDER BY rbd.createDateTime DESC) AS rn
  FROM "tblRedemptions-ByDay" rbd JOIN tblRetailers r ON rbd.retailerId = r.id
  WHERE rbd.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
    AND r.retailerName = 'ABC Store'
)
-- Join most recent redemption record to the date range to get a record for each date
SELECT dr.redemptionDate, mrr.redemptionCount, mrr.createDateTime,
       CASE WHEN mrr.redemptionDate IS NULL THEN 'Missing Data'
            WHEN mrr.numRecords > 1 THEN 'Multiple Records'
            ELSE 'OK'
       END AS status
FROM date_range dr
LEFT JOIN mostRecentRedemption mrr ON dr.redemptionDate = mrr.redemptionDate AND mrr.rn = 1