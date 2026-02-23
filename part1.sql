-- Create a CTE to order the redemption dates by the createDateTime and count the number of records for each redemption date
WITH mostRecentRedemption AS (
  SELECT r.retailerName, rbd.redemptionDate, rbd.redemptionCount,  rbd.createDateTime,
  COUNT(rbd.redemptionDate) OVER (PARTITION BY rbd.redemptionDate) AS numRecords,
  ROW_NUMBER() OVER (PARTITION BY rbd.redemptionDate ORDER BY rbd.createDateTime DESC) AS rn
  FROM "tblRedemptions-ByDay" rbd JOIN tblRetailers r ON rbd.retailerId= r.id
  WHERE rbd.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
  AND r.retailerName = 'ABC Store'
)
-- Select the most recent redemption record for each redemption date and pull in all other required fields
SELECT redemptionDate, redemptionCount, createdatetime,
CASE WHEN numRecords = 0 THEN 'Missing Data'
     WHEN numRecords > 1 THEN 'Multiple Records'
     ELSE 'OK'
END AS status
FROM mostRecentRedemption
WHERE rn = 1


