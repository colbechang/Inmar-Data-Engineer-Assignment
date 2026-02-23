# Inmar-Data-Engineer-Assignment

## Part 1: SQL

**Question Answers**
1. 2023-11-05 had the least number of redemptions with 3702 redemptions.
2. 2023-11-04 had the most number of redemptions with 5224 redemptions.
3. The createDateTime for 2023-11-05 was 2023-11-06 11:00:00.000000. The createDateTime for 2023-11-04 was 2023-11-05 11:00:00.000000.
4. A simple alternative would be to use a nested query instead of a CTE. This would involve just moving the query I have in the WITH statement into the FROM statement in the final SELECT query. Another approach is a self-join. One side is the base table (redemptions joined to retailers with the date and retailer filters). The other side is a subquery that groups by redemptionDate and returns MAX(createDateTime) and COUNT(*) per date. We join them on redemptionDate and createDateTime = MAX(createDateTime) so that only the most recent row per date remains. The COUNT(*) from the subquery is used for the status column (Missing Data / Multiple Records / OK).