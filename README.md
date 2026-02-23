# Inmar-Data-Engineer-Assignment

## Part 1: SQL

Used PostgreSQL dialect since I didn't have access to BigQuery.

### Question Answers
1. Could either be 2023-11-05 or 2023-11-02. 2023-11-02 had 0 records, so there were technically no redemptions. If we want to ignore that date, then 2023-11-05 had the least with 3702 redemptions.
2. 2023-11-04 had the most number of redemptions with 5224 redemptions.
3. The createDateTime for 2023-11-05 was 2023-11-06 11:00:00.000000, 2023-11-02 had no createDateTime. The createDateTime for 2023-11-04 was 2023-11-05 11:00:00.000000.
4. A simple alternative would be to use a nested query instead of a CTE. This would involve just moving the query I have in the WITH statement into the FROM statement in the final SELECT query. Another approach is a self-join. We'd still generate the date range using generate_series to ensure all 7 days are represented. One side is the base table (redemptions joined to retailers with the date and retailer filters). The other side is a subquery that groups by redemptionDate and returns MAX(createDateTime) and COUNT(*) per date. We join them on redemptionDate and createDateTime = MAX(createDateTime) so that only the most recent row per date remains. The COUNT(*) from the subquery is used for the status column (Multiple Records / OK). Finally, we LEFT JOIN the date range to this result so that dates with no data appear as Missing Data.

## Part 2: Python Data Pipeline

### Pipeline Overview

The pipeline processes daily Zomato restaurant data files through a series of validation and cleaning modules. Each input file is handled independently: `main.py` loops through the files in `data/input/`, runs them through Modules B, A, C, and D in sequence, and writes the results. The decision to run Module B first was to avoid doing redundant validation checks on already processed/invalid data. Clean records go to a `.out` file, bad records go to a `.bad` file, and a metadata file tracks each issue type and which rows were affected. After processing, the original file is moved to `data/processed/`.

The modules are structured so that each one receives a DataFrame, runs its checks, and returns clean records, bad records, and metadata. No module reads or writes files directly, all I/O is handled by `main.py` and the utility functions in `utils/file_io.py`. This keeps the modules focused on logic and makes them easy to test or reorder.

### Module B - File Check

Runs first before any data is loaded. It checks three things:
- Whether the file has already been processed by looking for it in `data/processed/`.
- Whether the file extension is `.csv`.
- Whether the file is empty.

If any check fails, the file is skipped entirely. This prevents reprocessing duplicates and avoids passing bad files into the pipeline.

### Module A - Data Quality Check

Handles the core data cleaning and validation:
- **Null checks**: Records with null values in `name`, `phone`, or `location` are flagged as bad and removed.
- **Phone validation**: The phone field is split into `phone_1` and `phone_2` since the raw data often contains multiple numbers separated by newlines. Each number is cleaned by stripping `+`, spaces, and dashes, then validated for 10, 11, or 12 digits (covering Indian mobile, landline, and country code formats). If one number is valid and the other isn't, the record is kept with the bad number discarded. Only if both are invalid is the record rejected.
- **Descriptive field cleaning**: Fields like `address`, `name`, `rest_type`, `dish_liked`, and `cuisines` are cleaned by removing encoding artifacts and special characters. `reviews_list` is handled separately since it's stored as a string representation of a list,  it gets parsed, each review is cleaned individually, and the list is reconstructed.

### Module C - Custom Data Quality Checks

Two additional checks not covered by Module A:
- **Duplicate detection**: Flags and removes duplicate rows based on `name`, `address`, and `phone` together. Using all three fields avoids false positives where different restaurants share the same name or address. The first occurrence is kept, the rest are flagged.
- **Rate/votes consistency**: If a restaurant has a `rate` value present, `votes` should be greater than 0. A rating with zero votes is logically inconsistent. Rows where `votes` can't be parsed as a number are skipped since that's a data type issue, not a consistency issue.

### Module D - Location Validation

Validates the `location` field against the `areas_in_blore.csv` reference file. Each record's location is compared to the list of known areas in Bangalore. Records with locations not found in the reference file are flagged as bad. The comparison is case-insensitive to account for inconsistencies in casing between the data and the reference file.