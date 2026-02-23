import pandas as pd


def check_duplicates(df):
    """Flags duplicate rows based on name, address, and phone. Keeps the first occurrence."""
    duplicate_mask = df.duplicated(subset=["name", "address", "phone"], keep="first")
    duplicate_indices = df[duplicate_mask].index.tolist()
    return duplicate_indices


def check_rate_votes(df):
    """Flags rows where rate is present but votes is 0 or less."""
    flagged = []
    for index, row in df.iterrows():
        rate = row.get("rate")
        votes = row.get("votes")
        if pd.notna(rate) and str(rate).strip() != "":
            try:
                votes_val = int(votes)
            except (ValueError, TypeError):
                continue
            if votes_val <= 0:
                flagged.append(index)
    return flagged


def run_custom_checks(df):
    """Runs custom data quality checks. Returns clean DataFrame, bad DataFrame, and metadata entries."""
    bad_indices = set()
    metadata = []

    duplicate_indices = check_duplicates(df)
    for index in duplicate_indices:
        metadata.append({"type_of_issue": "duplicate", "row_num": index})
    bad_indices.update(duplicate_indices)

    rate_votes_indices = check_rate_votes(df)
    for index in rate_votes_indices:
        metadata.append({"type_of_issue": "rate_votes_inconsistency", "row_num": index})
    bad_indices.update(rate_votes_indices)

    clean_df = df[~df.index.isin(bad_indices)].copy()
    bad_df = df[df.index.isin(bad_indices)].copy()

    return clean_df, bad_df, metadata