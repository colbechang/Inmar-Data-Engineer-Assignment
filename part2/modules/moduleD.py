import pandas as pd
from pathlib import Path


REFERENCE_PATH = Path(__file__).resolve().parent.parent / "reference" / "Areas_in_blore.xlsx"

def run_location_validation(df):
    """Validates the location field against the reference file. Returns clean DataFrame, bad DataFrame, and metadata entries."""
    ref_df = pd.read_excel(REFERENCE_PATH)
    valid_locations = set(ref_df["Area"].str.strip().str.lower())
    bad_indices = []
    metadata = []

    for index, row in df.iterrows():
        location = row.get("location")
        if pd.isna(location) or str(location).strip().lower() not in valid_locations:
            bad_indices.append(index)
            metadata.append({"type_of_issue": "invalid_location", "row_num": index})

    clean_df = df[~df.index.isin(bad_indices)].copy()
    bad_df = df[df.index.isin(bad_indices)].copy()

    return clean_df, bad_df, metadata