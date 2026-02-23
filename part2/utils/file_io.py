import os
import shutil
import pandas as pd
from datetime import datetime


def write_out_file(df, original_filename, output_dir):
    """Writes clean records to a .out file with a processed_timestamp appended to each row."""
    df = df.copy()
    df["processed_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    base_name = os.path.splitext(os.path.basename(original_filename))[0]
    output_path = os.path.join(output_dir, f"{base_name}.out")
    df.to_csv(output_path, index=False)
    return output_path


def write_bad_file(bad_dfs, original_filename, bad_dir):
    """Combines and writes all bad record DataFrames to a single .bad file."""
    combined = pd.concat(bad_dfs, ignore_index=True) if bad_dfs else pd.DataFrame()
    if combined.empty:
        return None
    base_name = os.path.splitext(os.path.basename(original_filename))[0]
    output_path = os.path.join(bad_dir, f"{base_name}.bad")
    combined.to_csv(output_path, index=False)
    return output_path


def write_metadata(metadata_entries, original_filename, metadata_dir):
    """Writes a metadata file summarizing issue types and affected row numbers for bad records."""
    if not metadata_entries:
        return None
    aggregated = {}
    for entry in metadata_entries:
        issue = entry["type_of_issue"]
        row_num = entry["row_num"]
        if issue not in aggregated:
            aggregated[issue] = []
        aggregated[issue].append(row_num)

    rows = [
        {"type_of_issue": issue, "row_num_list": row_nums}
        for issue, row_nums in aggregated.items()
    ]
    base_name = os.path.splitext(os.path.basename(original_filename))[0]
    output_path = os.path.join(metadata_dir, f"{base_name}_metadata.csv")
    pd.DataFrame(rows).to_csv(output_path, index=False)
    return output_path


def move_to_processed(filepath, processed_dir):
    """Moves a file to the processed directory after pipeline completion."""
    destination = os.path.join(processed_dir, os.path.basename(filepath))
    shutil.move(filepath, destination)
    return destination


def ensure_directories(*dirs):
    """Creates directories if they don't already exist."""
    for d in dirs:
        os.makedirs(d, exist_ok=True)