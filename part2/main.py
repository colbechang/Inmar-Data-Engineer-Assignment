from pathlib import Path
from modules.moduleB import file_check
from modules.moduleA import run_data_quality_check
from utils.file_io import write_out_file, write_bad_file, write_metadata, move_to_processed, ensure_directories
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent

INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
BAD_DIR = SCRIPT_DIR / "data" / "bad"
METADATA_DIR = SCRIPT_DIR / "data" / "metadata"
PROCESSED_DIR = SCRIPT_DIR / "data" / "processed"


def run_pipeline():
    """
    Data Validation Pipeline:
    - Reads in files from the input directory
    - Uses Module B to check if the file has already been processed
    - Uses Module A to run data quality checks on the file
    - Writes the clean records to the output directory
    - Writes the bad records to the bad directory
    - Writes the metadata to the metadata directory
    - Moves the file to the processed directory
    """
    ensure_directories(INPUT_DIR, OUTPUT_DIR, BAD_DIR, METADATA_DIR, PROCESSED_DIR)

    files = list(INPUT_DIR.iterdir())

    if not files:
        print("No files found in input directory.")
        return

    
    for file_path in files:
        file_name = file_path.name
        print(f"\n**Processing {file_name}**")

        try:
          if not file_check(file_path):
              print(f"Skipping {file_name}.")
              continue
            
          df = pd.read_csv(file_path)

          all_bad_dfs = []
          all_metadata = []

          clean_df, bad_df_a, metadata_a = run_data_quality_check(df)
          all_bad_dfs.append(bad_df_a)
          all_metadata.extend(metadata_a)
          print(f"Module A: {len(bad_df_a)} bad records, {len(clean_df)} clean records.")

          write_out_file(clean_df, file_name, str(OUTPUT_DIR))
          write_bad_file(all_bad_dfs, file_name, str(BAD_DIR))
          write_metadata(all_metadata, file_name, str(METADATA_DIR))
          move_to_processed(str(file_path), str(PROCESSED_DIR))

          print(f"Finished {file_name}. {len(clean_df)} clean, {len(bad_df_a)} bad.")

        except Exception as e:
          print(f"Error processing {file_name}: {e}")
          continue

if __name__ == "__main__":
    run_pipeline()