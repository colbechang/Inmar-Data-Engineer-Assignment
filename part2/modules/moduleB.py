from pathlib import Path
import pandas as pd

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

def file_check(file_path):
    file_path = Path(file_path)
    file_name = file_path.name

    if (PROCESSED_DIR / file_name).exists():
        print(f"File {file_name} already exists in the processed directory.")
        return False

    if file_path.suffix != ".csv":
        print(f"File {file_name} is not a CSV file.")
        return False

    df = pd.read_csv(file_path)
    if df.empty:
        print(f"File {file_name} is empty.")
        return False

    return True