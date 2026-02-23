import re
import ast
import pandas as pd


REQUIRED_FIELDS = ["name", "phone", "location"]
DESCRIPTIVE_FIELDS = ["address", "name", "rest_type", "dish_liked", "cuisines"]


def clean_phone(phone):
    """Strips spaces, '+', and dashes from a phone value."""
    if pd.isna(phone):
        return phone
    phone = str(phone)
    phone = phone.replace("+", "").replace(" ", "").replace("-", "")
    return phone


def validate_phone(phone):
    """Checks that a cleaned phone number is all digits and 10 or 12 characters long."""
    if pd.isna(phone):
        return False
    phone = str(phone)
    digits = re.sub(r"[^0-9]", "", phone)
    return len(digits) in [10, 11, 12]


def split_phone_field(phone):
    """Splits a phone field on common delimiters and returns two cleaned numbers."""
    if pd.isna(phone):
        return None, None
    phone = str(phone)
    parts = re.split(r"[/\n\\]", phone)
    parts = [clean_phone(p.strip()) for p in parts if p.strip()]
    phone_1 = parts[0] if len(parts) >= 1 else None
    phone_2 = parts[1] if len(parts) >= 2 else None
    return phone_1, phone_2


def clean_desc_field(value, is_list=False):
    """Removes special characters and encoding artifacts from descriptive fields. If is_list is True, parses the value as a list, cleans each element, and reconstructs it."""
    if pd.isna(value):
        return value

   # Convert reviews_list to a list and clean each element
    if is_list:
        try:
            parsed = ast.literal_eval(str(value))
            cleaned = []
            for item in parsed:
                if isinstance(item, tuple):
                    cleaned.append(tuple(clean_desc_field(str(elem)) for elem in item))
                else:
                    cleaned.append(clean_desc_field(str(item)))
            return str(cleaned)
        except (ValueError, SyntaxError):
            return clean_desc_field(str(value))

    value = str(value)
    value = re.sub(r"Ã[\x80-\xBF]+", "", value)
    value = re.sub(r"\\x[0-9a-fA-F]{2}", "", value)
    value = re.sub(r"[^a-zA-Z0-9\s,.\-\(\)'/&:;@!?\"#]", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def check_nulls(row):
    """Returns a list of required fields that are null for a given row."""
    null_fields = []
    for field in REQUIRED_FIELDS:
        if pd.isna(row.get(field)) or str(row.get(field)).strip() == "":
            null_fields.append(field)
    return null_fields


def run_data_quality_check(df):
    """Runs all data quality checks. Returns clean DataFrame, bad DataFrame, and metadata entries."""
    clean = []
    bad = []
    metadata = []

    for index, row in df.iterrows():
        issues = []

        null_fields = check_nulls(row)
        if null_fields:
            issues.append("null")

        phone_1, phone_2 = split_phone_field(row.get("phone"))

        if not null_fields or "phone" not in null_fields:
            if phone_1 and not validate_phone(phone_1):
                if phone_2 and validate_phone(phone_2):
                    phone_1 = phone_2
                    phone_2 = None
                else:
                    issues.append("invalid_phone")
            elif phone_2 and not validate_phone(phone_2):
                phone_2 = None

        if issues:
            for issue in issues:
                metadata.append({"type_of_issue": issue, "row_num": index})
            bad.append(row)
        else:
            row = row.copy()
            row["phone_1"] = phone_1
            row["phone_2"] = phone_2
            for field in DESCRIPTIVE_FIELDS:
                if field in row.index:
                    row[field] = clean_desc_field(row[field])
            if "reviews_list" in row.index:
                row["reviews_list"] = clean_desc_field(row["reviews_list"], is_list=True)
            clean.append(row)

    clean_df = pd.DataFrame(clean)
    bad_df = pd.DataFrame(bad)

    return clean_df, bad_df, metadata