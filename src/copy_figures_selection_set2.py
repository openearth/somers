import shutil
from pathlib import Path

import pandas as pd

excel_path = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_updated_20261125.xlsx"
)

df = pd.read_excel(
    excel_path,
    sheet_name="handmatige_aanpassingen",
)

df = df[df["selection"] == "yes"]

source_folder = Path(
    r"P:\11207812-somers-ontwikkeling"
    r"\3-somers_development"
    r"\QSOMERS"
    r"\Dataverzameling 2026"
    r"\Dataverzameling"
    r"\timeseries_metadata_ongecontroleerd"
)

target_folder = Path(
    r"P:\11207812-somers-ontwikkeling"
    r"\3-somers_development"
    r"\QSOMERS"
    r"\Dataverzameling 2026"
    r"\Dataverzameling"
    r"\timeseries_selection"
)

# Get the names from the Excel dataframe
names = set(df["well_id"].dropna().astype(str))

# Create target folder if it does not exist yet
target_folder.mkdir(parents=True, exist_ok=True)

i = 0

# Loop through all files in the input folder
for file in source_folder.glob("*"):
    if file.is_file() and file.stem in names:
        shutil.copy2(file, target_folder / file.name)
        print(f"Copied: {file.name}")
        i += 1
print(f"Total copied: {i}")

source_folder2 = Path(
    r"P:\11207812-somers-ontwikkeling"
    r"\3-somers_development"
    r"\QSOMERS"
    r"\Dataverzameling 2026"
    r"\Dataverzameling"
    r"\timeseries_metadata_ongecontroleerd_set2"
)

j = 0

# Loop through all files in the input folder
for file in source_folder2.glob("*"):
    if file.is_file() and file.stem in names:
        shutil.copy2(file, target_folder / file.name)
        print(f"Copied: {file.name}")
        j += 1
print(f"Total copied: {j}")
