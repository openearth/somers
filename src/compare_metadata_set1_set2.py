import shutil

import pandas as pd
from openpyxl import load_workbook

excel_set1_path = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_updated_20261028.xlsx"
excel_set2_path = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_updated_20261104.xlsx"
output_path = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/extra_peilbuizen_set2.xlsx"

# Get names that already occur in set1
df_set1 = pd.read_excel(excel_set1_path)
names_set1 = set(df_set1["name"].dropna())

# Copy set2 so that its Excel formatting/highlighting is preserved
shutil.copy2(excel_set2_path, output_path)

# Remove rows whose name occurs in set1
wb = load_workbook(output_path)
ws = wb.active

name_col = [cell.value for cell in ws[1]].index("name") + 1

for row in range(ws.max_row, 1, -1):
    if ws.cell(row, name_col).value in names_set1:
        ws.delete_rows(row)

wb.save(output_path)
