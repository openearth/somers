# step 1: read 2026 excel handmatige aanpassingen document
# step 2: read 2024 excel handmatige aanpassingen document
# step 3: compare where well id and ditch id are the same in the 2024 and 2026 document

import pandas as pd
from openpyxl.styles import PatternFill

path_to_handmatige_aanpassingen_2026 = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_werkdocument.xlsx"
path_to_handmatige_aanpassingen_2024 = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2024/handmatige_aanpassingen/handmatige_aanpassingen_kalibratie_werkdocument.xlsx"

handmatige_aanpassingen_2026 = pd.read_excel(path_to_handmatige_aanpassingen_2026)
handmatige_aanpassingen_2024 = pd.read_excel(path_to_handmatige_aanpassingen_2024)

# drop the columns ditch_id and ditch_name
handmatige_aanpassingen_2024 = handmatige_aanpassingen_2024.drop(
    columns=["ditch_id", "ditch_name", "well_id", "start_date", "end_date"],
    errors="ignore",
)

columns = [
    # "well_id",
    # "name_bgt",
    "name",
    # "transect",
    # "measure",
    # "ditch_id",
    # "ditch_name",
    # "soil_class",
    # "altitude_m_nap",
    # "ahn4_m_nap",
    # "start_date",
    # "end_date",
    # "parcel_width_m",
    # "distance_to_ditch_m",
    # "trenches",
    # "trench_depth_m_sfl",
    # "wis_distance_m",
    # "wis_depth_m_sfl",
    # "distance_to_wis_m",
    # "selection",
    # "description",
    # "source",
]

# handmatige_aanpassingen_2026 = handmatige_aanpassingen_2026[columns]
# handmatige_aanpassingen_2024 = handmatige_aanpassingen_2024[columns]

handmatige_aanpassingen_2026["name"] = handmatige_aanpassingen_2026["name"].str.replace(
    "GWM_", "", regex=False
)

# handmatige_aanpassingen_2026_filtered = handmatige_aanpassingen_2026.merge(
#     handmatige_aanpassingen_2024[["name"]].drop_duplicates(),
#     on=["name"],
#     how="inner",
# )

matching_names = handmatige_aanpassingen_2026["name"].isin(
    handmatige_aanpassingen_2024["name"]
)
# indices_2026 = handmatige_aanpassingen_2026.index[matching_names]

# matching_names_2024 = handmatige_aanpassingen_2024["name"].isin(
#     handmatige_aanpassingen_2026["name"]
# )

# indices_2024 = handmatige_aanpassingen_2024.index[matching_names_2024]

df_2026 = handmatige_aanpassingen_2026.set_index("name")
df_2024 = handmatige_aanpassingen_2024.set_index("name")

df_2026["description"] = df_2026["description"].astype(str)
# df_2024["start_date"] = df_2024["start_date"].astype(str)
# df_2024["end_date"] = df_2024["end_date"].astype(str)

# round so that comparison of "ahn4_m_nap" values is consistent
df_2024["ahn4_m_nap"] = df_2024["ahn4_m_nap"].round(3)
df_2026["ahn4_m_nap"] = df_2026["ahn4_m_nap"].round(3)

df_2026_original = df_2026.copy()

df_2026.update(df_2024)

handmatige_aanpassingen_2026_updated = df_2026.reset_index()

print(f"2026 before filtering with 2024 file: {len(handmatige_aanpassingen_2026)}")
print(
    f"2026 after filtering with 2024 file:  {len(matching_names[matching_names==True])}"
)
print(
    f"So in 2024 dataset and also present in 2026:     {len(handmatige_aanpassingen_2026) - len(matching_names[matching_names==True])}"
)

########################################################################################
# output the values to an excel file, also highlight the values that changed
########################################################################################


sheet_name = "handmatige_aanpassingen"

matching_names = set(df_2024.index).intersection(df_2026.index)

path_output = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_updated_20261028.xlsx"
# handmatige_aanpassingen_2026_updated.to_excel(path_output, index=False)

# Write DataFrame to Excel
with pd.ExcelWriter(path_output, engine="openpyxl") as writer:
    handmatige_aanpassingen_2026_updated.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
    )

    worksheet = writer.sheets[sheet_name]

    yellow_fill = PatternFill(
        fill_type="solid",
        fgColor="FFF2CC",
    )

    red_fill = PatternFill(
        fill_type="solid",
        fgColor="F4CCCC",
    )

    # Find the column positions that occur in both datasets
    common_columns = df_2026.columns.intersection(df_2024.columns)

    for dataframe_row, name in enumerate(
        handmatige_aanpassingen_2026_updated["name"],
        start=2,
    ):
        if name not in matching_names:
            continue

        for column_name in common_columns:
            value_2026 = df_2026_original.at[name, column_name]
            value_2024 = df_2024.at[name, column_name]

            # Highlight only when the values differ
            if pd.notna(value_2024) and (
                pd.isna(value_2026) or value_2026 != value_2024
            ):
                excel_column = (
                    handmatige_aanpassingen_2026_updated.columns.get_loc(column_name)
                    + 1
                )

                worksheet.cell(
                    row=dataframe_row,
                    column=excel_column,
                ).fill = yellow_fill

    # Highlight entire rows with selection == "no" in red
    for dataframe_row, selection in enumerate(
        handmatige_aanpassingen_2026_updated["selection"],
        start=2,
    ):
        if str(selection).strip().lower() == "no":
            for excel_column in range(
                1,
                len(handmatige_aanpassingen_2026_updated.columns) + 1,
            ):
                worksheet.cell(
                    row=dataframe_row,
                    column=excel_column,
                ).fill = red_fill

print("Done")
