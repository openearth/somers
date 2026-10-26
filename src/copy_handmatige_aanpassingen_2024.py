# step 1: read 2026 excel handmatige aanpassingen document
# step 2: read 2024 excel handmatige aanpassingen document
# step 3: compare where well id and ditch id are the same in the 2024 and 2026 document

import pandas as pd

path_to_handmatige_aanpassingen_2026 = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2026/handmatige_aanpassingen/handmatige_aanpassingen_2026_werkdocument_alle_kolommen.xlsx"
path_to_handmatige_aanpassingen_2024 = "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Dataverzameling 2024/handmatige_aanpassingen/handmatige_aanpassingen_kalibratie_werkdocument.xlsx"

handmatige_aanpassingen_2026 = pd.read_excel(path_to_handmatige_aanpassingen_2026)
handmatige_aanpassingen_2024 = pd.read_excel(path_to_handmatige_aanpassingen_2024)

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

handmatige_aanpassingen_2026 = handmatige_aanpassingen_2026[columns]
handmatige_aanpassingen_2024 = handmatige_aanpassingen_2024[columns]

handmatige_aanpassingen_2026_filtered = handmatige_aanpassingen_2026.merge(
    handmatige_aanpassingen_2024[["name"]].drop_duplicates(),
    on=["name"],
    how="inner",
)

print(f"2026 before filtering with 2024 file: {len(handmatige_aanpassingen_2026)}")
print(f"2026 after filtering with 2024 file:  {len(handmatige_aanpassingen_2026_filtered)}")
print(f"So in 2024 dataset and also present in 2026:     {len(handmatige_aanpassingen_2026) - len(handmatige_aanpassingen_2026_filtered)}")

print("Done")
