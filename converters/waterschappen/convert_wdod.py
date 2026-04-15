# %%
from pathlib import Path

import pandas as pd

basedir = Path(
    "p:/11207812-somers-ontwikkeling/3-somers_development/QSOMERS/Handmatige uitvraag 2026/handmatige_uitvraag_bestanden/WDOD"
)

ontvangen_dir = basedir.joinpath("ontvangen")

xlsx_path = Path(ontvangen_dir / r"data Somers.xlsx")
out_dir = basedir.joinpath("bewerkt")

# %% swm-files
output_series_name = "slootwaterstand (m NAP)"

def fmt_decimal_comma(x, ndigits=2):
    """Format float with comma decimal, strip trailing zeros if you prefer."""
    if pd.isna(x):
        return ""
    return f"{x:.{ndigits}f}".replace(".", ",")

def safe_filename(s):
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(s)).strip("_")

sheets = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")

for sheet_name, df in sheets.items():
    if df is None or df.empty:
        continue

    df.columns = [str(c).strip() for c in df.columns]

    required = ["Naam meetpunt", "x-coor", "y-coor", "Date", "Waterhoogte [mNAP]"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Skipping sheet '{sheet_name}' (missing columns: {missing})")
        continue

    df["datumtijd"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")

    wh = pd.to_numeric(df["Waterhoogte [mNAP]"].astype(str).str.replace(",", ".", regex=False),
                       errors="coerce")
    df = df.assign(waterhoogte=wh)

    df = df.dropna(subset=["datumtijd"])

    for meetpunt, g in df.groupby("Naam meetpunt", dropna=True):
        g = g.sort_values("datumtijd")
        x = g["x-coor"].dropna().iloc[0] if g["x-coor"].notna().any() else ""
        y = g["y-coor"].dropna().iloc[0] if g["y-coor"].notna().any() else ""

        fname = f"{"SWM_" + safe_filename(meetpunt)}.txt"
        path_out = out_dir / fname

        header = (
            f"# naam_meetpunt: {meetpunt}\n"
            f"# x-coor: {x}\n"
            f"# y-coor: {y}\n"
            "*\n"
            "> datumtijd (dd-mm-yyyy)\n"
            f"> {output_series_name}\n"
        )

        with open(path_out, "w", encoding="utf-8") as f:
            f.write(header)
            for t, v in zip(g["datumtijd"], g["waterhoogte"]):
                t_str = pd.Timestamp(t).strftime("%d-%m-%Y")
                v_str = fmt_decimal_comma(v, ndigits=2)
                f.write(f"{t_str};{v}\n")

        print(f"Wrote {path_out} (sheet: {sheet_name}, rows: {len(g)})")

# %%
