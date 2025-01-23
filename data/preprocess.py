import pandas as pd
import numpy as np

def preprocess_data(df):
    """
    Cleans and standardizes the raw CSV DataFrame.
    E.g., converts date fields, numeric fields, ensures victim_sex is in [F, M, X], etc.
    """
    df = df.where(pd.notnull(df), None)
    df['Weapon Used Cd'] = df['Weapon Used Cd'].where(pd.notnull(df['Weapon Used Cd']), None)

    # Convert dates
    df["Date Rptd"] = pd.to_datetime(df["Date Rptd"], format='%m/%d/%Y %I:%M:%S %p')
    df["DATE OCC"] = pd.to_datetime(df["DATE OCC"], format='%m/%d/%Y %I:%M:%S %p')

    # Numeric conversions
    numeric_fields = ["TIME OCC", "AREA", "Rpt Dist No", "Premis Cd", "Weapon Used Cd", "Vict Age"]
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors="coerce").astype("Int64")

    # Standardize Vict Sex
    df["Vict Sex"] = df["Vict Sex"].str.strip().str.upper().replace({"": None})
    df["Vict Sex"] = df["Vict Sex"].apply(lambda x: x if x in ["F", "M", "X"] else None)

    # Negative Vict Age => 0
    df["Vict Age"] = df["Vict Age"].apply(lambda x: x if x is None or x >= 0 else 0)

    # Coordinates
    df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
    df["LON"] = pd.to_numeric(df["LON"], errors="coerce")

    # Vict Descent mapping
    descent_mapping = {
        "A": "Other Asian","B": "Black","C": "Chinese","D": "Cambodian","F": "Filipino",
        "G": "Guamanian","H": "Hispanic/Latin/Mexican","I": "American Indian/Alaskan Native",
        "J": "Japanese","K": "Korean","L": "Laotian","O": "Other","P": "Pacific Islander",
        "S": "Samoan","U": "Hawaiian","V": "Vietnamese","W": "White","X": "Unknown","Z": "Asian Indian"
    }
    df["Vict Descent"] = df["Vict Descent"].str.strip().str.upper().replace({"": None})
    df["Vict Descent"] = df["Vict Descent"].apply(lambda x: descent_mapping.get(x, "Unknown") if x else None)

    return df
