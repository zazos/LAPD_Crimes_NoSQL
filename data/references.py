import pandas as pd
from pymongo.database import Database

def create_reference_documents(df, db):
    """
    Inserts area, weapons, and crime description reference data into separate collections.
    Example: db.areas, db.weapons, db.crime_descriptions
    """

    # AREAS
    areas_df = df[["AREA", "AREA NAME"]].dropna(subset=["AREA", "AREA NAME"]).drop_duplicates()
    areas_df["AREA"] = areas_df["AREA"].astype(int)
    area_docs = [{"area_code": row["AREA"], "area_name": row["AREA NAME"]} for _, row in areas_df.iterrows()]
    
    if area_docs:
        db.areas.insert_many(area_docs)
    db.areas.create_index("area_code", unique=True)

    # WEAPONS
    weapons_df = df[["Weapon Used Cd", "Weapon Desc"]].dropna(subset=["Weapon Used Cd", "Weapon Desc"]).drop_duplicates()
    weapons_df["Weapon Used Cd"] = weapons_df["Weapon Used Cd"].astype(int)
    weapon_docs = [{"weapon_code": row["Weapon Used Cd"], "weapon_desc": row["Weapon Desc"]} 
                   for _, row in weapons_df.iterrows()]

    if weapon_docs:
        db.weapons.insert_many(weapon_docs)
    db.weapons.create_index("weapon_code", unique=True)

    # CRIME DESCRIPTIONS
    crime_code_cols = ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]
    frames = []
    for col in crime_code_cols:
        tmp = df[[col, "Crm Cd Desc"]].dropna(subset=[col, "Crm Cd Desc"]).drop_duplicates()
        tmp = tmp.rename(columns={col: "crm_cd"})
        tmp["crm_cd"] = tmp["crm_cd"].astype(int)
        frames.append(tmp)
    crime_desc_df = pd.concat(frames).drop_duplicates(subset=["crm_cd"])
    crime_desc_docs = [{"crm_cd": row["crm_cd"], "crm_cd_desc": row["Crm Cd Desc"]} for _, row in crime_desc_df.iterrows()]

    if crime_desc_docs:
        db.crime_descriptions.insert_many(crime_desc_docs)
    db.crime_descriptions.create_index("crm_cd", unique=True)
