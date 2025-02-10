import pandas as pd
from pymongo.database import Database
from tqdm import tqdm

def build_crime_documents(df):
    """
    Builds a list of Crime Report document dictionaries from the preprocessed DataFrame.
    """
    crime_cds_map = {}
    cd_defs = df[['Crm Cd 1','Crm Cd Desc']].dropna(subset=['Crm Cd 1','Crm Cd Desc']).drop_duplicates()
    for _, row in cd_defs.iterrows():
        try:
            code = int(row['Crm Cd 1'])
            crime_cds_map[code] = row['Crm Cd Desc']
        except ValueError:
            pass

    crime_docs = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Building Crime Docs"):
        if row["DR_NO"] is None:
            continue
        try:
            cd_array = []
            for col in ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]:
                val = row[col]
                if val is not None:
                    val_int = int(val)
                    cd_array.append({"code": val_int, "desc": crime_cds_map.get(val_int, "Unknown")})

            doc = {
                "dr_no": int(row["DR_NO"]),
                "date_rptd": row["Date Rptd"] if isinstance(row["Date Rptd"], pd.Timestamp) else None,
                "date_occ": row["DATE OCC"] if isinstance(row["DATE OCC"], pd.Timestamp) else None,
                "time_occ": int(row["TIME OCC"]) if row["TIME OCC"] is not None and not pd.isna(row["TIME OCC"]) else None,
                "area_code": int(row["AREA"]) if row["AREA"] is not None and not pd.isna(row["AREA"]) else None,
                # We do NOT store 'area_name' here to avoid duplication.
                # "rpt_dist_no": int(row["Rpt Dist No"]) if row["Rpt Dist No"] is not None and not pd.isna(row["Rpt Dist No"]) else None,
                # "mocodes": row["Mocodes"],  # Possibly large, consider removing if not needed
                # "victim_age": int(row["Vict Age"]) if row["Vict Age"] is not None and not pd.isna(row["Vict Age"]) else None,
                # "victim_sex": row["Vict Sex"],
                # "victim_descent": row["Vict Descent"],
                # "premis_cd": int(row["Premis Cd"]) if row["Premis Cd"] is not None and not pd.isna(row["Premis Cd"]) else None,
                # "premis_desc": row["Premis Desc"],  # Omit for duplication reasons
                "weapon_code": int(row["Weapon Used Cd"]) if row["Weapon Used Cd"] is not None and not pd.isna(row["Weapon Used Cd"]) else None,
                # "weapon_desc": row["Weapon Desc"],  # omit for duplication reasons
                # "status": row["Status"],
                # "status_desc": row["Status Desc"],  # omit if you want a status reference collection as well
                "crm_cd_list": cd_array,
                "location": row["LOCATION"],
                # "lat": float(row["LAT"]) if row["LAT"] is not None and not pd.isna(row["LAT"]) else None,
                # "lon": float(row["LON"]) if row["LON"] is not None and not pd.isna(row["LON"]) else None
            }
            crime_docs.append(doc)
        except Exception as e:
            print(f"Error building doc for DR_NO={row['DR_NO']}: {e}")
    return crime_docs

def insert_crime_reports(db, crime_docs, chunk_size=10000):
    """
    Inserts the crime_docs into 'crime_reports' in batches.
    """
    total = len(crime_docs)
    if not crime_docs:
        print("No Crime Docs to insert.")
        return

    print(f"Inserting {total} documents into crime_reports...")
    chunk_size = 10000
    total_chunks = (len(crime_docs) + chunk_size - 1) // chunk_size
    with tqdm(total=total_chunks, desc="Inserting documents") as pbar:
        for i in range(0, len(crime_docs), chunk_size):
            chunk = crime_docs[i:i+chunk_size]
            result = db.crime_reports.insert_many(chunk)
            pbar.update(1)
            
    db.crime_reports.create_index("dr_no", unique=True)
    print("Finished inserting crime reports.")
