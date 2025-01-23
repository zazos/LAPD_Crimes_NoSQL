import pandas as pd 
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm
import os
from faker import Faker
import random

def generate_officers(n_officers=5000):
    print(f"Generating {n_officers} officers")
    fake = Faker()
    officers = []
    unique_badges = set()
    
    for _ in range(n_officers):
        badge_number = random.randint(1000, 999999)
        while badge_number in unique_badges:
            badge_number = random.randint(1000, 999999)
        unique_badges.add(badge_number)

        officers.append({
            "badge_number": badge_number,
            "name": fake.name(),
            "email": fake.email(),
            "upvotes": 0
        })

    return officers

def upvoting_reports(db, officers, threshold=0.3, max_upvotes=1000):
    print("Upvoting reports")
    fake = Faker()
    total_reports = db.crime_reports.count_documents({})
    all_report_docs = list(db.crime_reports.find({}, {"_id": 0, "dr_no": 1}))
    all_report_numbers = [doc["dr_no"] for doc in all_report_docs]
    n_upvotes = int(len(all_report_numbers) * threshold)
    random_upvoted_reports = random.sample(all_report_numbers, n_upvotes)
    
    upvoted_docs = []
    for dr_no in random_upvoted_reports:
        random_upvotes = random.randint(1, 3)
        
        for _ in range(random_upvotes):
            valid_officers = [officer for officer in officers if officer["upvotes"] < max_upvotes]
            if not valid_officers:
                print("All officers have reached the maximum upvotes.")
                break
            
            chosen_officer = random.choice(valid_officers)
            upvoted_doc = {
                "dr_no": dr_no,
                "badge_number": chosen_officer["badge_number"],
                "officer_name": chosen_officer["name"],
                "officer_email": chosen_officer["email"],
                "upvoted_at": fake.date_time_between(start_date='-2y', end_date='now')
                }
            upvoted_docs.append(upvoted_doc)
            chosen_officer["upvotes"] += 1
    
    if upvoted_docs:
        db.upvotes.drop()
        print(f"Inserting {len(upvoted_docs)} documents into upvotes...")
        chunk_size = 10000
        with tqdm(total=len(upvoted_docs), desc="Upvote insertion") as pbar:
            for i in range(0, len(upvoted_docs), chunk_size):
                chunk = upvoted_docs[i:i + chunk_size]
                db.upvotes.insert_many(chunk)
                pbar.update(len(chunk))
        
        # Optional: create a unique index on (dr_no, badge_number) if you want 
        # to prohibit the same officer from upvoting the same report more than once:
        db.upvotes.create_index([("dr_no", 1), ("badge_number", 1)], unique=True)
        
        print(f"Total upvotes in upvotes collection: {db.upvotes.count_documents({})}")
    else:
        print("No upvote documents to insert.")

def preprocess_data(df):
    print("Preprocessing data")
    df = df.where(pd.notnull(df), None)
    # Explicitly set weapon_used_cd to None if it contains any pd.NA or NaN
    df['Weapon Used Cd'] = df['Weapon Used Cd'].where(pd.notnull(df['Weapon Used Cd']), None)

    df["Date Rptd"] = pd.to_datetime(df["Date Rptd"], format='%m/%d/%Y %I:%M:%S %p')
    df["DATE OCC"] = pd.to_datetime(df["DATE OCC"], format='%m/%d/%Y %I:%M:%S %p')
    
    numeric_fields = [
    "TIME OCC",
    "AREA",
    "Rpt Dist No",
    "Premis Cd",
    "Weapon Used Cd",
    "Vict Age",
    ]
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors="coerce").astype("Int64")

    df["Vict Sex"] = df["Vict Sex"].str.strip().str.upper().replace({"": None})
    df["Vict Sex"] = df["Vict Sex"].apply(lambda x: x if x in ["F", "M", "X"] else None)

    df["Vict Age"] = df["Vict Age"].apply(lambda x: x if x is None or x >= 0 else 0) # negative values to 0 default

    df["Vict Descent"] = df["Vict Descent"].str.strip().str.upper().replace({"": None})

    df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
    df["LON"] = pd.to_numeric(df["LON"], errors="coerce")
    
    descent_mapping = {
        "A": "Other Asian","B": "Black","C": "Chinese","D": "Cambodian","F": "Filipino",
        "G": "Guamanian","H": "Hispanic/Latin/Mexican","I": "American Indian/Alaskan Native",
        "J": "Japanese","K": "Korean","L": "Laotian","O": "Other","P": "Pacific Islander",
        "S": "Samoan","U": "Hawaiian","V": "Vietnamese","W": "White","X": "Unknown",
        "Z": "Asian Indian"
    }
    df["Vict Descent"] = df["Vict Descent"].str.strip().str.upper()
    df["Vict Descent"] = df["Vict Descent"].apply(lambda x: descent_mapping.get(x, "Unknown") if x else None)
    
    df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
    df["LON"] = pd.to_numeric(df["LON"], errors="coerce")
    
    return df

def create_reference_documents(df, db):
    # AREAS
    areas_df = df[["AREA", "AREA NAME"]].dropna(subset=["AREA", "AREA NAME"]).drop_duplicates()
    areas_df["AREA"] = areas_df["AREA"].astype(int)

    area_docs = []
    for _, row in areas_df.iterrows():
        area_docs.append({
            "area_code": row["AREA"],
            "area_name": row["AREA NAME"]
        })

    db.areas.drop()
    if area_docs:
        db.areas.insert_many(area_docs)
    db.areas.create_index("area_code", unique=True)

    # WEAPONS
    weapons_df = df[["Weapon Used Cd", "Weapon Desc"]].dropna(subset=["Weapon Used Cd", "Weapon Desc"]).drop_duplicates()
    weapons_df["Weapon Used Cd"] = weapons_df["Weapon Used Cd"].astype(int)

    weapon_docs = []
    for _, row in weapons_df.iterrows():
        weapon_docs.append({
            "weapon_code": row["Weapon Used Cd"],
            "weapon_desc": row["Weapon Desc"]
        })

    db.weapons.drop()
    if weapon_docs:
        db.weapons.insert_many(weapon_docs)
    db.weapons.create_index("weapon_code", unique=True)
    
    # CRIME DESCRIPTIONS
    crime_code_cols = ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]
    crime_code_frames = []
    for col in crime_code_cols:
        subset = df[[col, "Crm Cd Desc"]].dropna(subset=[col, "Crm Cd Desc"]).drop_duplicates()
        subset = subset.rename(columns={col: "crm_cd"})
        subset["crm_cd"] = subset["crm_cd"].astype(int)
        crime_code_frames.append(subset)

    crime_descriptions_df = pd.concat(crime_code_frames, ignore_index=True).drop_duplicates(subset=["crm_cd"])
    crime_desc_docs = []
    for _, row in crime_descriptions_df.iterrows():
        crime_desc_docs.append({
            "crm_cd": row["crm_cd"],
            "crm_cd_desc": row["Crm Cd Desc"]
        })

    db.crime_descriptions.drop()
    if crime_desc_docs:
        db.crime_descriptions.insert_many(crime_desc_docs)
    db.crime_descriptions.create_index("crm_cd", unique=True)
    
def store_officers(db, officers):
    print("Storing officers")
    db.officers.drop()
    result = db.officers.insert_many(officers)
    print(f"Inserted {len(result.inserted_ids)} officers into `officers` collection.")
    
    # index on badge_number to ensure each is unique
    db.officers.create_index([("badge_number", 1)], unique=True)
    print("Created a unique index on badge_number.")
    
    total_officers = db.officers.count_documents({})
    print(f"Total officers in collection: {total_officers}")
    

def print_stats(df):
    for col in df.columns.tolist():
        unique_count = df[col].dropna().nunique()
        print(f"{col}: {unique_count} distinct values")
    
def build_crime_documents(df):
    print("Building crime documents")
    crime_cds_map = {}
    cleaned_cd_def = df[['Crm Cd 1','Crm Cd Desc']].dropna(subset=['Crm Cd 1','Crm Cd Desc']).drop_duplicates()
    for _, row in cleaned_cd_def.iterrows():
        try:
            code = int(row['Crm Cd 1'])
            crime_cds_map[code] = row['Crm Cd Desc']
        except:
            pass
        
    # Build the main documents
    crime_documents = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        if row["DR_NO"] is None:
            continue

        try:
            # Gather up to 4 crime codes
            cd_array = []
            for col in ["Crm Cd 1", "Crm Cd 2", "Crm Cd 3", "Crm Cd 4"]:
                val = row[col]
                if val is not None:
                    val_int = int(val)
                    cd_array.append({
                        "code": val_int,
                        "desc": crime_cds_map.get(val_int, "Unknown")
                    })

            doc = {
                "dr_no": int(row["DR_NO"]),
                "date_rptd": row["Date Rptd"] if isinstance(row["Date Rptd"], pd.Timestamp) else None,
                "date_occ": row["DATE OCC"] if isinstance(row["DATE OCC"], pd.Timestamp) else None,
                "time_occ": int(row["TIME OCC"]) if row["TIME OCC"] is not None and not pd.isna(row["TIME OCC"]) else None,
                "area_code": int(row["AREA"]) if row["AREA"] is not None and not pd.isna(row["AREA"]) else None,
                # We do NOT store 'area_name' here to avoid duplication.
                "rpt_dist_no": int(row["Rpt Dist No"]) if row["Rpt Dist No"] is not None and not pd.isna(row["Rpt Dist No"]) else None,
                # "mocodes": row["Mocodes"],  # Possibly large, consider removing if not needed
                "victim_age": int(row["Vict Age"]) if row["Vict Age"] is not None and not pd.isna(row["Vict Age"]) else None,
                "victim_sex": row["Vict Sex"],
                "victim_descent": row["Vict Descent"],
                "premis_cd": int(row["Premis Cd"]) if row["Premis Cd"] is not None and not pd.isna(row["Premis Cd"]) else None,
                # "premis_desc": row["Premis Desc"],  # Omit for duplication reasons
                "weapon_code": int(row["Weapon Used Cd"]) if row["Weapon Used Cd"] is not None and not pd.isna(row["Weapon Used Cd"]) else None,
                # "weapon_desc": row["Weapon Desc"],  # omit for duplication reasons
                # "status": row["Status"],
                # "status_desc": row["Status Desc"],  # omit if you want a status reference collection as well
                "crm_cd_list": cd_array,
                "location": row["LOCATION"],
                "lat": float(row["LAT"]) if row["LAT"] is not None and not pd.isna(row["LAT"]) else None,
                "lon": float(row["LON"]) if row["LON"] is not None and not pd.isna(row["LON"]) else None
            }
            crime_documents.append(doc)
        except Exception as e:
            print(f"Error building doc for DR_NO = {row['DR_NO']}: {e}")
            continue

    return crime_documents

def main():
    uri = "mongodb+srv://pzazos:08gFAWabTBcHZjge@lapdcrimes.wrl2r.mongodb.net/?retryWrites=true&w=majority&appName=LAPDcrimes"
    client = MongoClient(uri)
    print("Connected to MongoDB")
    db = client["LAPDcrimes"]
    db.crime_reports.drop()
    
    current_path = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_path, '..', '..', 'Crime_Data_from_2020_to_Present_20241103.csv')
    csv_path = os.path.abspath(csv_path)
    
    print("Reading data from CSV")
    df = pd.read_csv(csv_path, dtype=str)
    df = preprocess_data(df)
    
    officers = generate_officers()
    store_officers(db, officers)
    crime_documents = build_crime_documents(df)
    db.crime_reports.drop()
    print(f"Inserting {len(crime_documents)} documents into crime_reports...")
    if crime_documents:
        chunk_size = 10000
        total_chunks = (len(crime_documents) + chunk_size - 1) // chunk_size
        with tqdm(total=total_chunks, desc="Inserting documents") as pbar:
            for i in range(0, len(crime_documents), chunk_size):
                chunk = crime_documents[i:i+chunk_size]
                result = db.crime_reports.insert_many(chunk)
                pbar.update(1)
    else:
        print("No documents to insert.")
        client.close()
        print("Connection closed")
        exit()
        
    upvoting_reports(db, officers)
    total_crime_reports = db.crime_reports.count_documents({})
    print(f"Total documents in crime_reports: {total_crime_reports}")

    client.close()
    print("Connection closed")

if __name__ == "__main__":
    main()