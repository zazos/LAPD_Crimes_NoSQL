import random
from faker import Faker
from datetime import datetime
from pymongo.database import Database
from tqdm import tqdm

def generate_officers(n_officers=600):
    """
    600 officers (600 x 1000 = 600k capacity), thats slightly above 540k total upvotes. 
    
    For approximately 900k crime reports, upvoting 30% of them gives 270k and if each gets 2 upvotes, that's 540k
    """
    fake = Faker()
    officers = []
    used_badges = set()

    for _ in range(n_officers):
        badge_number = random.randint(1000, 999999)
        while badge_number in used_badges:
            badge_number = random.randint(1000, 999999)
        used_badges.add(badge_number)

        officers.append({
            "badge_number": badge_number,
            "name": fake.name(),
            "email": fake.email(),
            "upvotes": 0
        })
    return officers

def store_officers(db, officers):
    res = db.officers.insert_many(officers)
    print(f"Stored {len(res.inserted_ids)} officers.")
    db.officers.create_index("badge_number", unique=True)
    print("Created unique index on badge_number in 'officers'.")

def assign_upvotes(db, officers, threshold=0.3, max_upvotes=1000):
    """
    Randomly assigns upvotes to approximately 30% of the crime reports.
    Each selected report receives between 1 and a random number of upvotes (limited by the officer's maximum upvotes).
    
    The number of upvotes for each report ranges from 1 to 3.
    
    If there are not enough officers to assign the desired number of upvotes,
    limit the upvotes for that report to the number of available officers.
    """
        
    total_reports = db.crime_reports.count_documents({})
    if total_reports == 0:
        print("No crime reports found. Skipping upvotes.")
        return

    dr_no_list = [doc["dr_no"] for doc in db.crime_reports.find({}, {"dr_no":1, "_id":0})]

    upvote_count = int(len(dr_no_list) * threshold)
    chosen_reports = random.sample(dr_no_list, upvote_count)

    upvotes = []
    for dr_no in tqdm(chosen_reports, desc="Assigning Upvotes"):
        num_upvotes_for_this_report = random.randint(1, 3)
        
        valid_officers = [officer for officer in officers if officer["upvotes"] < max_upvotes]
        if len(valid_officers) < num_upvotes_for_this_report:
            print(f"Not enough officers to assign {num_upvotes_for_this_report} upvotes for DR_NO={dr_no}.")
            num_upvotes_for_this_report = len(valid_officers)
        
        chosen_officers = random.sample(valid_officers, num_upvotes_for_this_report)
        for chosen_officer in chosen_officers:
            upvote_doc = {
                "dr_no": dr_no,
                "badge_number": chosen_officer["badge_number"],
                "officer_name": chosen_officer["name"],
                "officer_email": chosen_officer["email"]
            }
            upvotes.append(upvote_doc)
            chosen_officer["upvotes"] += 1

    if upvotes:
        print(f"Inserting {len(upvotes)} upvote docs.")
        chunk_size = 10000
        for i in tqdm(range(0, len(upvotes), chunk_size), desc="Inserting Upvotes"):
            chunk = upvotes[i : i+chunk_size]
            db.upvotes.insert_many(chunk)

        db.upvotes.create_index([("dr_no", 1), ("badge_number", 1)], unique=True)
        print(f"Upvotes inserted: {db.upvotes.count_documents({})}")
    else:
        print("No upvotes to insert.")
