import os
import pandas as pd
from pymongo import MongoClient
from .preprocess import preprocess_data
from .references import create_reference_documents
from .crime_reports import build_crime_documents, insert_crime_reports
from .upvotes import generate_officers, store_officers, assign_upvotes

def populate_db(uri, csv_filename):
    client = MongoClient(uri)
    db = client["LAPDcrimes"]
    
    print("Dropping existing collections...")
    db.crime_reports.drop()
    db.areas.drop()
    db.weapons.drop()
    db.crime_descriptions.drop()
    db.officers.drop()
    db.upvotes.drop()
    print("Collections dropped!")

    print("Reading data from CSV file...")
    df = pd.read_csv(csv_filename, dtype=str)
    print("Data read successfully!")

    print("Preprocessing data...")
    df = preprocess_data(df)
    print("Data preprocessed!")

    print("Creating reference documents...")
    create_reference_documents(df, db)
    print("Reference documents created!")
 
    print("Building crime documents...")
    crime_docs = build_crime_documents(df)
    print("Crime documents built!")
    
    print("Inserting crime reports...")
    insert_crime_reports(db, crime_docs)
    print("Crime reports inserted!")
    
    print("Generating officers...")
    officers = generate_officers()
    print("Officers generated!")
    
    print("Storing officers...")
    store_officers(db, officers)
    print("Officers stored!")
    
    print("Assigning upvotes...")
    assign_upvotes(db, officers, threshold=0.3, max_upvotes=1000)
    print("Upvotes assigned!")
    
    print("Database population complete!")
    client.close()
