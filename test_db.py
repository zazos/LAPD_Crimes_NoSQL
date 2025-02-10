from pymongo import MongoClient

uri = "mongodb+srv://pzazos:08gFAWabTBcHZjge@lapdcrimes.wrl2r.mongodb.net/?retryWrites=true&w=majority&appName=LAPDcrimes"

try:
    client = MongoClient(uri)
    print(client.list_database_names())
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
