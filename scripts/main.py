import os
from data.populate_db import populate_db

def main():
    current_path = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_path, "..", "..", "Crime_Data_from_2020_to_Present_20241103.csv")
    csv_path = os.path.abspath(csv_path)
    uri = "mongodb+srv://pzazos:08gFAWabTBcHZjge@lapdcrimes.wrl2r.mongodb.net/?retryWrites=true&w=majority&appName=LAPDcrimes"
    populate_db(uri, csv_path)

if __name__ == "__main__":
    main()
