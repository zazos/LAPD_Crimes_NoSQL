from flask import Flask, Blueprint
from pymongo import MongoClient
from api.routes import crime_reports_routes

def create_app():
    app = Flask(__name__)
    client = MongoClient("mongodb+srv://pzazos:08gFAWabTBcHZjge@lapdcrimes.wrl2r.mongodb.net/?retryWrites=true&w=majority&appName=LAPDcrimes")
    app.config["db"] = client["LAPDcrimes"]
    app.register_blueprint(crime_reports_routes)
    return app
