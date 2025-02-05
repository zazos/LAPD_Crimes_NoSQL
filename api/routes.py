from flask import Blueprint, request, jsonify, current_app
from datetime import datetime

crime_reports_routes = Blueprint("crime_reports", __name__)

@crime_reports_routes.route("/reports/count_by_crime_code", methods=["GET"])
def count_by_crime_code():
    db = current_app.config["db"]  # Get the database connection

    try:
        # Get query parameters
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        
        # Validate parameters
        if not start_date_str or not end_date_str:
            return jsonify({"error": "Both 'start_date' and 'end_date' query parameters are required"}), 400
        
        # Convert strings to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Perform the query
        result = db.crime_reports.aggregate([
            {"$match": {"date_occ": {"$gte": start_date, "$lte": end_date}}},
            {"$unwind": "$crm_cd_list"},
            {"$group": {"_id": "$crm_cd_list.code", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ])
        
        return jsonify(list(result))
    
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@crime_reports_routes.route("/reports/count_by_day", methods=["GET"])
def count_by_day():
    db = current_app.config["db"]  # Get the database connection

    try:
        # Get query parameters
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        crm_cd = request.args.get("crm_cd")

        # Validate parameters
        if not start_date_str or not end_date_str or not crm_cd:
            return jsonify({"error": "Parameters 'start_date', 'end_date', and 'crm_cd' are required"}), 400

        # Convert strings to datetime objects
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Perform the query
        result = db.crime_reports.aggregate([
            {"$match": {
                "date_occ": {"$gte": start_date, "$lte": end_date},
                "crm_cd_list.code": int(crm_cd)
            }},
            {"$group": {
                "_id": {"date": "$date_occ"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.date": 1}}
        ])

        # Format the response
        response = [{"date": res["_id"]["date"].strftime("%Y-%m-%d"), "count": res["count"]} for res in result]

        return jsonify(response)
    
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
