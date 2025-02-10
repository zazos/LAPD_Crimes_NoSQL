from flask import Blueprint, request, jsonify, current_app
from datetime import datetime, timedelta

crime_reports_routes = Blueprint("crime_reports", __name__)

@crime_reports_routes.route("/reports/count_by_crime_code", methods=["GET"])
def count_by_crime_code():
    db = current_app.config["db"]

    try:
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        if not start_date_str or not end_date_str:
            return jsonify({"error": "Both 'start_date' and 'end_date' query parameters are required"}), 400
        
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

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
    db = current_app.config["db"]

    try:
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        crm_cd = request.args.get("crm_cd")
        if not start_date_str or not end_date_str or not crm_cd:
            return jsonify({"error": "Parameters 'start_date', 'end_date', and 'crm_cd' are required"}), 400

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
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
        response = [{"date": res["_id"]["date"].strftime("%Y-%m-%d"), "count": res["count"]} for res in result]
        return jsonify(response)
    
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Example: http://127.0.0.1:5000/reports/top_crimes_by_area?date=2023-01-01
@crime_reports_routes.route("/reports/top_crimes_by_area", methods=["GET"])
def top_crimes_by_area():
    db = current_app.config["db"]
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Parameter 'date' is required in YYYY-MM-DD format"}), 400
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        next_date = target_date + timedelta(days=1)
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400

    try:
        pipeline = [
            {"$match": {"date_occ": {"$gte": target_date, "$lt": next_date}}},
            {"$unwind": "$crm_cd_list"},
            {"$group": {
                "_id": {
                    "area_code": "$area_code",
                    "crime_code": "$crm_cd_list.code",
                    "crime_desc": "$crm_cd_list.desc"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.area_code": 1, "count": -1}},
            {"$group": {
                "_id": "$_id.area_code",
                "top_crimes": {"$push": {
                    "crime_code": "$_id.crime_code",
                    "crime_desc": "$_id.crime_desc",
                    "count": "$count"
                }}
            }},
            {"$project": {
                "area_code": "$_id",
                "top_crimes": {"$slice": ["$top_crimes", 3]},
                "_id": 0
            }}
        ]
        result = list(db.crime_reports.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Example: http://127.0.0.1:5000/reports/least_common_crimes?start_date=2023-05-01&end_date=2023-05-31
# Since many reports can have the same count, many reports occured only once in the time period, we limit to 2
# therefore random 2 crimes will be returned
@crime_reports_routes.route("/reports/least_common_crimes", methods=["GET"])
def least_common_crimes():
    db = current_app.config["db"]
    start_date_str = request.args.get("start_date")
    end_date_str = request.args.get("end_date")
    if not start_date_str or not end_date_str:
        return jsonify({"error": "Parameters 'start_date' and 'end_date' are required"}), 400
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1)  # inclusive end
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400

    try:
        pipeline = [
            {"$match": {"date_occ": {"$gte": start_date, "$lt": end_date}}},
            {"$unwind": "$crm_cd_list"},
            {"$group": {
                "_id": {"crime_code": "$crm_cd_list.code", "crime_desc": "$crm_cd_list.desc"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": 1}},
            {"$limit": 2},
            {"$project": {
                "_id": 0,
                "crime_code": "$_id.crime_code",
                "crime_desc": "$_id.crime_desc",
                "count": 1
            }}
        ]
        result = list(db.crime_reports.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Example: http://127.0.0.1:5000/reports/weapons_by_crime_across_areas?crm_cd=510
# Crime Code (crm_cd) is required as a query parameter to filter the results by crime code
@crime_reports_routes.route("/reports/weapons_by_crime_across_areas", methods=["GET"])
def weapons_by_crime_across_areas():
    db = current_app.config["db"]
    crm_cd = request.args.get("crm_cd")
    
    if not crm_cd:
        return jsonify({"error": "Parameter 'crm_cd' is required"}), 400

    try:
        crm_cd_int = int(crm_cd)
    except ValueError:
        return jsonify({"error": "Invalid crm_cd value"}), 400

    try:
        pipeline = [
            {"$unwind": "$crm_cd_list"},
            {"$match": {"crm_cd_list.code": crm_cd_int}},
            {"$group": {
                "_id": {
                    "crime_code": "$crm_cd_list.code",
                    "weapon_code": "$weapon_code"
                },
                "areas": {"$addToSet": "$area_code"}
            }},
            {"$project": {
                "crime_code": "$_id.crime_code",
                "weapon_code": "$_id.weapon_code",
                "area_count": {"$size": "$areas"}
            }},
            {"$match": {
                "area_count": {"$gt": 1},
                "weapon_code": {"$ne": None}
            }},
            {"$lookup": {
                "from": "weapons",
                "localField": "weapon_code",
                "foreignField": "weapon_code",
                "as": "weapon_info"
            }},
            {"$unwind": "$weapon_info"},
            {"$project": {
                "_id": 0,
                "crime_code": 1,
                "weapon_code": 1,
                "weapon_desc": "$weapon_info.weapon_desc",
                "area_count": 1
            }}
        ]
        result = list(db.crime_reports.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Example: http://127.0.0.1:5000/reports/top_50_upvoted_reports?date=2023-01-31
@crime_reports_routes.route("/reports/top_50_upvoted_reports", methods=["GET"])
def top_50_upvoted_reports():
    db = current_app.config["db"]
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Parameter 'date' is required in YYYY-MM-DD format"}), 400
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        next_date = target_date + timedelta(days=1)
    except ValueError:
        return jsonify({"error": "Invalid date format. Use 'YYYY-MM-DD'"}), 400

    try:
        pipeline = [
            {"$match": {"date_occ": {"$gte": target_date, "$lt": next_date}}},
            {"$lookup": {
                "from": "upvotes",
                "localField": "dr_no",
                "foreignField": "dr_no",
                "as": "upvotes"
            }},
            {"$addFields": {"upvote_count": {"$size": "$upvotes"}}},
            {"$sort": {"upvote_count": -1}},
            {"$limit": 50},
            {"$project": {
                "_id": 0,
                "dr_no": 1,
                "upvote_count": 1
            }}
        ]
        result = list(db.crime_reports.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Example: http://127.0.0.1:5000/officers/top50_most_active_officers
@crime_reports_routes.route("/officers/top50_most_active_officers", methods=["GET"])
def top50_most_active_officers():
    db = current_app.config["db"]
    try:
        pipeline = [
            {"$group": {
                "_id": {"badge_number": "$badge_number", "name": "$officer_name", "email": "$officer_email"},
                "upvote_count": {"$sum": 1}
            }},
            {"$sort": {"upvote_count": -1}},
            {"$limit": 50},
            {"$project": {
                "_id": 0,
                "badge_number": "$_id.badge_number",
                "name": "$_id.name",
                "email": "$_id.email",
                "upvote_count": 1
            }}
        ]
        result = list(db.upvotes.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Example: http://127.0.0.1:5000/officers/top50_officers_by_areas
@crime_reports_routes.route("/officers/top50_officers_by_areas", methods=["GET"])
def top50_officers_by_areas():
    db = current_app.config["db"]
    try:
        pipeline = [
            {"$lookup": {
                "from": "crime_reports",
                "localField": "dr_no",
                "foreignField": "dr_no",
                "as": "crime_report"
            }},
            {"$unwind": "$crime_report"},
            {"$group": {
                "_id": {"badge_number": "$badge_number", "name": "$officer_name", "email": "$officer_email"},
                "areas": {"$addToSet": "$crime_report.area_code"}
            }},
            {"$project": {
                "_id": 0,
                "badge_number": "$_id.badge_number",
                "name": "$_id.name",
                "email": "$_id.email",
                "area_count": {"$size": "$areas"}
            }},
            {"$sort": {"area_count": -1}},
            {"$limit": 50}
        ]
        result = list(db.upvotes.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Example: http://127.0.0.1:5000/reports/problematic_reports
# Query results in "error": "PlanExecutor error during aggregation :: caused by :: Exceeded memory limit for $group
@crime_reports_routes.route("/reports/problematic_reports", methods=["GET"])
def problematic_reports():
    db = current_app.config["db"]
    try:
        pipeline = [
            {"$group": {
                "_id": {"email": "$officer_email", "dr_no": "$dr_no"},
                "badge_numbers": {"$addToSet": "$badge_number"}
            }},
            {"$match": {
                "$expr": {"$gt": [{"$size": "$badge_numbers"}, 1]}
            }},
            {"$group": {
                "_id": None,
                "reports": {"$addToSet": "$_id.dr_no"}
            }},
            {"$project": {"_id": 0, "reports": 1}}
        ]
        # Pass allowDiskUse=True to let MongoDB use disk space when needed.
        result = list(db.upvotes.aggregate(pipeline, allowDiskUse=True))
        if result:
            return jsonify(result[0])
        else:
            return jsonify({"reports": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    
# Example: http://127.0.0.1:5000/officers/voted_areas_by_officer?name=Holly%20Martinez
@crime_reports_routes.route("/officers/voted_areas_by_officer", methods=["GET"])
def voted_areas_by_officer():
    db = current_app.config["db"]
    name = request.args.get("name")
    if not name:
        return jsonify({"error": "Parameter 'name' is required"}), 400
    try:
        pipeline = [
            {"$match": {"officer_name": name}},
            {"$lookup": {
                "from": "crime_reports",
                "localField": "dr_no",
                "foreignField": "dr_no",
                "as": "crime_report"
            }},
            {"$unwind": "$crime_report"},
            {"$group": {
                "_id": "$crime_report.area_code"
            }},
            {"$lookup": {
                "from": "areas",
                "localField": "_id",
                "foreignField": "area_code",
                "as": "area_info"
            }},
            {"$unwind": "$area_info"},
            {"$project": {
                "_id": 0,
                "area_code": "$_id",
                "area_name": "$area_info.area_name"
            }}
        ]
        result = list(db.upvotes.aggregate(pipeline))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500