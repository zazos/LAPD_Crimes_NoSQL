## LAPD Crimes NoSQL API

### Requirements

Install all dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

### Initialize server

```bash
python scripts/run_api.py
```

## API Routes

### Crime Statistics

#### Get the number of crimes grouped by crime codes within a date range
```bash
GET http://127.0.0.1:5000/reports/count_by_crime_code?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

#### Get the daily count of a specific crime code within a date range
```bash
GET http://127.0.0.1:5000/reports/count_by_day?crime_code=XXXX&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

#### Get the most frequently occurring crimes in different police areas on a specific date
```bash
GET http://127.0.0.1:5000/reports/top_crimes_by_area?date=YYYY-MM-DD
```

#### Get the least reported crimes within a specified date range
```bash
GET http://127.0.0.1:5000/reports/least_common_crimes?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

---

### Officer Analysis

#### Get the top 50 most active officers by the number of reported crimes
```bash
GET http://127.0.0.1:5000/officers/top50_most_active_officers
```

#### Get the top 50 officers ranked by the number of unique areas where they filed reports
```bash
GET http://127.0.0.1:5000/officers/top50_officers_by_areas
```

#### Get the areas where a specific officer has received upvotes for their crime reports
```bash
GET http://127.0.0.1:5000/officers/voted_areas_by_officer?officer_id=XXXX
```

---

### 5.3. Upvote-Based Insights

#### Get the top 50 most upvoted crime reports for a specific date
```bash
GET http://127.0.0.1:5000/reports/top_50_upvoted_reports?date=YYYY-MM-DD
```

#### Identify reports where multiple officers have reported the same crime (potential inconsistencies)
```bash
GET http://127.0.0.1:5000/reports/problematic_reports
```

---

### Weapon and Crime Analysis Across Areas

#### Get the weapons used in crimes across different areas, filtered by crime code
```bash
GET http://127.0.0.1:5000/reports/weapons_by_crime_across_areas?crime_code=XXXX
```

