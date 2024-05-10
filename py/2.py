import sys
from pathlib import Path

config_path = Path(__file__).resolve().parent.parent / "config.py"

if config_path.is_file():
    sys.path.append(str(config_path.parent))
    import config
else:
    raise ImportError("The config.py file does not exist in the expected directory.")

from pymongo import MongoClient
from config import (
    MONGO_HOST,
    MONGO_PORT,
    USERNAME,
    PASSWORD,
    ADMIN_DB,
    DB_NAME,
    COLLECTION_NAME,
)


def build_mongodb_uri(username, password, host, port, db_name):
    if username and password:
        credentials = f"{username}:{password}@"
        auth_source = "?authSource={ADMIN_DB}"
    else:
        credentials = ""
        auth_source = ""

    return f"mongodb://{credentials}{host}:{port}/{db_name}{auth_source}"


MONGODB_URI = build_mongodb_uri(USERNAME, PASSWORD, MONGO_HOST, MONGO_PORT, DB_NAME)

client = MongoClient(MONGODB_URI)
db = client[config.DB_NAME]
flights_collection = db[config.COLLECTION_NAME]

pipeline = [
    {"$match": {"has_delay": True}},
    {
        "$group": {
            "_id": {
                "airline": "$carrier.airline",
                "year": "$year",
                "delay_type": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$gt": ["$carrier_delay", 0]},
                                "then": "Airline Delay",
                            },
                            {
                                "case": {"$gt": ["$late_aircraft_delay", 0]},
                                "then": "Late Aircraft Delay",
                            },
                            {
                                "case": {"$gt": ["$nas_delay", 0]},
                                "then": "Air System Delay",
                            },
                            {
                                "case": {"$gt": ["$weather_delay", 0]},
                                "then": "Weather Delay",
                            },
                        ],
                        "default": "Other Delay",
                    }
                },
            },
            "delay": {"$sum": 1},
        }
    },
    {"$sort": {"_id.airline": 1, "_id.year": 1, "_id.delay_type": 1}},
    {
        "$project": {
            "_id": 0,
            "airline": "$_id.airline",
            "year": "$_id.year",
            "delay_type": "$_id.delay_type",
            "delay": "$delay",
        }
    },
]

# Execute the aggregation pipeline
results = db.flights.aggregate(pipeline)

# Print the results
for result in results:
    try:
        print(
            "|".join(
                [
                    str(result.get("airline", "")),
                    str(result.get("year", "")),
                    str(result.get("delay_type", "")),
                    str(result.get("delay", "")),
                ]
            )
        )
    except Exception as e:
        print(f"An error occurred: {e}")
