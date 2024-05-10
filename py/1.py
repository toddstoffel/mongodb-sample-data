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
    {"$match": {"year": 2020}},
    {
        "$group": {
            "_id": "$carrier.airline",
            "flight_count": {"$sum": 1},
            "diverted": {"$sum": "$diverted"},
            "cancelled": {"$sum": "$cancelled"},
        }
    },
    {
        "$project": {
            "airline": "$_id",
            "_id": 0,
            "flight_count": 1,
            "cancelled_pct": {
                "$round": [
                    {"$multiply": [{"$divide": ["$cancelled", "$flight_count"]}, 100]},
                    2,
                ]
            },
            "diverted_pct": {
                "$round": [
                    {"$multiply": [{"$divide": ["$diverted", "$flight_count"]}, 100]},
                    2,
                ]
            },
        }
    },
    {
        "$group": {
            "_id": None,
            "total_volume": {"$sum": "$flight_count"},
            "flights": {"$push": "$$ROOT"},
        }
    },
    {"$unwind": "$flights"},
    {
        "$project": {
            "airline": "$flights.airline",
            "flight_count": "$flights.flight_count",
            "cancelled_pct": "$flights.cancelled_pct",
            "diverted_pct": "$flights.diverted_pct",
            "market_share_pct": {
                "$round": [
                    {
                        "$multiply": [
                            {"$divide": ["$flights.flight_count", "$total_volume"]},
                            100,
                        ]
                    },
                    2,
                ]
            },
        }
    },
    {"$sort": {"flight_count": -1}},
]

results = flights_collection.aggregate(pipeline)

for result in results:
    try:
        print(
            "|".join(
                [
                    str(result.get("airline", "")),
                    str(result.get("flight_count", "")),
                    str(result.get("cancelled_pct", "")),
                    str(result.get("diverted_pct", "")),
                    str(result.get("market_share_pct", "")),
                ]
            )
        )
    except Exception as e:
        print(f"An error occurred: {e}")
