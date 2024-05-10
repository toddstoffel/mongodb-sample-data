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
    {
        "$match": {
            "dest.state": "CA",
            "year": 2020,
        }
    },
    {
        "$group": {
            "_id": {
                "airline": "$carrier.airline",
                "airport": "$dest.airport",
            },
            "volume": {"$sum": 1},
            "total_arrival_delay": {"$sum": "$arr_delay"},
        }
    },
    {
        "$project": {
            "_id": 1,
            "volume": 1,
            "avg_arrival_delay": {"$divide": ["$total_arrival_delay", "$volume"]},
        }
    },
    {
        "$project": {
            "_id": 1,
            "volume": 1,
            "avg_arrival_delay": {"$round": ["$avg_arrival_delay", 6]},
        }
    },
    {
        "$sort": {
            "_id.airline": 1,
            "_id.airport": 1,
        }
    },
]

results = flights_collection.aggregate(pipeline)

for result in results:
    try:
        _id = result.get("_id", {})
        airline = _id.get("airline", "")
        airport = _id.get("airport", "")
        volume = result.get("volume", "")
        avg_arrival_delay = result.get("avg_arrival_delay", "")

        print(
            "|".join(
                [
                    str(airline),
                    str(airport),
                    str(volume),
                    str(avg_arrival_delay),
                ]
            )
        )
    except Exception as e:
        print(f"An error occurred: {e}")
