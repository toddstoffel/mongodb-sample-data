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
            "$and": [
                {"arr_delay": {"$gt": 0}},
                {"dest.code": {"$in": ["SFO", "OAK", "SJC"]}},
                {"year": 2020},
            ]
        }
    },
    {
        "$addFields": {
            "monthname": {
                "$let": {
                    "vars": {
                        "monthsInString": [
                            None,
                            "January",
                            "February",
                            "March",
                            "April",
                            "May",
                            "June",
                            "July",
                            "August",
                            "September",
                            "October",
                            "November",
                            "December",
                        ]
                    },
                    "in": {"$arrayElemAt": ["$$monthsInString", "$month"]},
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "dest": "$dest.code",
            "month": 1,
            "monthname": 1,
            "scheduled_arrival_hr": {"$toInt": {"$substr": ["$crs_arr_time", 0, 2]}},
            "arr_delay": 1,
        }
    },
    {
        "$group": {
            "_id": {
                "dest": "$dest",
                "month": "$month",
                "monthname": "$monthname",
                "scheduled_arrival_hr": "$scheduled_arrival_hr",
            },
            "avg_arr_delay": {"$avg": "$arr_delay"},
            "max_arr_delay": {"$max": "$arr_delay"},
        }
    },
    {
        "$project": {
            "_id": 0,
            "dest": "$_id.dest",
            "month": "$_id.month",
            "monthname": "$_id.monthname",
            "scheduled_arrival_hr": "$_id.scheduled_arrival_hr",
            "avg_arr_delay": {"$round": ["$avg_arr_delay", 6]},
            "max_arr_delay": 1,
        }
    },
    {
        "$sort": {
            "dest": 1,
            "month": 1,
            "scheduled_arrival_hr": 1,
        }
    },
]

results = list(flights_collection.aggregate(pipeline))

for result in results:
    try:
        print(
            "|".join(
                [
                    str(result.get("dest", "")),
                    str(result.get("month", "")),
                    str(result.get("monthname", "")),
                    str(result.get("scheduled_arrival_hr", "")),
                    str(result.get("avg_arr_delay", "")),
                    str(result.get("max_arr_delay", "")),
                ]
            )
        )
    except Exception as e:
        print(f"An error occurred: {e}")
