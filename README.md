# MongoDB for SQL Developers
## Overview
This repository contains a straightforward dataset from the [Bureau of Transportation Statistics (BTS)](https://www.bts.gov/explore-topics-and-geography/topics/time-data), focusing on the on-time performance data of U.S. commercial airline flights.

The primary aim here is to demonstrate how traditional SQL queries can be translated into MongoDB commands, providing a bridge for those proficient with relational databases to understand and adapt to MongoDB's document-oriented approach.

## Requirements
Before you begin, ensure that the following tools are installed:

* [MongoDB](https://www.mongodb.com/try/download/community)
* [PyMongo](https://pypi.org/project/pymongo/)
* [gdown](https://pypi.org/project/gdown/)

## Setup Instructions
Clone the repository and navigate to the project directory:

```
git clone https://github.com/toddstoffel/mongodb-sample-data.git
cd mongodb-sample-data
```
Configure [config.py](config.py) with your settings (If necessary)
```
./run_project
```

## Comparative Queries
In the SQL directory, you'll find a collection of SQL queries. These are paralleled by scripts in the `py` (Python) and `js` (JavaScript) directories, which perform equivalent operations in MongoDB.

## Executing MongoDB JavaScript Queries
To run the JavaScript queries through the MongoDB shell, use the following commands:

```
mongosh bts < js/1.js
mongosh bts < js/2.js
mongosh bts < js/3.js
mongosh bts < js/4.js
mongosh bts < js/5.js
```

## Running Python Scripts
Execute the corresponding Python scripts like so:

```
python3 py/1.py
python3 py/2.py
python3 py/3.py
python3 py/4.py
python3 py/5.py
```

Engage with these examples to better grasp the syntactic and conceptual differences between SQL and MongoDB's query language.