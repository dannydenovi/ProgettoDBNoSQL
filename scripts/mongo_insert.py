import pymongo
from csv import DictReader


def insert_mongo(ip: str = "localhost", port: str = "27017", debug: bool = False):
    conn_str = "mongodb://"+ip+":"+port+"/test?retryWrites=true&w=majority"
    db = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000).test

    if debug:
        db.people.drop()
        db.calls.drop()
        db.cells.drop()

    with open("../csv/people.csv") as peoplef:
        rows = DictReader(peoplef)
        db.people.create_index("Number", unique=True)
        db.people.insert_many(rows)
        if debug:
            print("MONGODB: Persone inserite")

    with open("../csv/cells.csv") as cellsf:
        rows = DictReader(cellsf)
        db.cells.create_index([("City", 1), ("State", 2), ("Address", 3)], unique=True)
        db.cells.insert_many(rows)
        if debug:
            print("MONGODB: Celle inserite")

    with open("../csv/calls.csv") as callsf:
        rows = DictReader(callsf)
        db.calls.insert_many(rows)
        if debug:
            print("MONGODB: Chiamate inserite")

    return
