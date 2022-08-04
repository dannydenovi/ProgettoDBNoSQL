from scripts.connections import connect_mongo
from csv import DictReader


def to_dict(csv: [dict]):
    for k, e in enumerate(csv):
        for f, i in e.items():
            if i.isdigit():
                e[f] = int(i)
    return csv


def insert_mongo(ip: str = "localhost", port: str = "27017", debug: bool = False):
    client = connect_mongo(ip, port)
    db = client.test
    if debug:
        client.drop_database('test')
    del client

    with open("./csv/people.csv") as peoplef:
        rows = to_dict(list(DictReader(peoplef)))
        db.people.create_index("Number", unique=True)
        db.people.insert_many(rows)
        if debug:
            print("MONGODB: Persone inserite")

    with open("./csv/cells.csv") as cellsf:
        rows = to_dict(list(DictReader(cellsf)))
        db.cells.insert_many(rows)
        if debug:
            print("MONGODB: Celle inserite")

    with open("./csv/calls.csv") as callsf:
        rows = to_dict(list(DictReader(callsf)))
        db.calls.insert_many(rows)
        if debug:
            print("MONGODB: Chiamate inserite")

    return
