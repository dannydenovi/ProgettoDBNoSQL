from scripts.connections    import connect_mongo
from csv                    import DictReader
from time                   import time



def to_dict(csv: [dict]):
    for k, e in enumerate(csv):
        for f, i in e.items():
            if i.isdigit():
                e[f] = int(i)
    return csv

def timestamp(start: float = 0) -> float:
    return time() - start

def insert_mongo(ip: str = "localhost", port: str = "27017", debug: bool = False, dim: int = 25):
    client = connect_mongo(ip, port)
    db = client.test
    if debug:
        client.drop_database('test')
        print("MONGODB: Drop del database effettuato.")

    del client

    start = timestamp()

    with open("./csv/people.csv") as peoplef:
        rows = to_dict(list(DictReader(peoplef)))
        db.people.create_index("Number", unique=True)
        db.people.insert_many(rows)
        if debug:
            print("MONGODB: Persone inserite.")

    with open("./csv/cells.csv") as cellsf:
        rows = to_dict(list(DictReader(cellsf)))
        db.cells.insert_many(rows)
        if debug:
            print("MONGODB: Celle inserite.")

    with open("./csv/calls.csv") as callsf:
        rows = to_dict(list(DictReader(callsf)))
        db.calls.insert_many(rows)
        if debug:
            print("MONGODB: Chiamate inserite.")

    with open("./csv/mongo_upload_time.csv", "a+") as timef:
        timef.write(str(dim)+"%: "+str(timestamp(start))+"\n")

    return
