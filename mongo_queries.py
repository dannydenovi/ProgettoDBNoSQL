#!/bin/env python3
from scripts.connections import connect_mongo
from datetime import datetime
from argparse import ArgumentParser
from time import time, mktime
from sys import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def exec_query(n: int, client=connect_mongo(), t: bool = False) -> float:
    start = timestamp()
    if n == 1:
            query_1(client)
    elif n == 2:
            query_2(client)
    elif n == 3:
            query_3(client)
    elif n == 4:
            query_4(client)
    elif n == 5:
            query_5(client)
    if t:
        return timestamp(start)
    return 0


def query_1(client=connect_mongo()):
    query = {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple()))}}
    z = []
    client.test.calls.find(query)
    #for x in client.test.calls.find(query):
    #    z += [x['CalledNbr']]
    # for x in client.test.cells.find(query):
    #    z += [y['CallingNbr'] for y in client.test.calls.find({'CellSite': x['CellSite']})]
    return


def query_2(client=connect_mongo()):
    query = {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple())),
                           '$lt': int(mktime(datetime(2020, 1, 29).timetuple()))}}
    client.test.calls.find(query)
    return


def query_3(client=connect_mongo()):
    query = {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 15).timetuple())), '$lt': int(mktime(datetime(2020, 1, 16).timetuple()))}}
    client.test.calls.find(query)
    return


def query_4(client=connect_mongo()):
    client.test.calls.aggregate([{"$lookup": {"from": "people", "localField":'CallingNbr', "foreignField":"Number", "as": "Calling"}}, {"$lookup": {"from": "people", "localField":'CalledNbr', "foreignField":"Number", "as": "Called"}}])
    return


def query_5(client=connect_mongo()):
    return


if __name__ == "__main__":
    parser = ArgumentParser(description="Executes some queries to MongoDB.")
    parser.add_argument('-t', '--time',
                        action="store_true",
                        default=False,
                        help='Prints execution time.')
    parser.add_argument('-n',
                        dest="N",
                        required=True,
                        help='Specify which query to execute [1-5].')

    args = parser.parse_args()
    time_f = args.time
    num = int(args.N)

    if 0 < num < 6:
        print(exec_query(num, t=time_f))
        # with open('csv/mongo_result_'+str(num)+'.csv', 'w') as f:
        #    sum = 0
        #    for _ in range(30):
        #        tmp = exec_query(queries[num - 1], t=time_f)
        #        f.write(str(tmp)+'\n')
        #        sum += tmp
        # print(sum/30)
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)
