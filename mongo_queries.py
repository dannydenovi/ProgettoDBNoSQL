#!/bin/env python3
from scripts.connections import connect_mongo, parse
from datetime import datetime
from argparse import ArgumentParser
from time import time, mktime
from statistics import stdev
from sys import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def clear_cache(client=connect_mongo()):
    client.test.command(
        {"planCacheClear": "calls"},
        {"planCacheClear": "cells"},
        {"planCacheClear": "people"}
    )
    return


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

    return timestamp(start) if t else 0


def query_1(client=connect_mongo()):
    query = [{"$match": {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple()))}}}]
    client.test.calls.aggregate(query)
    return


def query_2(client=connect_mongo()):
    query = [{"$match": {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple())),
                                       '$lt': int(mktime(datetime(2020, 1, 29).timetuple()))}}}]
    client.test.calls.aggregate(query)
    return


def query_3(client=connect_mongo()):
    query = [{"$match": {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple())),
                                       '$lt': int(mktime(datetime(2020, 1, 29).timetuple()))}}},
             {"$lookup": {"from": "people", "localField": 'CallingNbr', "foreignField": "Number", "as": "Calling"}}]
    client.test.calls.aggregate(query)
    return


def query_4(client=connect_mongo()):
    query = [{"$match": {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple())),
                                       '$lt': int(mktime(datetime(2020, 1, 29).timetuple()))}}},
             {"$lookup": {"from": "people", "localField": 'CallingNbr', "foreignField": "Number", "as": "Calling"}},
             {"$lookup": {"from": "cells", "localField": 'CellSite', "foreignField": "CellSite", "as": "Cell"}}]

    client.test.calls.aggregate(query)
    return


def query_5(client=connect_mongo()):
    query = [{"$match": {"StartDate": {'$gte': int(mktime(datetime(2020, 1, 27).timetuple())),
                                       '$lt': int(mktime(datetime(2020, 1, 29).timetuple()))},
                         "Duration": {"$gte": 900}}},
             {"$lookup": {"from": "people", "localField": 'CallingNbr', "foreignField": "Number", "as": "Calling"}},
             {"$lookup": {"from": "cells", "localField": 'CellSite', "foreignField": "CellSite", "as": "Cell"}}]
    client.test.calls.aggregate(query)
    return


if __name__ == "__main__":
    args = parse()
    num = int(args.N)
    perc = args.P + "_" if args.P != "" else ""

    if 0 < num < 6:
        clear_cache()
        with open('csv/mongo_result_' + perc + str(num) +'.csv', 'w') as f:
            f.write("First," + str(exec_query(num, t=args.time)) + "\n")
            tmp = []
            for i in range(30):
                tmp += [exec_query(num, t=args.time)]
                f.write("," + str(tmp[i])+'\n')
            f.write("Mean," + str(sum(tmp)/30) + "\n")
            f.write("Std. Dev.," + str(stdev(tmp)) + "\n")
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)