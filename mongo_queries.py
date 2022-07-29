#!/bin/env python3
from datetime               import datetime
from scripts.connections    import connect_mongo
from argparse               import ArgumentParser
from time                   import time, mktime
from sys                    import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def exec_query(query: list[str, ...], client=connect_mongo(), t: bool = False) -> float:
    start = timestamp()
    match query[0]:
        case 'f':
            client.test.calls.find(query[1])
        case 'a':
            client.test.calls.aggregate(query[1])

    if t:
        return timestamp(start)
    return 0


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

    args    = parser.parse_args()
    time_f  = args.time
    num     = int(args.N)

    queries = [
        [
            'f',
            {'StartDate':
                {'$gte': str(int(mktime(datetime(2020, 1, 15).timetuple())))}
             }
        ],
        [
            'f',
            {'StartDate':
                {'$gte': str(int(mktime(datetime(2020, 1, 15).timetuple()))),
                 '$lt': str(int(mktime(datetime(2020, 1, 20).timetuple())))}
             }
        ],
        [
            'a',
            [
                {'$lookup':
                    {'from': "people", 'localField': "CallingNbr", 'foreignField': "Number", 'as': "Calling"}
                 },
                {'$lookup':
                    {'from': "people", 'localField': "CalledNbr", 'foreignField': "Number", 'as': "Called"}
                 },
                {'$project':
                    {"Called": 1}
                 }
            ]
        ],
        [
            'a',
            [
                {'$lookup':
                    {'from': "people", 'localField': "CallingNbr", 'foreignField': "Number", 'as': "Calling"}
                 },
                {'$lookup':
                    {'from': "people", 'localField': "CalledNbr", 'foreignField': "Number", 'as': "Called"}
                 },
                {'$lookup':
                    {'from': "cells", 'localField': "CellSite", 'foreignField': "CellSite", 'as': "Cell"}
                 },
                {'$project':
                    {"Called": 1}
                 }
            ]
        ],
        [
            'a',
            [
                {'$lookup':
                    {'from': "people", 'localField': "CallingNbr", 'foreignField': "Number", 'as': "Calling"}
                 },
                {'$lookup':
                    {'from': "people", 'localField': "CalledNbr", 'foreignField': "Number", 'as': "Called"}
                 },
                {'$lookup':
                    {'from': "cells", 'localField': "CellSite", 'foreignField': "CellSite", 'as': "Cell"}
                 },
                {'$match':
                    {"Cell.City": "Messina"}
                 },
                {'$project':
                    {"Called": 1}
                 }
            ]
        ]
    ]

    if 0 < num < 6:
        print(exec_query(queries[num - 1], t=time_f))
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)
