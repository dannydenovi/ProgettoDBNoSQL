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
    if query[0] == 'f':
        z = []
        for x in client.test.calls.find(query[1]):
            z += [y['FullName'] for y in client.test.people.find({'Number': x['CalledNbr']})]
        print(z)
    elif query[0] == 'a':
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
            #[
            #    {'$lookup':
            #        {'from': 'people', 'localField': 'CallingNbr', 'foreignField': 'Number', 'as': "Calling"}
            #     },
            #    {'$lookup':
            #        {'from': 'people', 'localField': 'CalledNbr', 'foreignField': 'Number', 'as': 'Called'}
            #     },
            #    #{'$lookup':
            #    #    {'from': 'cells', 'localField': 'CellSite', 'foreignField': 'CellSite', 'as': 'Cell'}
            #    # },
            #    #{'$match':
            #    #    {'CallingNbr': '3390806281', 'Duration': {'$gt': '200'}}
            #    # },
            #    {'$project':
            #        {'Calling': 1}
            #     }
            #]
            {"Duration": {'$lt': 2}}#, {"CalledNbr": 1}
            #{'StartDate':
            #    {'$gte': str(int(mktime(datetime(2020, 1, 15).timetuple())))}
            # }
        ],
        [
            'f',

            [
                {'$lookup':
                    {'from': 'people', 'localField': 'CallingNbr', 'foreignField': 'Number', 'as': "Calling"}
                 },
                {'$lookup':
                    {'from': 'people', 'localField': 'CalledNbr', 'foreignField': 'Number', 'as': 'Called'}
                 },
                {},
                # {'$match':
                #    {'CallingNbr': '3390806281', 'Duration': {'$gt': '200'}}
                # },
                {'$project':
                    {'Calling': 1}
                 }
            ]
        ],
        [
            'a',
            {"CallingNbr": "3791033065", "Duration": {'$gte': '1200'}, "StartDate": {'$gte': "1579046400"}}, {"CalledNbr": 1}
            #[
            #    {'$lookup':
            #        {'from': "people", 'localField': "CallingNbr", 'foreignField': "Number", 'as': "Calling"}
            #     },
            #    {'$lookup':
            #        {'from': "people", 'localField': "CalledNbr", 'foreignField': "Number", 'as': "Called"}
            #     },
            #    {'$project':
            #        {"Called": 1}
            #     }
            #]
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
        #with open('csv/mongo_result_'+str(num)+'.csv', 'w') as f:
        #    sum = 0
        #    for _ in range(30):
        #        tmp = exec_query(queries[num - 1], t=time_f)
        #        f.write(str(tmp)+'\n')
        #        sum += tmp
        #print(sum/30)
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)
