#!/bin/env python3
from scripts.connections    import connect_neo
from argparse               import ArgumentParser
from time                   import time
from sys                    import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def exec_query(qry: str, client=connect_neo(), t: bool = False) -> float:
    start = timestamp()
    with client.session() as db:
        db.run(qry)
        if t:
            return timestamp(start)
    return 0


if __name__ == "__main__":
    parser = ArgumentParser(description="Executes some queries to Neo4j.")
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
        "MATCH (c:call) WHERE c.StartDate >= '1579046400' AND c.StartDate < '1579132800' RETURN c",
        #"MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person)                     \
        #WHERE c.StartDate >= '1580197490'   \
        #RETURN c"

        "MATCH (p:person)-[r:is_calling]->(c:call) WHERE c.CallingNbr = '3390806281' AND c.FirstName <> 'a' RETURN distinct p",
        #"MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person)                    \
        #WHERE c.StartDate >= '1580197490' AND c.StartDate < '1580515200'    \
        #RETURN c",

        "MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person)   \
        WHERE c.StartDate >= '1580197490' AND c.StartDate < '1580515200' AND p1.FirstName = 'Giuliano'   \
        RETURN p1, p2, r1, r2, c",

        "MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person), (c)-[r3: is_located]->(ce:cell)\
        WHERE c.StartDate >= '1580197490' AND c.StartDate < '1580515200' AND p1.FirstName = 'Giuliano' \
        RETURN p1, p2, r1, r2, r3, c, ce",

        "MATCH(p1:person)-[r1: is_calling]->(c:call) < -[r2: is_called]-(p2:person), (c)-[r3: is_located]->(ce:cell)\
        WHERE ce.City = 'Messina' OR ce.City = 'Napoli' AND c.StartDate >= '1580197490' AND c.StartDate < '1580515200' AND p1.FirstName = 'Giuliano' \
        RETURN c"
    ]

    if 0 < num < 6:
        print(exec_query(queries[num - 1], t=time_f))
        #with open('csv/neo_result_' + str(num) + '.csv', 'w') as f:
        #    sum = 0
        #    for _ in range(30):
        #        tmp = exec_query(queries[num - 1], t=time_f)
        #        f.write(str(tmp) + '\n')
        #        sum += tmp
        #print(sum / 30)
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)











