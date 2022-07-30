#!/bin/env python3
from scripts.connections    import connect_neo
from argparse               import ArgumentParser
from time                   import time
from sys                    import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def exec_query(tx, qry: str):
    tx.run(qry)
    return


def call_query(qry: str, client=connect_neo(), t: bool = False) -> float:
    start = timestamp()
    with client.session() as db:
        db.write_transaction(exec_query, qry)
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

    args = parser.parse_args()
    time_f = args.time
    num = int(args.N)

    queries = [
        "MATCH(c:call)                      \
        WHERE c.StartDate >= '2020-01-15'   \
        RETURN c",

        "MATCH(c:call)                                                      \
        WHERE c.StartDate >= '2020-01-15' AND c.StartDate < '2020-01-20'    \
        RETURN c",

        "MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person)   \
        RETURN p1, p2, r1, r2, c",

        "MATCH(p1:person)-[r1: is_calling]->(c:call)<-[r2: is_called]-(p2:person), (c)-[r3: is_located]->(ce:cell)\
        RETURN p1, p2, r1, r2, r3, c, ce",

        "MATCH(p1:person)-[r1: is_calling]->(c:call) < -[r2: is_called]-(p2:person), (c)-[r3: is_located]->(ce:cell)\
        WHERE ce.City = 'Messina'                                                                                   \
        RETURN p1, p2, r1, r2, r3, c, ce"
    ]

    if 0 < num < 6:
        print(call_query(queries[num - 1], t=time_f))
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)











