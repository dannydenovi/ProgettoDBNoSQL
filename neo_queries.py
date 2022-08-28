#!/bin/env python3
from scripts.connections import connect_neo, parse
from time import time
from statistics import stdev
from sys import exit


def timestamp(start: float = 0) -> float:
    return time()*1000 - start


def exec_query(query: str, client=connect_neo(), t: bool = False) -> float:
    start = timestamp()
    with client.session() as db:
        db.run(query)

    return timestamp(start) if t else 0


def clear_cache():
    exec_query("CALL db.clearQueryCaches()")
    return


if __name__ == "__main__":
    args = parse()
    num = int(args.N)
    perc = args.P + "_" if args.P != "" else ""

    queries = [
       "MATCH (c:call)  \
        WHERE c.StartDate >= 1580083200 \
            AND c.StartDate < 1580256000   \
        RETURN c",

       "MATCH (p1:person)-[r1:is_calling]->(c:call)  \
        WHERE c.StartDate >= 1580083200 \
            AND c.StartDate < 1580256000   \
        RETURN p1, r1, c",

       "MATCH (p1:person)-[r1:is_calling]->(c:call)-[r2:is_done]->(ce:cell) \
        WHERE c.StartDate >= 1580083200 \
            AND c.StartDate < 1580256000   \
        RETURN p1, r1, c, r2, ce",

       "MATCH (p1:person)-[r1:is_calling]->(c:call)-[r2:is_done]->(ce:cell) \
        WHERE c.StartDate >= 1580083200 \
            AND c.StartDate < 1580256000 \
            AND c.Duration > 900   \
        RETURN p1, r1, c, r2, ce"
    ]

    if 0 < num < 5:
        clear_cache()
        with open('csv/neo_result_' + perc + str(num) + '.csv', 'w') as f:
            f.write("First," + str(exec_query(queries[num - 1], t=args.time)) + "\n")
            tmp = []
            for i in range(30):
                tmp += [exec_query(queries[num - 1], t=args.time)]
                f.write("," + str(tmp[i]) + '\n')
            f.write("Mean," + str(sum(tmp)/30) + "\n")
            f.write("Std. Dev.," + str(stdev(tmp)) + "\n")
    else:
        print("Wrong query number. Only from 1 to 4.")
    exit(0)
