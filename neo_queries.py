#!/bin/env python3
from scripts.connections import connect_neo, parse
from time import time
from statistics import stdev
from sys import exit


def timestamp(start: float = 0) -> float:
    return time() - start


def exec_query(qry: str, client=connect_neo(), t: bool = False) -> float:
    start = timestamp()
    with client.session() as db:
        db.run(qry)
        if t:
            return timestamp(start)
    return 0


def clear_cache():
    exec_query("CALL db.clearQueryCaches()")
    return


if __name__ == "__main__":
    args = parse()
    num = int(args.N)

    queries = [
        "MATCH (c:call) \
         WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000   \
         RETURN c",

        "MATCH (p:person)-[r:is_calling]->(c:call)  \
         WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000   \
         RETURN p, r, c",

        "MATCH (p1:person)-[r1:is_calling]->(c:call)-[r2:is_called]->(p2:person)  \
         WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000   \
         RETURN p1, r1, c, r2, p2",
        
        "MATCH (p1:person)-[r1:is_calling]->(c:call)-[r2:is_called]->(p2:person)  \
         WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000 AND c.Duration > 900   \
         RETURN p1, r1, c, r2, p2",
         
        "MATCH (p1:person)-[r1:is_calling]->(c:call)-[r2:is_called]->(p2:person), (c)-[r3:is_done]->(ce:cell) \
         WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000 AND c.Duration > 900   \
         RETURN p1, r1, c, r2, p2, r3, ce"
    ]

    #queries = [
    #    "MATCH(p1:person)-[r1: is_calling]->(c:call)\
    #    RETURN p1, r1, c",

    #    "MATCH(p1:person)-[r1: is_calling]->(c:call)-[r2: is_called]->(p2:person)                    \
    #    RETURN p1, r1, c, r2 ,p2",

    #    "MATCH(p1:person)-[r1: is_calling]->(c:call)-[r2: is_called]->(p2:person)  \
    #    WHERE c.StartDate >= 1580083200 AND c.StartDate < 1580256000 \
    #    RETURN p1, p2, r1, r2, c",

    #    "MATCH(p1:person)-[r1: is_calling]->(c:call)-[r2: is_called]->(p2:person), (c)-[r3:is_done]->(ce:cell)   \
    #    WHERE c.StartDate >= 1580197490 AND c.StartDate < 1580515200 \
    #    RETURN p1, p2, r1, r2, r3, ce, c",

    #    "MATCH(p1:person)-[r1: is_calling]->(c:call)-[r2: is_called]->(p2:person), (c)-[r3: is_located]->(ce:cell)\
    #    WHERE c.StartDate >= 1580197490 AND c.StartDate < 1580515200 AND c.Duration > 900 \
    #    RETURN p1, p2, r1, r2, r3, ce, c"
    #]

    if 0 < num < 6:
        clear_cache()
        with open('csv/neo_result_' + str(num) + '.csv', 'w') as f:
            f.write("First," + str(exec_query(queries[num - 1], t=args.time)) + "\n")
            tmp = []
            for i in range(30):
                if args.c:
                    clear_cache()
                tmp += [exec_query(queries[num - 1], t=args.time)]
                f.write("," + str(tmp[i]) + '\n')
            f.write("Mean," + str(sum(tmp)/30) + "\n")
            f.write("Std. Dev.," + str(stdev(tmp)) + "\n")
        # print(sum(tmp)/30)
    else:
        print("Wrong query number. Only from 1 to 5.")
    exit(0)
