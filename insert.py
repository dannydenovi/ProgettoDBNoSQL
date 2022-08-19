#!/bin/env python3
from sys                    import exit
from scripts.mongo_insert   import insert_mongo
from scripts.neo_insert     import insert_neo
from threading              import Thread
from argparse               import ArgumentParser


parser = ArgumentParser(description="Inserts files into either MongoDB or Neo4j databases, or both of them.")
parser.add_argument('-d', '--debug',
                    action="store_true",
                    default=False,
                    help='Debugging, it deletes all collections in \'test\' database.')
parser.add_argument('-m', '--mongo',
                    action="store_true",
                    default=False,
                    help='Insert files only to MongoDB database.')
parser.add_argument('-n', '--neo',
                    action="store_true",
                    default=False,
                    help='Insert files only to Neo4j database.')
parser.add_argument('-a', '--all',
                    action="store_true",
                    default=False,
                    help='Insert files only to Neo4j database.')
parser.add_argument('-p', '--percentage',
                    dest="P",
                    required=True,
                    help='Specify the percentage.')



args    = parser.parse_args()
debug   = args.debug
mongo, neo = (args.mongo, args.neo) if not args.all else (args.all, args.all)


if mongo:
    if not neo:
        insert_mongo(debug=debug, dim=args.P)
    else:
        thread_mongo = Thread(target=insert_mongo, kwargs={'debug': debug, 'dim': args.P})
        thread_mongo.start()

if neo:
    insert_neo(debug=debug, dim=args.P)

if 'thread_mongo' in locals():
    thread_mongo.join()

print("Inserimenti finiti.")
exit(0)
