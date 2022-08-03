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

args    = parser.parse_args()
debug   = args.debug
mongo, neo = (args.mongo, args.neo) if not args.all else (args.all, args.all)

if mongo:
    thread_mongo = Thread(target=insert_mongo, kwargs={'debug': debug})
    thread_mongo.start()

if neo:
    thread_neo = Thread(target=insert_neo, kwargs={'debug': debug})
    thread_neo.start()

if 'thread_mongo' in locals():
    thread_mongo.join()

if 'thread_neo' in locals():
    thread_neo.join()

print("Inserimenti finiti.")
exit(0)
