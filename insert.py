from sys import argv, exit
from scripts.mongo_insert import insert_mongo
from scripts.neo_insert import insert_neo
from threading import Thread
import argparse


parser = argparse.ArgumentParser(description="Inserts files into either MongoDB or Neo4j databases, or both of them.")
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

if len(argv) - 1:
    if "-d" in argv:
        debug = True
        print("Debug attivo.")

#   mongo, neo = (True, True) if "--all" in argv else (True, False) if "--mongo" in argv else (False, True) if "--neo" in argv else (False, False)

    if "--all" in argv:
        mongo = neo = True
    else:
        if "--mongo" in argv:
            mongo = True
        if "--neo" in argv:
            neo = True

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
