import sys
from sys import argv
from mongo import insert_mongo
from neo import insert_neo
from threading import Thread


debug = mongo = neo = False

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
sys.exit(0)
