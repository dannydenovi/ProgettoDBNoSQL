#!/bin/env python3
from pymongo import MongoClient
from neo4j import GraphDatabase
from argparse import ArgumentParser


def connect_mongo(ip: str = "localhost", port: str = "27017"):
    conn_str = "mongodb://" + ip + ":" + port + "/test?retryWrites=true&w=majority"
    return MongoClient(conn_str, serverSelectionTimeoutMS=5000)


def connect_neo(ip: str = "localhost", port: str = "7687", user: str = "neo4j", passwd: str = "neo4j"):
    uri = "neo4j://" + ip + ":" + port
    return GraphDatabase.driver(uri, auth=(user, passwd))


def parse():
    parser = ArgumentParser(description="Executes some queries to Neo4j.")
    parser.add_argument('-t', '--time',
                        action="store_true",
                        default=False,
                        help='Prints execution time.')
    parser.add_argument('-n',
                        dest="N",
                        required=True,
                        help='Specify which query to execute [1-5].')
    parser.add_argument('-c',
                        action="store_true",
                        default=False,
                        help='Clear cache')

    return parser.parse_args()
