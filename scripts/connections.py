#!/bin/env python3
from pymongo import MongoClient
from neo4j import GraphDatabase


def connect_mongo(ip: str = "localhost", port: str = "27017"):
    conn_str = "mongodb://" + ip + ":" + port + "/test?retryWrites=true&w=majority"
    return MongoClient(conn_str, serverSelectionTimeoutMS=5000)


def connect_neo(ip: str = "localhost", port: str = "7687", user: str = "neo4j", passwd: str = "neo4j"):
    uri = "neo4j://" + ip + ":" + port
    return GraphDatabase.driver(uri, auth=(user, passwd))
