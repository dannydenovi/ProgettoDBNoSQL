from neo4j import GraphDatabase


def drop_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")
    return


def create_people(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row \
            CREATE (n:person \
            {FullName:row.FullName, FirstName:row.FirstName, LastName:row.LastName, Number:row.Number})")
    return


def create_cells(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///cells.csv' AS row \
            CREATE (n:cell {CellSite:row.CellSite, State:row.State, City:row.City, Address:row.Address})")
    return


def create_calls(tx):
    tx.run("LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row \
            CREATE (n:call {CallingNbr:row.CallingNbr, CalledNbr:row.CalledNbr, StartDate:row.StartDate, \
            EndDate:row.EndDate, Duration:row.Duration, CellSite:row.CellSite})")


def create_is_calling(tx):
    tx.run("MATCH (p:person), (c:call) \
            WHERE p.Number = c.CallingNbr \
            CREATE (p)-[r:is_calling]->(c)")


def create_is_called(tx):
    tx.run("MATCH (p:person), (c:call) \
            WHERE p.Number = c.CalledNbr \
            CREATE (c)-[r:is_called]->(p)")
    

def create_is_located(tx):
    tx.run("MATCH (c1:call), (c2:cell) \
            WHERE c1.CellSite = c2.CellSite \
            MERGE (c1)-[r:is_done]->(c2)")


def insert_neo(ip: str = "localhost", port: str = "7687", user: str = "neo4j", passwd: str = "neo4j", debug: bool = False):
    uri = "neo4j://" + ip + ":" + port
    driver = GraphDatabase.driver(uri, auth=(user, passwd))

    with driver.session() as db:
#       Drop per debug
        if debug:
            db.write_transaction(drop_all)
            print("NEO4J: Drop del database effettuato.")

#       Entità
        db.write_transaction(create_people)
        if debug:
            print("NEO4J: Persone inserite")
        
        db.write_transaction(create_cells)
        if debug:
            print("NEO4J: Celle inserite")
        
        db.write_transaction(create_calls)
        if debug:
            print("NEO4J: Chiamate inserite")
            
#       Relazioni
        db.write_transaction(create_is_calling)
        if debug:
            print("NEO4J: Relazione is_calling creata")
        
        db.write_transaction(create_is_called)
        if debug:
            print("NEO4J: Relazione is_called creata")
            
        db.write_transaction(create_is_located)
        if debug:
            print("NEO4J: Relazione is_located creata")
            
    driver.close()
    return
