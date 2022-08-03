

# Query 1
```sql
    SELECT CallingNbr 
    FROM Calls 
        WHERE StartDate >= 2020-01-27
```

# Query 2
```sql
    SELECT CallingNbr 
    FROM Calls 
        JOIN Cells ON Calls.CellSite = Cells.CellSite 
            WHERE StartDate >= 2020-01-15
```

# Query 3



```
MATCH(p1:person)-[r1: is_calling]->(c:call)-[r2: is_called]->(p2:person)   \
WHERE c.StartDate >= '1580197490' AND c.StartDate < '1580515200' \
RETURN p1, p2, r1, r2, c


client.test.calls.aggregate([{"$lookup": {"from": "people", "localField":'CallingNbr', "foreignField":"Number", "as": "Calling"}}, {"$lookup": {"from": "people", "localField":'CalledNbr', "foreignField":"Number", "as": "Called"}},{"$match": {"StartDate": {'$gte': 1580197490, '$lt': 1580515200}}}])
```


# Tempi di querying 

| query | mongodb | neo4j |
|-------|---------|-------|
|   1   | 0.011   | 0.013 |
|   2   | 0.019   | 0.017 |
|   3   | 0.15    | 0.025 |
|   4   | 0.84    | 0.028 |
|   5   | 1.23    | 0.032 |