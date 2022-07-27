# ProgettoDBNoSQL
Progetto DBNoSQL De Novi - Anchesi

## Importazione CSV

```cypher
LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row
CREATE (n:person {FullName:row.FullName, FirstName:row.FirstName, LastName:row.LastName, Number:row.Number})
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///cells.csv' AS row
CREATE (n:cell {CellSite:row.CellSite, State:row.State, City:row.City, Address:row.Address})
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
CREATE (n:call {CallingNbr:row.CallingNbr, CalledNbr:row.CalledNbr, StartDate:row.StartDate, EndDate:row.EndDate, Duration:row.Duration, CellSite:row.CellSite})
```

```cypher
MATCH (p:person), (c:call)
WHERE p.Number = c.CallingNbr 
CREATE (p)-[r:is_calling]->(c)
```

```cypher
MATCH (p:person), (c:call)
WHERE p.Number = c.CalledNbr
CREATE (c)-[r:is_called]->(p)
```

```cypher
MATCH (c1:call), (c2:cell)
WHERE c1.CellSite = c2.CellSite
MERGE (c1)-[r:is_done]->(c2)
```

```cypher
MATCH (c:cell), (l:location)
WHERE c.City = l.City
MERGE (c)-[r:is_located]->(l)
```

```bash
\
mongoimport \
--db test \
--collection people \
--type csv \
--upsertFields="Number" \
--headerline \
--file "/csv/people.csv"
```

```bash
\
mongoimport \
--db test \
--collection cells \
--type csv \
--upsertFields="City","State","Address" \
--headerline \
--file "/csv/cells.csv"
```

```bash
\
mongoimport \
--db test \
--collection calls \
--type csv \
--headerline \
--file "/csv/calls.csv"
```
