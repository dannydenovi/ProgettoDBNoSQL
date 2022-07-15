# ProgettoDBNoSQL
Progetto DBNoSQL De Novi - Anchesi

## Importazione CSV

```cypher
LOAD CSV WITH HEADERS FROM 'file:///cells.csv' AS row
CREATE (n:cell {id:row.CellSite, state:row.State, city:row.City, address:row.Address})
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row
CREATE (n:person {full_name:row.FullName, first_name:row.FirstName, last_name:row.LastName, number:row.CallingNbr})
```

```cypher
MATCH (p:person), (c:call)
WHERE p.full_name = c.calling 
CREATE (p) - [r:is_calling] -> (c)
```

```cypher
MATCH (p:person), (c:call)
WHERE p.full_name = c.called 
CREATE (c) - [r:is_called] -> (p)
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///prova.csv' AS row
CREATE (n:call {calling:row.CallingNbr, called:row.CalledNbr,start_date:row.StartDate, end_date:row.EndDate, duration:row.Duration})
```

```cypher
MATCH (c1:call), (c2:cell)  WHERE c1.cell = c2.id  
CREATE (c1) - [r:is_located] -> (c2)
```
