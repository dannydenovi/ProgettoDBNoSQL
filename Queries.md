## Query 1
```
db.calls.find({StartDate: {$gte: new Date("2020-01-15").getTime()/1000}})

MATCH (c:call)
WHERE c.StartDate >= "2020-01-15"
RETURN c
```

## Query 2
```
db.calls.find({StartDate: {$gte: new Date("2020-01-15").getTime()/1000, $lt: new Date("2020-01-20").getTime()/1000}})

MATCH (c:call)
WHERE c.StartDate >= "2020-01-15" AND c.StartDate < "2020-01-20"
RETURN c
```

## Query 3
```
db.calls.aggregate({$lookup: {from: "people", localField: "CallingNbr", foreignField: "Number", as: "Calling"}}, {$lookup: {from: "people", localField: "CalledNbr", foreignField: "Number", as: "Called"}}, {$project: {"Called": 1}})

MATCH (p1:person-[r1:is_calling]->(c:call)<-[r2:is_called]-(p2:person)
RETURN p1, p2, r1, r2, c
```

## Query 4
```
db.calls.aggregate({$lookup: {from: "people", localField: "CallingNbr", foreignField: "Number", as: "Calling"}}, {$lookup: {from: "people", localField: "CalledNbr", foreignField: "Number", as: "Called"}}, {$lookup: {from: "cells", localField: "CellSite", foreignField: "CellSite", as: "Cell"}}, {$project: {"Called": 1}})

MATCH (p1:person-[r1:is_calling]->(c:call)<-[r2:is_called]-(p2:person), (c)-[r3:is_located]->(ce:cell)
RETURN p1, p2, r1, r2, r3, c, ce
```

## Query 5
```
db.calls.aggregate({$lookup: {from: "people", localField: "CallingNbr", foreignField: "Number", as: "Calling"}}, {$lookup: {from: "people", localField: "CalledNbr", foreignField: "Number", as: "Called"}}, {$lookup: {from: "cells", localField: "CellSite", foreignField: "CellSite", as: "Cell"}}, {$match: {"Cell.City": "Messina"}}, {$project: {"Called": 1}})

MATCH (p1:person-[r1:is_calling]->(c:call)<-[r2:is_called]-(p2:person), (c)-[r3:is_located]->(ce:cell)
WHERE ce.City = "Messina"
RETURN p1, p2, r1, r2, r3, c, ce
```
