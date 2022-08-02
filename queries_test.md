

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
