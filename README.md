# hascas
Cassandra driver for haskell

This is a *work in progress* driver for cassandra.
It currently has:
* Select
* Insert
* Update
* Delete
* Prepared Queries
* Batch Queries

How it looks (looks pretty bad, but WIP) :
```Haskell
main :: IO ()
main = do
    ch <- CQL.init
    
    --create a keyspace
    let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
    runCQL ch LOCAL_ONE q
    
    --create a table
    let q = create "TABLE demodb.emp (empID int,deptID int,alive boolean,id uuid,first_name varchar,last_name varchar,salary double,age bigint,PRIMARY KEY (empID, deptID))"
    runCQL ch LOCAL_ONE q
    
    --execute prepared queries
    prepq <- prepare ch "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    case prepq of
      Left e -> print e
      Right p -> do
        res <- execCQL ch LOCAL_ONE p [
          put (104::Int32),
          put (15::Int32),
          put True,
          put $ fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b",
          put $ CQLString "Hot",
          put $ CQLString "Shot",
          putFloat64be 100000.0,
          put (98763::Int64)]

    --execute prepared queries and get results
    prepq <- prepare ch "select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?"
      case prepq of
        Left e -> print e
        Right p -> do
          res <- execCQL ch LOCAL_ONE p [put (104::Int32), put (15::Int32)]
          case res of
            Left e -> print e
            Right rs -> print $ (fromRow (Prelude.head rs) (ShortStr "salary")::Maybe Double)
    
    --select rows from table
    let q = select "demodb.emp" # where' "empID" (104::Int32) # and' "deptID" (15::Int32)
    rows <- runCQL ch LOCAL_ONE q
    case rows of
      Left e -> print e
      Right rs -> print $ (fromRow (Prelude.head rs) (ShortStr "salary")::Maybe Double)

    --drop a table
    let q = drop' "table demodb.emp"
    runCQL ch LOCAL_ONE q
    
    --drop a keyspace
    let q = drop' "keyspace demodb"
    runCQL ch LOCAL_ONE q

```

