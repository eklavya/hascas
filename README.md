# hascas
Cassandra CQL driver for haskell

This is a *work in progress* driver for cassandra.

GHC 7.10.3 and 8.0.x are supported.

It currently has:
* Select
* Insert
* Update
* Delete
* Prepared Queries
* Batch Queries
* Automatic Records Conversion
* Collections


**The driver gets the list of all nodes in the cluster and load balances amongst them.**

# Example
Look at the project in the example directory.

How it looks :
```Haskell
data Emp = Emp {
  empID    :: Int64,
  deptID   :: Int32,
  alive    :: Bool ,
  id       :: UUID,
  name     :: CQLString,
  salary   :: CQLDouble,
  someList :: CQLList Int32,
  someSet  :: CQLSet CQLDouble,
  someMap  :: CQLMap CQLString CQLString
}
  deriving(Show, Eq)

deriveBuildRec ''Emp

main :: IO ()
main = do
    candle <- CQL.init "127.0.0.1" (PortNumber 9042)

    res <- flip runReaderT candle $ runExceptT $ do
      let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
      runCQL LOCAL_ONE q

      --create a table
      let tableQuery = "TABLE demodb.emp (empID bigint,deptID int,alive boolean,id uuid,name varchar,salary double,"
                       ++ "someset set<double>,somelist list<int>,somemap map<text, text>,PRIMARY KEY (empID, deptID))"

      let q = create tableQuery
      runCQL LOCAL_ONE q

      --execute prepared queries
      p <- prepare "INSERT INTO demodb.emp (empID,deptID,alive,id,name,salary,somelist,someset,somemap) VALUES (?,?,?,?,?,?,?,?,?)"
      execCQL LOCAL_ONE p [
            put (104::Int64),
            put (15::Int32),
            put True,
            put $ fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b",
            put $ CQLString "Hot Shot",
            put $ CQLDouble 100000.0,
            put ((CQLList [1,2,3,4,5,6]) :: CQLList Int32),
            put ((CQLSet $ fromList [CQLDouble 0.001, CQLDouble 1000.0]) :: CQLSet CQLDouble),
            put $ CQLMap $ DMS.fromList [(CQLString "some", CQLString "Things")]]

      -- execute prepared queries and get results
      p <- prepare "select empID, deptID, alive, id, name, salary, someset, somemap, somelist from demodb.emp where empid = ? and deptid = ?"
      res <- execCQL LOCAL_ONE p [
            put (104::Int64),
            put (15::Int32)]
      liftIO $ print $ catMaybes ((fmap fromRow res)::[Maybe Emp])
      liftIO $ print (fromCQL (Prelude.head res) (CQLString "salary")::Maybe Double)
      liftIO $ print (fromCQL (Prelude.head res) (CQLString "name")::Maybe CQLString)

      --select rows from table
      let q = select "demodb.emp" # where' "empID" (104::Int64) # and' "deptID" (15::Int32)
      rows <- runCQL LOCAL_ONE q
      liftIO $ print $ catMaybes ((fmap fromRow res)::[Maybe Emp])

      --batch queries
      p <- prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, name, salary) VALUES (?, ?, ?, ?, ?, ?)"
      let q = batch (update "demodb.emp" # with "name" (CQLString "some name") # where' "empID" (104::Int64) # and' "deptID" (15::Int32)) <>
            batch (update "demodb.emp" # with "alive" False # where' "empID" (104::Int64) # and' "deptID" (15::Int32)) <>
            prepBatch p [
                        put (101::Int64),
                        put (13::Int32),
                        put True,
                        put $ fromJust $ fromString "48d0ceb1-9e3e-427c-bc36-0106398f672b",
                        put $ CQLString "Hot1 Shot1",
                        put $ CQLDouble 10000.0]
      runBatch q

      --drop a table
      let q = drop' "table demodb.emp"
      runCQL LOCAL_ONE q

      --drop a keyspace
      let q = drop' "keyspace demodb"
      runCQL LOCAL_ONE q

    print res
```
