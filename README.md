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
candle <- CQL.init

let q = select "demodb.transactions" # limit 1
rows <- runCQL candle LOCAL_ONE q
case rows of
  Left err -> print err
  Right rs -> print (fromRow (Prelude.head rs) (ShortStr "quantity")::Maybe Double)

prep <- prepare candle "select * from demodb.transactions where vendor_id = ?"
case prep of
  Left er -> print er
  Right p -> do
    res <- execCQL candle LOCAL_ONE p [fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b"]
    let qw = update "demodb.products" # (with "name" (CQLString "some name")) # whre "vendor_id" (fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b") # (aand "product_id" (fromJust $ fromString "069af83a-3104-433a-bf4d-ed23e8cd5c1a"))
    res <- runBatch candle $ batch qw <> prepBatch p [fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b"]
    print res
    
let attrs = select "demodb.products" # whre "vendor_id" (fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b") # (aand "product_id" (fromJust $ fromString "9ad9c6a9-ee52-4e84-8b17-5e2c70975c00"))
rows <- runCQL candle LOCAL_ONE attrs
case rows of
  Left err -> print err
  Right rs -> do
    print (fromRow (Prelude.head rs) (ShortStr "attributes")::Maybe (DMS.Map ShortStr ShortStr))
```

