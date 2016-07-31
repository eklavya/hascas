{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Main where


import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBQueue
import           Control.Exception              (bracket, catch)
import           Control.Monad                  (forM_, forever, replicateM)
import           Control.Monad.Reader
import           Control.Monad.Except
import           CQL
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.IEEE754
import           Data.Binary.Put
import           Data.Bits
import           Data.ByteString
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString.Lazy           as DBL
import           Data.Int
import qualified Data.IntMap                    as IM
import           Data.List
import qualified Data.Map.Strict                as DMS
import           Data.Maybe
import           Data.Monoid                    as DM
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network


main :: IO ()
main = do
    candle <- CQL.init "127.0.0.1" (PortNumber 9042)

    res <- flip runReaderT candle $ runExceptT $ do
      let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
      runCQL LOCAL_ONE q
      --create a table
      let q = create "TABLE demodb.emp (empID int,deptID int,alive boolean,id uuid,first_name varchar,last_name varchar,salary double,age bigint,PRIMARY KEY (empID, deptID))"
      runCQL LOCAL_ONE q

      --execute prepared queries
      p <- prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      execCQL LOCAL_ONE p [
            put (104::Int32),
            put (15::Int32),
            put True,
            put $ fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b",
            put $ CQLString "Hot",
            put $ CQLString "Shot",
            putFloat64be 100000.0,
            put (98763::Int64)]

      --execute prepared queries and get results
      p <- prepare "select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?"
      res <- execCQL LOCAL_ONE p [
            put (104::Int32),
            put (15::Int32)]
      liftIO $ print (fromRow (Prelude.head res) (ShortStr "salary")::Maybe Double)

      --select rows from table
      let q = select "demodb.emp" # where' "empID" (104::Int32) # and' "deptID" (15::Int32)
      rows <- runCQL LOCAL_ONE q
      liftIO $ print (fromRow (Prelude.head rows) (ShortStr "salary")::Maybe Double)

      --batch queries
      p <- prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      let q = batch (update "demodb.emp" # with "first_name" (CQLString "some name") # where' "empID" (104::Int32) # and' "deptID" (15::Int32)) <>
            batch (update "demodb.emp" # with "last_name" (CQLString "no name") # where' "empID" (104::Int32) # and' "deptID" (15::Int32)) <>
            prepBatch p [
                        put (101::Int32),
                        put (13::Int32),
                        put True,
                        put $ fromJust $ fromString "48d0ceb1-9e3e-427c-bc36-0106398f672b",
                        put $ CQLString "Hot1",
                        put $ CQLString "Shot1",
                        putFloat64be 10000.0,
                        put (9763::Int64)]
      runBatch q

      --drop a table
      let q = drop' "table demodb.emp"
      runCQL LOCAL_ONE q

      --drop a keyspace
      let q = drop' "keyspace demodb"
      runCQL LOCAL_ONE q

    print res
