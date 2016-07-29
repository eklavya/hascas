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
import           Test.Hspec


main :: IO ()
main = do
    ch <- CQL.init

    hspec $ do
      describe "driver should be able to" $ do
        it "create a keyspace" $ do
          let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
          rows <- runCQL ch LOCAL_ONE q
          rows `shouldBe` Right []

        it "create a table" $ do
          let q = create "TABLE demodb.emp (empID int,deptID int,alive boolean,id uuid,first_name varchar,last_name varchar,salary double,age bigint,PRIMARY KEY (empID, deptID))"
          rows <- runCQL ch LOCAL_ONE q
          rows `shouldBe` Right []

        it "execute prepared queries" $ do
          prepq <- prepare ch "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
          case prepq of
            Left e -> do
              print e
              False `shouldBe` True
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
              res `shouldBe` Right []

        it "execute prepared queries and get results" $ do
          prepq <- prepare ch "select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?"
          case prepq of
            Left e -> do
              print e
              False `shouldBe` True
            Right p -> do
              res <- execCQL ch LOCAL_ONE p [
                put (104::Int32),
                put (15::Int32)]
              res `shouldBe` Right [DMS.fromList [(ShortStr "age",(2,Nothing,Nothing,Bytes "\NUL\NUL\NUL\NUL\NUL\SOH\129\203")),(ShortStr "alive",(4,Nothing,Nothing,Bytes "\SOH")),(ShortStr "deptid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NUL\SI")),(ShortStr "empid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NULh")),(ShortStr "first_name",(13,Nothing,Nothing,Bytes "Hot")),(ShortStr "id",(12,Nothing,Nothing,Bytes "8\208\206\177\158>B|\188\&6\SOH\ACK9\143g+")),(ShortStr "last_name",(13,Nothing,Nothing,Bytes "Shot")),(ShortStr "salary",(7,Nothing,Nothing,Bytes "@\248j\NUL\NUL\NUL\NUL\NUL"))]]

        it "select rows from table" $ do
          let q = select "demodb.emp" # where' "empID" (104::Int32) # and' "deptID" (15::Int32)
          rows <- runCQL ch LOCAL_ONE q
          rows `shouldBe` Right [DMS.fromList [(ShortStr "age",(2,Nothing,Nothing,Bytes "\NUL\NUL\NUL\NUL\NUL\SOH\129\203")),(ShortStr "alive",(4,Nothing,Nothing,Bytes "\SOH")),(ShortStr "deptid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NUL\SI")),(ShortStr "empid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NULh")),(ShortStr "first_name",(13,Nothing,Nothing,Bytes "Hot")),(ShortStr "id",(12,Nothing,Nothing,Bytes "8\208\206\177\158>B|\188\&6\SOH\ACK9\143g+")),(ShortStr "last_name",(13,Nothing,Nothing,Bytes "Shot")),(ShortStr "salary",(7,Nothing,Nothing,Bytes "@\248j\NUL\NUL\NUL\NUL\NUL"))]]

        it "batch queries" $ do
          prepq <- prepare ch "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
          case prepq of
            Left e -> do
              print e
              False `shouldBe` True
            Right p -> do
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
              rows <- runBatch ch q
              rows `shouldBe` Right []
              let q = select "demodb.emp" # where' "empID" (101::Int32) # and' "deptID" (13::Int32)
              rows <- runCQL ch LOCAL_ONE q
              rows `shouldBe` Right [DMS.fromList [(ShortStr "age",(2,Nothing,Nothing,Bytes "\NUL\NUL\NUL\NUL\NUL\NUL&#")),(ShortStr "alive",(4,Nothing,Nothing,Bytes "\SOH")),(ShortStr "deptid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NUL\r")),(ShortStr "empid",(9,Nothing,Nothing,Bytes "\NUL\NUL\NULe")),(ShortStr "first_name",(13,Nothing,Nothing,Bytes "Hot1")),(ShortStr "id",(12,Nothing,Nothing,Bytes "H\208\206\177\158>B|\188\&6\SOH\ACK9\143g+")),(ShortStr "last_name",(13,Nothing,Nothing,Bytes "Shot1")),(ShortStr "salary",(7,Nothing,Nothing,Bytes "@\195\136\NUL\NUL\NUL\NUL\NUL"))]]

        it "drop a table" $ do
          let q = drop' "table demodb.emp"
          rows <- runCQL ch LOCAL_ONE q
          rows `shouldBe` Right []

        it "drop a keyspace" $ do
          let q = drop' "keyspace demodb"
          rows <- runCQL ch LOCAL_ONE q
          rows `shouldBe` Right []
