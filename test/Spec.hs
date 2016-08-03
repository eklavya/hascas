{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}


module Main where


import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBQueue
import           Control.Exception              (bracket, catch)
import           Control.Monad                  (forM_, forever, replicateM)
import           Control.Monad.Except
import           Control.Monad.Reader
import           CQL
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.Bits
import           Data.ByteString
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString.Lazy           as DBL
import           Data.Int
import qualified Data.IntMap                    as IM
import           Data.List
import           Data.Map.Strict                (lookup)
import qualified Data.Map.Strict                as DMS
import           Data.Maybe
import           Data.Monoid                    as DM
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network
import           Test.Hspec


data Emp = Emp { empID :: Int32, deptID :: Int32, alive :: Bool , id :: UUID, first_name :: CQLString, last_name :: CQLString, salary :: CQLDouble, age :: Int64 }
  deriving(Show, Eq)

deriveBuildRec ''Emp



main :: IO ()
main = do
    ch <- CQL.init "127.0.0.1" (PortNumber 9042)

    hspec $
      describe "driver should be able to" $ do
        it "create a keyspace" $ do
          let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "create a table" $ do
          let q = create "TABLE demodb.emp (empID int,deptID int,alive boolean,id uuid,first_name varchar,last_name varchar,salary double,age bigint,PRIMARY KEY (empID, deptID))"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "execute prepared queries" $ do
          prep <- (runReaderT . runExceptT) (prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)") ch
          case prep of
            Left e -> print e
            Right p -> do
              res <- (runReaderT . runExceptT) (execCQL LOCAL_ONE p [
                put (104::Int32),
                put (15::Int32),
                put True,
                put $ fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b",
                put $ CQLString "Hot",
                put $ CQLString "Shot",
                put $ CQLDouble 100000.0,
                put (98763::Int64)]) ch
              res `shouldBe` Right []

        it "execute prepared queries and get results" $ do
          prep <- (runReaderT . runExceptT) (prepare "select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?") ch
          case prep of
            Left e -> print e
            Right p -> do
              res <- (runReaderT . runExceptT) (execCQL LOCAL_ONE p [
                put (104::Int32),
                put (15::Int32)]) ch
              fmap fromRow <$> res `shouldBe` Right [Just Emp {empID = 104, deptID = 15, alive = True, Main.id = fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b", first_name = CQLString "Hot", last_name = CQLString "Shot", salary = CQLDouble 100000.0, age = 98763}]

        it "select rows from table" $ do
          let q = select "demodb.emp" # where' "empID" (104::Int32) # and' "deptID" (15::Int32)
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          fmap fromRow <$> res `shouldBe` Right [Just Emp {empID = 104, deptID = 15, alive = True, Main.id = fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b", first_name = CQLString "Hot", last_name = CQLString "Shot", salary = CQLDouble 100000.0, age = 98763}]

        it "batch queries" $ do
          prep <- (runReaderT . runExceptT) (prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)") ch
          case prep of
            Left e -> print e
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
                        put $ CQLDouble 10000.0,
                        put (9763::Int64)]
              res <- (runReaderT . runExceptT) (runBatch q) ch
              res `shouldBe` Right []
              let q = select "demodb.emp" # where' "empID" (101::Int32) # and' "deptID" (13::Int32)
              res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
              fmap fromRow <$> res `shouldBe` Right [Just Emp {empID = 101, deptID = 13, alive = True, Main.id = fromJust $ fromString "48d0ceb1-9e3e-427c-bc36-0106398f672b", first_name = CQLString "Hot1", last_name = CQLString "Shot1", salary = CQLDouble 10000.0, age = 9763}]

        it "drop a table" $ do
          let q = drop' "table demodb.emp"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "drop a keyspace" $ do
          let q = drop' "keyspace demodb"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []
