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
import           Data.Set
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network
import           Test.Hspec


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
    ch <- CQL.init "127.0.0.1" (PortNumber 9042)

    hspec $
      describe "driver should be able to" $ do
        it "create a keyspace" $ do
          let q = create "keyspace demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "create a table" $ do
          let tableQuery = "TABLE demodb.emp (empID bigint,deptID int,alive boolean,id uuid,name varchar,salary double,"
                       ++ "someset set<double>,somelist list<int>,somemap map<text, text>,PRIMARY KEY (empID, deptID))"
          let q = create tableQuery
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "execute prepared queries" $ do
          prep <- (runReaderT . runExceptT) (prepare "INSERT INTO demodb.emp (empID,deptID,alive,id,name,salary,somelist,someset,somemap) VALUES (?,?,?,?,?,?,?,?,?)") ch
          case prep of
            Left e -> print e
            Right p -> do
              res <- (runReaderT . runExceptT) (execCQL LOCAL_ONE p [
                put (104::Int64),
                put (15::Int32),
                put True,
                put $ fromJust $ fromString "38d0ceb1-9e3e-427c-bc36-0106398f672b",
                put $ CQLString "Hot Shot",
                put $ CQLDouble 100000.0,
                put ((CQLList [1,2,3,4,5,6]) :: CQLList Int32),
                put ((CQLSet $ fromList [CQLDouble 0.001, CQLDouble 1000.0]) :: CQLSet CQLDouble),
                put $ CQLMap $ DMS.fromList [(CQLString "some", CQLString "Things")]]) ch
              res `shouldBe` Right []

        it "execute prepared queries and get results" $ do
          prep <- (runReaderT . runExceptT) (prepare "select empID, deptID, alive, id, name, salary, someset, somemap, somelist from demodb.emp where empid = ? and deptid = ?") ch
          case prep of
            Left e -> print e
            Right p -> do
              res <- (runReaderT . runExceptT) (execCQL LOCAL_ONE p [
                put (104::Int64),
                put (15::Int32)]) ch
              fmap fromRow <$> res `shouldBe` Right [Just (Emp {empID = 104, deptID = 15, alive = True, Main.id = fromJust $ fromString $ "38d0ceb1-9e3e-427c-bc36-0106398f672b", name = CQLString "Hot Shot", salary = CQLDouble 100000.0, someList = CQLList [1,2,3,4,5,6], someSet = CQLSet (fromList [CQLDouble 1.0e-3,CQLDouble 1000.0]), someMap = CQLMap $ DMS.fromList [(CQLString "some",CQLString "Things")]})]

        it "select rows from table" $ do
          let q = select "demodb.emp" # where' "empID" (104::Int64) # and' "deptID" (15::Int32)
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          fmap fromRow <$> res `shouldBe` Right [Just (Emp {empID = 104, deptID = 15, alive = True, Main.id = fromJust $ fromString $ "38d0ceb1-9e3e-427c-bc36-0106398f672b", name = CQLString "Hot Shot", salary = CQLDouble 100000.0, someList = CQLList [1,2,3,4,5,6], someSet = CQLSet (fromList [CQLDouble 1.0e-3,CQLDouble 1000.0]), someMap = CQLMap $ DMS.fromList [(CQLString "some",CQLString "Things")]})]

        it "batch queries" $ do
          prep <- (runReaderT . runExceptT) (prepare "INSERT INTO demodb.emp (empID, deptID, alive, id, name, salary) VALUES (?, ?, ?, ?, ?, ?)") ch
          case prep of
            Left e -> print e
            Right p -> do
              let q = batch (update "demodb.emp" # with "name" (CQLString "some name") # where' "empID" (104::Int64) # and' "deptID" (15::Int32)) <>
                      batch (update "demodb.emp" # with "alive" False # where' "empID" (104::Int64) # and' "deptID" (15::Int32)) <>
                      prepBatch p [
                        put (101::Int64),
                        put (13::Int32),
                        put True,
                        put $ fromJust $ fromString "48d0ceb1-9e3e-427c-bc36-0106398f672b",
                        put $ CQLString "Hot1 Shot1",
                        put $ CQLDouble 10000.0]
              res <- (runReaderT . runExceptT) (runBatch q) ch
              res `shouldBe` Right []
              let q = select "demodb.emp" # where' "empID" (101::Int64) # and' "deptID" (13::Int32)
              res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
              fmap fromRow <$> res `shouldBe` Right [Just (Emp {empID = 101, deptID = 13, alive = True, Main.id = fromJust $ fromString $ "48d0ceb1-9e3e-427c-bc36-0106398f672b", name = CQLString "Hot1 Shot1", salary = CQLDouble 10000.0, someList = CQLList [], someSet = CQLSet Data.Set.empty, someMap = CQLMap DMS.empty})]

        it "drop a table" $ do
          let q = drop' "table demodb.emp"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []

        it "drop a keyspace" $ do
          let q = drop' "keyspace demodb"
          res <- (runReaderT . runExceptT) (runCQL LOCAL_ONE q) ch
          res `shouldBe` Right []
