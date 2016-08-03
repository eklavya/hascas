{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Encoding where


import           Common
import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBQueue
import           Control.Exception              (bracket, catch)
import           Control.Monad                  (forM_, forever, replicateM)
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
import qualified Data.Set                       as Set
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network


instance Binary StringMap where
  put (StringMap m) = do
    put (fromIntegral (DMS.size m) :: Int16)
    forM_ (DMS.toList m) (\(k, v) -> put k >> put v)

  get = do
    len <- get :: Get Int16
    pl <- replicateM (fromIntegral len :: Int) $ do
      k <- get :: Get ShortStr
      v <- get :: Get ShortStr
      return (k, v)
    return $ StringMap $ DMS.fromList pl


getColumnSpec ksname tname = do
  colName <- get
  tpe <- get
  case tpe of
    t | t == 32 || t == 34 -> do
      elTpe <- get
      return ColumnSpec {ksname = ksname, tname = tname, colName, tpe, elTpe = Just elTpe, elTpeV = Nothing}
    33 -> do
      elTpe <- get
      elTpeV <- get
      return ColumnSpec {ksname = ksname, tname = tname, colName, tpe, elTpe = Just elTpe, elTpeV = Just elTpeV}
    _ ->
      return ColumnSpec {ksname = ksname, tname = tname, colName, tpe, elTpe = Nothing, elTpeV = Nothing}


getMeta :: Get Metadata
getMeta = do
  mflags <- get
  numCols <- get
  if mflags .&. 1 == 1
    then do
      gts <- get :: Get GlobalTableSpec
      colSpecs <- replicateM (fromIntegral numCols :: Int) $ getColumnSpec Nothing Nothing
      return Metadata {mflags, numCols, gts = Just gts, colSpecs}
    else do
      colSpecs <- replicateM (fromIntegral numCols :: Int) $ do
        ksname <- get :: Get ShortStr
        tname <- get :: Get ShortStr
        getColumnSpec (Just ksname) (Just tname)
      return Metadata {mflags, numCols, gts = Nothing, colSpecs}


getRows :: Get Rows
getRows = do
  meta <- getMeta
  numRows <- get :: Get Int32
  rs <- replicateM (fromIntegral numRows :: Int) $ mapM (\cs -> do
      r <- get :: Get Bytes
      let (ShortStr s) = colName cs
      return (CQLString $ DBL.toStrict s, (tpe cs, elTpe cs, elTpeV cs, r))) $ colSpecs meta
  return Rows { content = fmap DMS.fromList rs}


instance FromCQL Int8 where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get Int8) b) <$> DMS.lookup s r

instance FromCQL Int16 where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get Int16) b) <$> DMS.lookup s r

instance FromCQL Int32 where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get Int32) b) <$> DMS.lookup s r

instance FromCQL Int64 where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get Int64) b) <$> DMS.lookup s r

instance FromCQL Double where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet getFloat64be b) <$> DMS.lookup s r

instance FromCQL Bool where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get Bool) b) <$> DMS.lookup s r

instance FromCQL UUID where
  fromCQL r s = (\(_, _, _, Bytes b) -> runGet (get :: Get UUID) b) <$> DMS.lookup s r

instance FromCQL CQLString where
  fromCQL r s = (\(_, _, _, Bytes b) -> (CQLString . DBL.toStrict) b) <$> DMS.lookup s r

instance (FromCQL k, FromCQL v, Ord k, Binary k, Binary v) => FromCQL (DMS.Map k v) where
  fromCQL r s =  (\(_, kt, vt, Bytes b) -> runGet (if DBL.length b == 0
    then
      pure DMS.empty
    else
      do
      num <- get :: Get Int32
      ls <- replicateM (fromIntegral num :: Int) $ do
        kv <- get :: Get k
        vv <- get :: Get v
        return (kv, vv)
      return $ DMS.fromList ls
    ) b) <$> DMS.lookup s r
