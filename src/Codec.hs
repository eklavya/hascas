{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Codec where


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
import qualified Data.Map.Strict                as DMS

import           Common
import           Data.List                      as List
import           Data.Maybe
import           Data.Monoid                    as DM
import qualified Data.Set                       as Set
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network


instance Binary CQLDouble where
  get = CQLDouble <$> getFloat64be
  put (CQLDouble d) = putFloat64be d


instance (Ord k, Binary k, Binary v) => Binary (CQLMap k v) where
  get = do
    zeroLen <- isEmpty
    if zeroLen
      then
        pure $ CQLMap DMS.empty
      else
        do
          num <- get :: Get Int32
          ls <- replicateM (fromIntegral num :: Int) $ do
            r <- get :: Get Bytes
            let Bytes b = r
            let kv = runGet (get :: Get k) b
            r <- get :: Get Bytes
            let Bytes b = r
            let vv = runGet (get :: Get v) b
            return (kv, vv)
          return $ CQLMap $ DMS.fromList ls
  put (CQLMap m) = do
      let n = DMS.size m
      put (fromIntegral n :: Int32)
      forM_ (DMS.toList m) (\(k, v) -> do
        let l = (addLength . runPut . put) k <> (addLength . runPut . put) v
        putLazyByteString l)


instance (Binary a, Ord a) => Binary (CQLSet a) where
  get = do
    zeroLen <- isEmpty
    if zeroLen
      then
        pure $ CQLSet Set.empty
      else
        do
          num <- get :: Get Int32
          ls <- replicateM (fromIntegral num :: Int) getValue
          return $ CQLSet $ Set.fromList ls
  put (CQLSet s) = do
      let n = Set.size s
      put (fromIntegral n :: Int32)
      forM_ (Set.toList s) (\el -> do
        let l = (addLength . runPut .put) el
        putLazyByteString l)


getValue :: (Binary a) => Get a
getValue = do
            r <- get :: Get Bytes
            let Bytes b = r
            return $ runGet get b


instance (Binary a, Ord a) => Binary (CQLList a) where
  get = do
    zeroLen <- isEmpty
    if zeroLen
      then
        pure $ CQLList []
      else
        do
          num <- get :: Get Int32
          ls <- replicateM (fromIntegral num :: Int) getValue
          return $ CQLList ls
  put (CQLList ls) = do
      let n = List.length ls
      put (fromIntegral n :: Int32)
      forM_ ls (\ el -> do
        let l = (addLength . runPut . put) el
        putLazyByteString l)

instance Binary CQLString where
  get = (CQLString . DBL.toStrict) <$> getRemainingLazyByteString
  put (CQLString s) = putByteString s
