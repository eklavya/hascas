{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Batch where


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

import           Common
import           Data.Maybe
import           Data.Monoid                    as DM
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network


instance Batchable LoggedBatch where
  runBatch (Candle driverQ) (LoggedBatch qs) = do
    mvar <- newEmptyMVar
    let q = encode (0::Int8) <> encode (fromIntegral (Data.List.length qs) :: Int16) <> mconcat qs <> encode SERIAL <> encode (0x00 :: Int8)
    atomically $ writeTBQueue driverQ (LongStr q, 13, mvar)
    res <- takeMVar mvar
    return $ fmap (\r -> case r of RRows rows -> rows) res


prepBatch :: Prepared -> [Put] -> LoggedBatch
prepBatch (Prepared pid) ks =
  let b = encode (1::Int8) <> encode pid <> encode (fromIntegral (Data.List.length ks) :: Int16) <> (mconcat $ fmap (addLength . runPut) ks) in
  LoggedBatch [b]


batch :: Q -> LoggedBatch
batch (Query (ShortStr q) bs) =
  let b = encode (0::Int8) <> encode (fromIntegral (DBL.length q) :: Int32) <> q <> encode (fromIntegral (Data.List.length bs) :: Int16) <> (mconcat $ fmap (DBL.fromStrict) bs) in
    LoggedBatch [b]
