{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Query where


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
import           Driver
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network


create :: String -> Q
create s = Query (ShortStr $ (DBL.fromStrict (C8.pack "create ")) <> (DBL.fromStrict $ C8.pack s <> " ")) []

drop' :: String -> Q
drop' s = Query (ShortStr $ (DBL.fromStrict (C8.pack "drop ")) <> (DBL.fromStrict $ C8.pack s <> " ")) []

select :: String -> Q
select s = Query (ShortStr $ (DBL.fromStrict (C8.pack "select * from ")) <> (DBL.fromStrict $ C8.pack s <> " ")) []

limit :: Int -> Q
limit i = Query (ShortStr $ " limit " <> (DBL.fromStrict (C8.pack (show i)))) []

update :: String -> Q
update s = Query (ShortStr $ "update " <> DBL.fromStrict (C8.pack s) <> " set ") []

with :: (Binary k) =>  String -> k -> Q
with n v = Query (ShortStr (DBL.fromStrict (C8.pack n) <> " = ? ")) [(DBL.toStrict . addLength . runPut . put) v]

delete :: String -> Q
delete s = Query (ShortStr $ "delete from " <> DBL.fromStrict (C8.pack s) <> " ") []

where' :: (Binary k) => String -> k -> Q
where' n v = Query (ShortStr (" where " <> DBL.fromStrict (C8.pack n) <> " = ? ")) [(DBL.toStrict . addLength . runPut . put) v]

and' :: (Binary k) =>  String -> k -> Q
and' n v = Query (ShortStr (" and " <> DBL.fromStrict (C8.pack n) <> " = ? ")) [(DBL.toStrict . addLength . runPut . put) v]


prepare :: Candle -> ByteString -> IO (Either ShortStr Prepared)
prepare (Candle driverQ) q = do
  mvar <- newEmptyMVar
  let len = fromIntegral (C8.length q)::Int32
  let hd = encode len <> DBL.fromStrict q
  atomically $ writeTBQueue driverQ (LongStr hd, 9, mvar)
  res <- takeMVar mvar
  return $ fmap (\r -> case r of RPrepared sb -> Prepared sb) res


runCQL :: Candle -> Consistency -> Q -> IO (Either ShortStr [Row])
runCQL (Candle driverQ) c (Query (ShortStr q) bs) = do
  mvar <- newEmptyMVar
  let flagAndNum = if Data.List.null bs then encode (0x00 :: Int8) else encode (0x01 :: Int8) <> encode (fromIntegral (Data.List.length bs) :: Int16)
  let q' = q <> encode c <> flagAndNum <> (DBL.fromStrict $ mconcat bs)
  let len = fromIntegral (DBL.length q)::Int32
  let hd = encode len <> q'
  atomically $ writeTBQueue driverQ (LongStr hd, 7, mvar)
  res <- takeMVar mvar
  return $ fmap (\r -> case r of RRows rows -> rows) res


execCQL :: Candle -> Consistency -> Prepared -> [Put] -> IO (Either ShortStr [Row])
execCQL (Candle driverQ) c (Prepared pid) ls = do
  mvar <- newEmptyMVar
  let flag = if Data.List.null ls then 0x00 else 0x01
  let q = encode pid <> (encode c <> encode (flag :: Int8)) <> encode (fromIntegral (Data.List.length ls) :: Int16) <> (mconcat $ fmap (addLength . runPut) ls)
  atomically $ writeTBQueue driverQ (LongStr q, 10, mvar)
  res <- takeMVar mvar
  return $ fmap (\r -> case r of RRows rows -> rows) res


(#) :: Q -> Q -> Q
(#) (Query s1 bs1) (Query s2 bs2) = Query (s1 <> s2) (bs1 <> bs2)

infixl 7 #
