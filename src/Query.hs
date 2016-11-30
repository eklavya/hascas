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
import           Control.Monad.Except
import           Control.Monad.Reader
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
create s = Query (ShortStr $ DBL.fromStrict $ C8.pack "create " <> C8.pack s <> " ") []

drop' :: String -> Q
drop' s = Query (ShortStr $ DBL.fromStrict $ C8.pack "drop " <> C8.pack s <> " ") []

select :: String -> Q
select s = Query (ShortStr $ DBL.fromStrict $ C8.pack "select * from " <> C8.pack s <> " ") []

limit :: Int -> Q
limit i = Query (ShortStr $ " limit " <> DBL.fromStrict (C8.pack (show i))) []

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


-- | Prepare a query, returns a prepared query which can be fed to execCQL for execution.
prepare :: ByteString -> ExceptT ShortStr (ReaderT Candle IO) Prepared
prepare q = do
  (Candle driverQ) <- ask
  mvar <- liftIO newEmptyMVar
  let len = fromIntegral (C8.length q)::Int32
  let hd = encode len <> DBL.fromStrict q
  liftIO $ atomically $ writeTBQueue driverQ (LongStr hd, 9, mvar)
  res <- liftIO $ takeMVar mvar
  case res of
      Left e -> throwError e
      Right (RPrepared sb) -> return $ Prepared sb


-- | Run a query directly.
runCQL :: Consistency -> Q -> ExceptT ShortStr (ReaderT Candle IO) [Row]
runCQL c (Query (ShortStr q) bs) = do
  (Candle driverQ) <- ask
  mvar <- liftIO newEmptyMVar
  let flagAndNum = if Data.List.null bs then encode (0x00 :: Int8) else encode (0x01 :: Int8) <> encode (fromIntegral (Data.List.length bs) :: Int16)
  let q' = q <> encode c <> flagAndNum <> DBL.fromStrict (mconcat bs)
  let len = fromIntegral (DBL.length q)::Int32
  let hd = encode len <> q'
  liftIO $ atomically $ writeTBQueue driverQ (LongStr hd, 7, mvar)
  res <- liftIO $ takeMVar mvar
  case res of
    Left e -> throwError e
    Right (RRows rows) -> return rows


-- | Execute a prepared query.
execCQL :: Consistency -> Prepared -> [Put] -> ExceptT ShortStr (ReaderT Candle IO) [Row]
execCQL c (Prepared pid) ls = do
  (Candle driverQ) <- ask
  mvar <- liftIO newEmptyMVar
  let flag = if Data.List.null ls then 0x00 else 0x01
  let q = encode pid <> (encode c <> encode (flag :: Int8)) <> encode (fromIntegral (Data.List.length ls) :: Int16) <> mconcat (fmap (addLength . runPut) ls)
  liftIO $ atomically $ writeTBQueue driverQ (LongStr q, 10, mvar)
  res <- liftIO $ takeMVar mvar
  case res of
    Left e -> throwError e
    Right (RRows rows) -> return rows


-- | Combine DSL actions
-- @
--  select "demodb.emp" # where' "empID" (104::Int64) # and' "deptID" (15::Int32)
-- @
(#) :: Q -> Q -> Q
(#) (Query s1 bs1) (Query s2 bs2) = Query (s1 <> s2) (bs1 <> bs2)

infixl 7 #
