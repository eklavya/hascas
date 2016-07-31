{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Driver (Driver.init) where


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

import           Codec
import           Common
import           Data.Maybe
import           Data.Monoid                    as DM
import           Data.UUID
import           Debug.Trace
import           Encoding
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (Handle, hClose, hFlush)
import           Network


startUp :: Handle -> IO ()
startUp h = do
  let s = encode $ StringMap (DMS.fromList [(ShortStr "CQL_VERSION", ShortStr "3.0.0")])
  let startup = DBL.toStrict $ DBL.pack [4, 0, 0, 1, 1] <> encode (fromIntegral (DBL.length s)::Int32) <> s
  hPut h startup
  header <- hGet h 9
  he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
  hGet h (fromIntegral $ len he)
  return ()


receiveThread :: Handle -> MVar [Int16] -> MVar(IM.IntMap (MVar (Either ShortStr Result))) -> IO ()
receiveThread h streams streamMap = do
  forkIO $ forever $ do
        header <- hGet h 9
        he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
        p <- hGet h (fromIntegral $ len he)
        if opcode he == 0
          then do
            let (erCode, erMsg) = runGet getErr (DBL.fromStrict p)
            mvar <- getResHolder he streams streamMap
            putMVar mvar (Left erMsg)
          else do
            let resultType = runGet (get :: Get Int32) (DBL.fromStrict p)

            case resultType of
              1 -> do
                mvar <- getResHolder he streams streamMap
                putMVar mvar (Right $ RRows [])

              2 -> do
                let rows = content $ runGet getRows (DBL.fromStrict $ C8.drop 4 p)
                mvar <- getResHolder he streams streamMap
                putMVar mvar (Right $ RRows rows)

              4 -> do
                let prep = runGet (get :: Get ShortBytes) (DBL.fromStrict $ C8.drop 4 p)
                mvar <- getResHolder he streams streamMap
                putMVar mvar (Right $ RPrepared prep)

              5 -> do
                mvar <- getResHolder he streams streamMap
                putMVar mvar (Right $ RRows [])
  return ()


getResHolder :: Header -> MVar [Int16] -> MVar(IM.IntMap (MVar (Either ShortStr Result))) -> IO (MVar (Either ShortStr Result))
getResHolder he1 streams streamMap = do
          m <- takeMVar streamMap
          let strm = stream he1
          let fm = IM.lookup (fromIntegral strm :: Int) m
          let mvar = fromJust fm
          putMVar streamMap (IM.delete (fromIntegral strm :: Int) m)
          strs <- takeMVar streams
          putMVar streams (strs ++ [strm])
          return mvar


sendThread :: Handle -> MVar [Int16] -> MVar(IM.IntMap (MVar (Either ShortStr Result))) -> TBQueue (LongStr, Word8, MVar (Either ShortStr Result)) -> IO ()
sendThread h streams streamMap driverQ = do
  forkIO $ forever $ do
          strs <- takeMVar streams
          case Data.List.uncons strs of
            Just (i, strsTail) -> do
              putMVar streams strsTail
              (bs, opc, mvar) <- atomically $ readTBQueue driverQ
              m <- takeMVar streamMap
              let m' = IM.insert (fromIntegral i :: Int) mvar m
              putMVar streamMap m'
              seq m' (return ())
              let bs' = Data.ByteString.pack [4, 0] <> DBL.toStrict (encode i <> DBL.pack [opc] <> encode bs)
              hPut h bs'
              -- atomically $ writeTBQueue channelQ bs'
            Nothing -> do
              putMVar streams strs
              sendThread h streams streamMap driverQ
  return ()


init :: HostName -> PortID -> IO Candle
init host port = do
    streamNum <- newMVar 0
    streams <- newMVar ([0..32767] :: [Int16])
    streamMap <- newMVar (IM.empty :: (IM.IntMap (MVar (Either ShortStr Result))))
    driverQ <- atomically $ newTBQueue 32768

    h <- connectTo host port
    startUp h
    receiveThread h streams streamMap
    sendThread h streams streamMap driverQ
    return $ Candle driverQ
