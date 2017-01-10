{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Driver (Driver.init, Driver.allConnections) where


import           Codec
import           Common
import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Concurrent.STM.TBQueue
import           Control.Exception.Safe         (bracket, catchAny, mask)
import           Control.Monad                  (forM_, forever, replicateM)
import           Control.Monad.Except
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.IEEE754
import           Data.Binary.Put
import           Data.Bits
import           Data.ByteString
import qualified Data.ByteString.Char8          as C8
import qualified Data.ByteString.Lazy           as DBL
import           Data.Either
import           Data.Int
import qualified Data.IntMap                    as IM
import           Data.List
import qualified Data.Map.Strict                as DMS
import           Data.Maybe
import           Data.Monoid                    as DM
import           Data.Set
import           Data.Traversable
import           Data.UUID
import           Debug.Trace
import           Encoding
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (Handle, hClose, hFlush)
import           Network
import           System.IO.Unsafe


startUp :: Handle -> IO ()
startUp h = do
  let s = encode $ StringMap (DMS.fromList [(ShortStr "CQL_VERSION", ShortStr "3.0.0")])
  let startup = DBL.toStrict $ DBL.pack [4, 0, 0, 1, 1] <> encode (fromIntegral (DBL.length s)::Int32) <> s
  hPut h startup
  header <- hGet h 9
  he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
  hGet h (fromIntegral $ len he)
  return ()


registerForEvents :: Handle -> IO ()
registerForEvents h = do
  let s = encode $ CQLList [CQLString "TOPOLOGY_CHANGE", CQLString "STATUS_CHANGE"]
  let register = DBL.toStrict $ DBL.pack [4, 0, 0, 1, 11] <> encode (fromIntegral (DBL.length s)::Int32) <> s
  hPut h register
  header <- hGet h 9
  he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
  hGet h (fromIntegral $ len he)
  return ()


{-# NOINLINE prepareQueries #-}
prepareQueries :: MVar (Set LongStr)
prepareQueries = unsafePerformIO $ do
  mvar <- newEmptyMVar
  putMVar mvar Data.Set.empty
  return mvar


receiveThread :: HostName -> PortID -> Handle -> MVar [Int16] -> MVar(IM.IntMap (MVar (Either ShortStr Result))) -> IO ()
receiveThread host port h streams streamMap = do
  forkIO $ catchAny (forever $ do
                                header <- hGet h 9
                                he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
                                p <- hGet h (fromIntegral $ len he)
                                case opcode he of
                                  0 -> do
                                    let (erCode, erMsg) = runGet getErr (DBL.fromStrict p)
                                    mvar <- getResHolder he streams streamMap
                                    putMVar mvar (Left erMsg)
                                  8 -> do
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
                                        putMVar mvar (Right $ RRows [])) (\e -> do
                                                                                  print "error in receiving"
                                                                                  sm <- takeMVar streamMap
                                                                                  nm <- sequence $ fmap (\mvar -> tryPutMVar mvar (Left $ ShortStr $ DBL.fromStrict $ C8.pack $ "connection to node " <> show host <> " was lost")) sm
                                                                                  putMVar streamMap sm)

          -- 12 -> do
          --   print "something happened"
          --   print $ runGet (get :: Get ShortStr) (DBL.fromStrict p)
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


writeQuery :: Handle -> MVar [Int16] -> MVar (IM.IntMap (MVar (Either ShortStr Result))) -> (LongStr, Word8, MVar (Either ShortStr Result)) -> IO ()
writeQuery h streams streamMap (bs, opc, mvar) = do
      strs <- takeMVar streams
      case Data.List.uncons strs of
        Just (i, strsTail) -> do
          putMVar streams strsTail
          m <- takeMVar streamMap
          let m' = IM.insert (fromIntegral i :: Int) mvar m
          putMVar streamMap m'
          seq m' (return ())
          let bs' = Data.ByteString.pack [4, 0] <> DBL.toStrict (encode i <> DBL.pack [opc] <> encode bs)
          hPut h bs'

        Nothing -> do
          putMVar streams strs
          writeQuery h streams streamMap (bs, opc, mvar)


sendThread :: HostName -> PortID -> Handle -> MVar [Int16] -> MVar(IM.IntMap (MVar (Either ShortStr Result))) -> TBQueue (LongStr, Word8, MVar (Either ShortStr Result)) -> IO ()
sendThread host port h streams streamMap driverQ = do
  forkIO $ catchAny (forever $ do
          (bs, opc, mvar) <- atomically $ readTBQueue driverQ
          when (opc == 9) $ do
             pq <- takeMVar prepareQueries
             putMVar prepareQueries (Data.Set.insert bs pq)
             cons <- readMVar allConnections
             mvars <- forM cons $ \(_, _, h', str, strMap) -> do
               mv <- newEmptyMVar
               writeQuery h' str strMap (bs, opc, mv)
               return mv
             forkIO $ do
               ls <- sequence $ fmap (\mva -> do {a <- takeMVar mva; return a}) mvars
               putMVar mvar $ Data.List.foldl' (\b a -> if isLeft a then a else b) (Data.List.head ls) (Data.List.tail ls)
             return ()
          when (opc /= 9) $ writeQuery h streams streamMap (bs, opc, mvar)) (\e -> do
                                                                                  sm <- takeMVar streamMap
                                                                                  nm <- sequence $ fmap (\mvar -> tryPutMVar mvar (Left $ ShortStr $ DBL.fromStrict $ C8.pack $ "connection to node " <> show host <> " was lost")) sm
                                                                                  putMVar streamMap sm
                                                                                  (host, port, h, streams, streamMap) <- setupNode host port
                                                                                  sendThread host port h streams streamMap driverQ
                                                                                  receiveThread host port h streams streamMap)
  return ()


getPeers :: Handle -> IO (Either ShortStr [Row])
getPeers h = do
  let q' = "select peer from system.peers"
  let q = q' <> encode LOCAL_ONE <> encode (0x00 :: Int8)
  let l = fromIntegral (DBL.length q')::Int32
  let hd = LongStr $ encode l <> q
  let bs' = Data.ByteString.pack [4, 0] <> DBL.toStrict (encode (0::Int16) <> DBL.pack [7] <> encode hd)
  hPut h bs'
  header <- hGet h 9
  he <- pure $ runGet (get :: Get Header) $ DBL.fromStrict header
  p <- hGet h (fromIntegral $ len he)
  if opcode he == 0
    then do
      let (erCode, erMsg) = runGet getErr (DBL.fromStrict p)
      return $ Left erMsg
    else do
      let resultType = runGet (get :: Get Int32) (DBL.fromStrict p)
      case resultType of
        2 -> do
          let rows = content $ runGet getRows (DBL.fromStrict $ C8.drop 4 p)
          return $ Right rows
        _ ->
          return $ Left $ ShortStr "Could not get list of peers. Please check if this cassandra version is supported."


{-# NOINLINE allConnections #-}
allConnections :: MVar [(HostName, PortID, Handle, MVar [Int16], MVar(IM.IntMap (MVar (Either ShortStr Result))))]
allConnections = unsafePerformIO $ newMVar []


setupNode peer port = catchAny (do
  streams <- newMVar ([0..32767] :: [Int16])
  streamMap <- newMVar (IM.empty :: (IM.IntMap (MVar (Either ShortStr Result))))
  h <- connectTo peer port
  startUp h
  return (peer, port, h, streams, streamMap)) (\e -> do
                                                  threadDelay 1000000
                                                  setupNode peer port)


-- | The first function you need to call. It initializes the driver and connects to the cluster.
-- You only need to specify one node from your cluster here.
init :: HostName -> PortID -> ExceptT ShortStr IO Candle
init host port = do
    streamNum <- liftIO $ newMVar 0
    streams <- liftIO $ newMVar ([0..32767] :: [Int16])
    streamMap <- liftIO $ newMVar (IM.empty :: (IM.IntMap (MVar (Either ShortStr Result))))
    driverQ <- liftIO $ atomically $ newTBQueue 32768

    h <- liftIO $ connectTo host port
    liftIO $ startUp h
    -- liftIO $ registerForEvents h

    res <- liftIO $ getPeers h
    case res of
      Left err -> throwError err
      Right rows -> do
        let peers = (\(CQLString p) -> Data.List.concat <$> Data.List.intersperse "." $ fmap show (unpack p)) <$> catMaybes (fmap (\r -> fromCQL r (CQLString "peer")::Maybe CQLString) rows)
        oCons <- liftIO $ sequence $ fmap (\peer -> setupNode peer port) peers
        let connections = (host, port, h, streams, streamMap) : oCons
        allCon <- liftIO $ takeMVar allConnections
        liftIO $ putMVar allConnections connections
        forM_ connections (\(host, port, h, streams, streamMap) -> do
          liftIO $ receiveThread host port h streams streamMap
          liftIO $ sendThread host port h streams streamMap driverQ)
        return $ Candle driverQ
