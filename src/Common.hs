{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Common where


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

import           Data.Maybe
import           Data.Monoid                    as DM
import qualified Data.Set                       as Set
import           Data.UUID
import           Debug.Trace
import           GHC.Generics                   (Generic)
import           GHC.IO.Handle                  (hClose, hFlush)
import           Network
import           Text.Printf                    (printf)




class Batchable a where
  runBatch :: a -> ExceptT ShortStr (ReaderT Candle IO) [Row]

newtype BatchQuery = BatchQuery DBL.ByteString

data LoggedBatch = LoggedBatch [DBL.ByteString]

instance Monoid LoggedBatch where
  mempty = LoggedBatch []
  mappend (LoggedBatch l1) (LoggedBatch l2) = LoggedBatch (l1 <> l2)

data Candle = Candle (TBQueue (LongStr, Word8, MVar (Either ShortStr Result)))

data Header = Header {
  protocolVersion :: Int8,
  flags           :: Int8,
  stream          :: Int16,
  opcode          :: Int8,
  len             :: Int32
}
  deriving(Eq, Show, Generic)

instance Binary Header

newtype ShortStr = ShortStr DBL.ByteString
  deriving(Eq, Ord, Show)

newtype LongStr = LongStr DBL.ByteString
  deriving(Show)

newtype Bytes = Bytes DBL.ByteString
  deriving(Show, Eq, Ord)

newtype ShortBytes = ShortBytes ByteString
  deriving(Show)

data Consistency = ANY | ONE | TWO | THREE | QUORUM | ALL | LOCAL_QUORUM | EACH_QUORUM
                  | SERIAL | LOCAL_SERIAL | LOCAL_ONE
  deriving(Eq, Generic)

newtype StringMap = StringMap (DMS.Map ShortStr ShortStr)

data OptionId =  Ascii
            | Bigint
            | Blob
            | Boolean
            | Counter
            | Decimal
            | Double
            | Float
            | Int
            | Timestamp
            | Uuid
            | Varchar
            | Varint
            | Timeuuid
            | Inet
            | Date
            | Time
            | Smallint
            | Tinyint

data GlobalTableSpec = GlobalTableSpec {
  ksName :: ShortStr,
  tName  :: ShortStr
}
  deriving(Show, Generic)

instance Binary GlobalTableSpec

data Metadata = Metadata {
  mflags   :: Int32,
  numCols  :: Int32,
  gts      :: Maybe GlobalTableSpec,
  colSpecs :: [ColumnSpec]
}
  deriving(Show)

data ColumnSpec = ColumnSpec {
  ksname  :: Maybe ShortStr,
  tname   :: Maybe ShortStr,
  colName :: ShortStr,
  tpe     :: Word16,
  elTpe   :: Maybe Word16,
  elTpeV  :: Maybe Word16
}
  deriving(Show)

data RowContent = RowContent {
  values :: [Bytes]
}


data Rows = Rows {
  content :: [Row]
}
  deriving(Show)

type Row = DMS.Map CQLString (Word16, Maybe Word16, Maybe Word16, Bytes)


class BuildRec a where
  fromRow :: Row -> Maybe a

class FromCQL a where
  fromCQL :: Row -> CQLString -> Maybe a

instance Monoid ShortStr where
  mempty = ShortStr ""
  mappend (ShortStr s1) (ShortStr s2) = ShortStr (s1 <> s2)

data Prepared = Prepared ShortBytes
  deriving(Show)

data Result = RRows [Row] | RPrepared ShortBytes
  deriving(Show)


data Query a b where
  Query :: ShortStr -> [ByteString] -> Query ShortStr [ByteString]

type Q = Query ShortStr [ByteString]

addLength :: DBL.ByteString -> DBL.ByteString
addLength bs = encode (fromIntegral (DBL.length bs) :: Int32) <> bs

getErr :: Get (Int32, ShortStr)
getErr = do
           erc <- get :: Get Int32
           erm <- get :: Get ShortStr
           return (erc, erm)


instance Binary ShortStr where
  put (ShortStr s) =
    let len = fromIntegral (DBL.length s) :: Int16 in
      do
        put (len :: Int16)
        forM_ (DBL.unpack s) (\c -> put (c :: Word8))

  get = do
    len <- get :: Get Int16
    bs <- replicateM (fromIntegral len ::Int) getWord8
    return $ ShortStr $ DBL.pack bs



instance Binary LongStr where
  put (LongStr s) =
    let len = fromIntegral (DBL.length s) :: Int32 in
      do
        put (len :: Int32)
        forM_ (DBL.unpack s) (\c -> put (c :: Word8))

  get = do
    len <- get :: Get Int32
    bs <- replicateM (fromIntegral len ::Int) getWord8
    return $ LongStr $ DBL.pack bs



instance Binary Bytes where
  put (Bytes bs) =
    let len = fromIntegral (DBL.length bs) :: Int32 in
      do
        put (len :: Int32)
        forM_ (DBL.unpack bs) (\c -> put (c :: Word8))

  get = do
      len <- get :: Get Int32
      bs <- replicateM (fromIntegral len ::Int) getWord8
      return $ Bytes $ DBL.pack bs



instance Binary ShortBytes where
  put (ShortBytes s) =
    let len = fromIntegral (C8.length s) :: Int16 in
      do
        put (len :: Int16)
        forM_ (unpack s) (\c -> put (c :: Word8))

  get = do
    len <- get :: Get Int16
    bs <- replicateM (fromIntegral len ::Int) getWord8
    return $ ShortBytes $ pack bs


conToWord :: Consistency -> Word16
conToWord c
  | c == ANY = 0x0000
  | c == ONE = 0x0001
  | c == TWO = 0x0002
  | c == THREE = 0x0003
  | c == QUORUM = 0x0004
  | c == ALL = 0x0005
  | c == LOCAL_QUORUM = 0x0006
  | c == EACH_QUORUM = 0x0007
  | c == SERIAL = 0x0008
  | c == LOCAL_SERIAL = 0x0009
  | c == LOCAL_ONE = 0x000A

wordToCon :: Word16 -> Consistency
wordToCon w
  | w == 0x0000 = ANY
  | w == 0x0001 = ONE
  | w == 0x0002 = TWO
  | w == 0x0003 = THREE
  | w == 0x0004 = QUORUM
  | w == 0x0005 = ALL
  | w == 0x0006 = LOCAL_QUORUM
  | w == 0x0007 = EACH_QUORUM
  | w == 0x0008 = SERIAL
  | w == 0x0009 = LOCAL_SERIAL
  | w == 0x000A = LOCAL_ONE

instance Binary Consistency where
  get = do
    c <- get :: Get Word16
    return $ wordToCon c

  put c = put $ conToWord c

newtype CQLDouble = CQLDouble Double
  deriving(Show, Eq, Ord, Generic)

newtype CQLString = CQLString ByteString
  deriving(Show, Eq, Ord, Generic)

newtype CQLMap k v = CQLMap (DMS.Map k v)
  deriving(Show, Eq, Ord, Generic)

newtype CQLSet el = CQLSet (Set.Set el)
  deriving(Show, Eq, Ord, Generic)

newtype CQLList el = CQLList [el]
  deriving(Show, Eq, Ord, Generic)
