-- Simple key value store

module KeyValue (
  get,
  put,
  delete,
  initDB) where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashTable.IO    as HT
import           Data.Word
import           System.Directory
import           System.IO

type Key   = B.ByteString
type Value = B.ByteString

-- Store offset of key
data RecordMap = RecordMap (HT.BasicHashTable Key Integer)

-- Hint Log format
data HintLog = HintLog { hKeySize :: Word32
                       , hKey     :: Key
                       , hOffset   :: Word64
                       }

-- Data Log format
data DataLog = DataLog { dKeySize   :: Word32
                       , dKey       :: Key
                       , dValueSize :: Word32
                       , dValue     :: Value
                       }

-- Name of the file containing records
recordsFileName = "records.dat"

-- Name of the file containing offsets
hintFileName = "hint.dat"

parseHintLog :: Get HintLog
parseHintLog = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  offset <- getWord64le
  return (HintLog keySize key offset)

parseHintLogs :: Get [HintLog]
parseHintLogs = do
  empty <- isEmpty
  if empty
     then return []
     else do hintlog <- parseHintLog
             hintlogs <- parseHintLogs
             return (hintlog:hintlogs)

parseDataLog :: Get DataLog
parseDataLog = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  valueSize <- getWord32le
  value <- getByteString (fromIntegral valueSize)
  return (DataLog keySize key valueSize value)

getKeyOffsetPair :: HintLog -> (Key, Integer)
getKeyOffsetPair h = (hKey h, (fromIntegral . hOffset) h)

-- Initialize the database
initDB :: IO RecordMap
initDB = do
    -- Create the records file if not exists
    check <- doesFileExist recordsFileName
    if check
      then
        do
          allcontent <- BL.readFile hintFileName
          m <- HT.fromList (map getKeyOffsetPair (runGet parseHintLogs allcontent))
          return (RecordMap m)
      else
        do
          writeFile recordsFileName ""
          writeFile hintFileName ""
          m <- HT.new
          return (RecordMap m)

deserializeData :: Key -> Value -> Put
deserializeData k v = do
  _ <- putWord32le ((fromIntegral . B.length) k)
  _ <- putByteString k
  _ <- putWord32le ((fromIntegral . B.length) v)
  _ <- putByteString v
  return ()

deserializeHint :: Key -> Integer -> Put
deserializeHint k o = do
  _ <- putWord32le ((fromIntegral . B.length) k)
  _ <- putByteString k
  _ <- putWord64le (fromIntegral o)
  return ()

-- Insert a key value in the database
put :: RecordMap -> Key -> Value -> IO ()
put (RecordMap m) k v = do
    ht <- openFile recordsFileName AppendMode
    offset <- hFileSize ht
    BL.hPutStr ht (runPut (deserializeData k v))
    hClose ht
    BL.appendFile hintFileName (runPut (deserializeHint k (offset + 1)))
    HT.insert m k (offset + 1)
    return ()

-- Get the value of the key
get :: RecordMap -> Key -> IO (Maybe Value)
get (RecordMap m) k = do
    maybeOffset <- HT.lookup m k
    case maybeOffset of
      Nothing -> return Nothing
      (Just offset) -> if offset == 0
        then
          return Nothing
        else
          do
            ht <- openFile recordsFileName ReadMode
            hSeek ht AbsoluteSeek (offset - 1)
            l <- BL.hGetContents ht
            return (Just (dValue (runGet parseDataLog l)))

-- Delete key from database
delete :: RecordMap -> Key -> IO ()
delete (RecordMap m) k = do
    HT.delete m k
    BL.appendFile hintFileName (runPut (deserializeHint k 0))
    return ()
