{-|
  Module      : Database.KeyValue
  Description : A simple key value store inspired from Bitcask
  Stability   : Experimental

  KeyValue is a simple key value store inspired from Bitcask.
  The design is based on log-structured file systems.
-}

module Database.KeyValue ( get
                         , put
                         , delete
                         , initDB
                         , listKeys
                         , closeDB
                         , mergeDataLogs
                         , Config
                         ) where

import qualified Data.ByteString                 as B
import qualified Data.ByteString.Lazy            as BL
import qualified Data.HashTable.IO               as HT
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Init
import           Database.KeyValue.Merge
import           Database.KeyValue.Parsing
import           Database.KeyValue.Timestamp
import           Database.KeyValue.Types
import           System.IO


-- | Insert a key value in the database
put :: KeyValue -> Key -> Value -> IO ()
put db k v = do
    offset <- hFileSize (currRecordHandle db)
    t <- currentTimestamp
    B.hPut (currRecordHandle db) (runPut (deserializeData k v t))
    B.hPut (currHintHandle db) (runPut (deserializeHint k (offset + 1) t))
    HT.insert (offsetTable db) k (ValueLoc (offset + 1) (currRecordHandle db))
    return ()

-- | Get the value of the key
get :: KeyValue -> Key -> IO (Maybe Value)
get db k = do
    maybeRecordInfo <- HT.lookup (offsetTable db) k
    case maybeRecordInfo of
      Nothing -> return Nothing
      (Just recordInfo) -> if rOffset recordInfo == 0
        then
          return Nothing
        else
          do
            hSeek (rHandle recordInfo) AbsoluteSeek (rOffset recordInfo - 1)
            l <- BL.hGetContents (rHandle recordInfo)
            case runGetLazy parseDataLog l of
              Left _ -> return Nothing
              Right v -> return (Just (dValue v))

-- | Delete key from database
delete :: KeyValue -> Key -> IO ()
delete db k = do
    HT.delete (offsetTable db) k
    t <- currentTimestamp
    B.hPut (currHintHandle db) (runPut (deserializeHint k 0 t))
    return ()

-- | List all keys
listKeys :: KeyValue -> IO [Key]
listKeys db = fmap (map fst) $ (HT.toList . offsetTable) db

-- | Close the current instance of database
closeDB :: KeyValue -> IO ()
closeDB db = do
    hClose (currHintHandle db)
    hClose (currRecordHandle db)
    recordHandles <- fmap (map (rHandle . snd)) ((HT.toList . offsetTable) db)
    mapM_ hClose recordHandles
