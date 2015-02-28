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
                         , Config(..)
                         , KeyValue
                         ) where

import           Control.Concurrent
import           Control.Monad
import qualified Data.ByteString             as B
import qualified Data.HashTable.IO           as HT
import           Data.Serialize.Put
import           Database.KeyValue.Init
import           Database.KeyValue.Merge
import           Database.KeyValue.Parsing
import           Database.KeyValue.Timestamp
import           Database.KeyValue.Types
import           System.IO


-- | Insert a key value in the database
put :: MVar KeyValue -> Key -> Value -> IO ()
put m k v = do
    db <- takeMVar m
    offset <- hFileSize (currRecordHandle db)
    when (offset /= 0) $ hSeek (currRecordHandle db) SeekFromEnd 0
    t <- currentTimestamp
    B.hPut (currRecordHandle db) (runPut (deserializeData k v t))
    B.hPut (currHintHandle db) (runPut (deserializeHint k (offset + 1) t))
    HT.insert (offsetTable db) k (ValueLoc (offset + 1) (currRecordHandle db))
    putMVar m db
    return ()

-- | Get the value of the key
get :: MVar KeyValue -> Key -> IO (Maybe Value)
get m k = do
    db <- takeMVar m
    maybeRecordInfo <- HT.lookup (offsetTable db) k
    putMVar m db
    case maybeRecordInfo of
      Nothing -> return Nothing
      (Just recordInfo) -> if rOffset recordInfo == 0
        then
          return Nothing
        else
          do
            hSeek (rHandle recordInfo) AbsoluteSeek (rOffset recordInfo - 1)
            getValueFromHandle (rHandle recordInfo)

-- | Delete key from database
delete :: MVar KeyValue -> Key -> IO ()
delete m k = do
    db <- takeMVar m
    HT.delete (offsetTable db) k
    putMVar m db
    t <- currentTimestamp
    B.hPut (currHintHandle db) (runPut (deserializeHint k 0 t))
    return ()

-- | List all keys
listKeys :: MVar KeyValue -> IO [Key]
listKeys m = do
    db <- takeMVar m
    let table = offsetTable db
    putMVar m db
    fmap (map fst) $ HT.toList table

-- | Close the current instance of database
closeDB :: MVar KeyValue -> IO ()
closeDB m = do
    db <- takeMVar m
    hClose (currHintHandle db)
    hClose (currRecordHandle db)
    recordHandles <- fmap (map (rHandle . snd)) ((HT.toList . offsetTable) db)
    mapM_ hClose recordHandles
