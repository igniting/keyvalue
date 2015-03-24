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
put :: KeyValue -> Key -> Header -> Value -> IO ()
put db k h v = do
    offset <- hFileSize (currRecordHandle db)
    when (offset /= 0) $ hSeek (currRecordHandle db) SeekFromEnd 0
    t <- currentTimestamp
    B.hPut (currRecordHandle db) (runPut (deserializeData k h v t))
    B.hPut (currHintHandle db) (runPut (deserializeHint k (offset + 1) t))
    table <- takeMVar (offsetTable db)
    HT.insert table k (ValueLoc (offset + 1) (currRecordHandle db))
    putMVar (offsetTable db) table
    return ()

-- | Get the value of the key
get :: KeyValue -> Key -> IO (Maybe Value)
get db k = do
    table <- takeMVar (offsetTable db)
    putMVar (offsetTable db) table
    maybeRecordInfo <- HT.lookup table k
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
delete :: KeyValue -> Key -> IO ()
delete db k = do
    table <- takeMVar (offsetTable db)
    HT.delete table k
    putMVar (offsetTable db) table
    t <- currentTimestamp
    B.hPut (currHintHandle db) (runPut (deserializeHint k 0 t))
    return ()

-- | List all keys
listKeys :: KeyValue -> IO [Key]
listKeys db = do
    table <- takeMVar (offsetTable db)
    putMVar (offsetTable db) table
    fmap (map fst) $ HT.toList table

-- | Close the current instance of database
closeDB :: KeyValue -> IO ()
closeDB db = do
    hClose (currHintHandle db)
    hClose (currRecordHandle db)
    table <- takeMVar (offsetTable db)
    putMVar (offsetTable db) table
    recordHandles <- fmap (map (rHandle . snd)) (HT.toList table)
    mapM_ hClose recordHandles
