{-|
  Module      : Database.KeyValue.Parsing
  Description : Parse data and hint logs
  Stability   : Experimental
-}

module Database.KeyValue.Parsing where

import qualified Data.ByteString         as B
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Types

-- | Read one hint log
parseHintLog :: Get HintLog
parseHintLog = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  offset <- getWord64le
  timestamp <- getWord64le
  return (HintLog keySize key offset timestamp)

-- | Read all the hint logs
parseHintLogs :: Get [HintLog]
parseHintLogs = do
  empty <- isEmpty
  if empty
     then return []
     else do hintlog <- parseHintLog
             hintlogs <- parseHintLogs
             return (hintlog:hintlogs)

-- | Read one data log
parseDataLog :: Get DataLog
parseDataLog = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  valueSize <- getWord32le
  value <- getByteString (fromIntegral valueSize)
  timestamp <- getWord64le
  return (DataLog keySize key valueSize value timestamp)

-- | Read all the data logs
parseDataLogs :: Get [DataLog]
parseDataLogs = do
  empty <- isEmpty
  if empty
     then return []
     else do dataLog <- parseDataLog
             dataLogs <- parseDataLogs
             return (dataLog:dataLogs)

-- | Get the key offset pair from a hint log
getKeyOffsetPair :: HintLog -> (Key, Integer)
getKeyOffsetPair h = (hKey h, (fromIntegral . hOffset) h)

-- | Write a data log
deserializeData :: Key -> Value -> Timestamp -> Put
deserializeData k v t = do
  putWord32le ((fromIntegral . B.length) k)
  putByteString k
  putWord32le ((fromIntegral . B.length) v)
  putByteString v
  putWord64le (fromIntegral t)
  return ()

-- | Write a hint log
deserializeHint :: Key -> Integer -> Timestamp -> Put
deserializeHint k o t = do
  putWord32le ((fromIntegral . B.length) k)
  putByteString k
  putWord64le (fromIntegral o)
  putWord64le (fromIntegral t)
  return ()
