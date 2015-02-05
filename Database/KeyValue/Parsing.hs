module Database.KeyValue.Parsing where

import qualified Data.ByteString         as B
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Types

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

parseDataLogs :: Get [DataLog]
parseDataLogs = do
  empty <- isEmpty
  if empty
     then return []
     else do dataLog <- parseDataLog
             dataLogs <- parseDataLogs
             return (dataLog:dataLogs)

getKeyOffsetPair :: HintLog -> (Key, Integer)
getKeyOffsetPair h = (hKey h, (fromIntegral . hOffset) h)

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
