{-|
  Module      : Database.KeyValue.Parsing
  Description : Parse data and hint logs
  Stability   : Experimental
-}

module Database.KeyValue.Parsing where

import           Control.Monad           (replicateM)
import qualified Data.ByteString         as B
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Types
import           System.IO

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

-- | Read header of value
parseHeader :: Get Header
parseHeader = do
  constIdx <- getWord32le
  size <- getWord32le
  fields <- getWord32le
  lengths <- replicateM (fromIntegral fields) getWord32le
  return (Header constIdx size fields lengths)

-- | Read one data log
parseDataLog :: Get DataLog
parseDataLog = do
  keySize <- getWord32le
  key <- getByteString (fromIntegral keySize)
  header <- parseHeader
  value <- getByteString (fromIntegral (valueSize header))
  timestamp <- getWord64le
  return (DataLog keySize key header value timestamp)

-- | Read all the data logs
parseDataLogs :: Get [DataLog]
parseDataLogs = do
  empty <- isEmpty
  if empty
     then return []
     else do dataLog <- parseDataLog
             dataLogs <- parseDataLogs
             return (dataLog:dataLogs)

-- | Read a value from the current handle
getValueFromHandle :: Handle -> IO (Maybe Value)
getValueFromHandle ht = do
  maybeKeysize <- fmap (runGet getWord32le) (B.hGet ht 4)
  case maybeKeysize of
       Left _ -> return Nothing
       Right ksz -> do
         hSeek ht RelativeSeek (fromIntegral ksz)
         hSeek ht RelativeSeek 4
         maybeValueSize <- fmap (runGet getWord32le) (B.hGet ht 4)
         case maybeValueSize of
              Left _ -> return Nothing
              Right vsz -> do
                maybeNumberOfFields <- fmap (runGet getWord32le) (B.hGet ht 4)
                case maybeNumberOfFields of
                     Left _ -> return Nothing
                     Right numFields -> do
                       hSeek ht RelativeSeek (4 * fromIntegral numFields)
                       fmap Just (B.hGet ht (fromIntegral vsz))

-- | Get the key offset pair from a hint log
getKeyOffsetPair :: HintLog -> (Key, Integer)
getKeyOffsetPair h = (hKey h, (fromIntegral . hOffset) h)

-- | Deserialize header
deserializeHeader :: Header -> Put
deserializeHeader h = do
  putWord32le (constructorIdx h)
  putWord32le (valueSize h)
  putWord32le (numberOfFields h)
  mapM_ putWord32le (fieldLengths h)

-- | Write a data log
deserializeData :: Key -> Header -> Value -> Timestamp -> Put
deserializeData k h v t = do
  putWord32le ((fromIntegral . B.length) k)
  putByteString k
  deserializeHeader h
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
