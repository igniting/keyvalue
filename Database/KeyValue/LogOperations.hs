{-|
  Module      : Database.KeyValue.LogOperations
  Description : Operations on data and hint logs
  Stability   : Experimental
-}

module Database.KeyValue.LogOperations( hintExt
                                      , recordExt
                                      , getKeyAndValueLocs
                                      , addNewRecord
                                      , getKeysFromHintFiles
                                      , putKeysFromRecordFiles
                                      ) where

import           Control.Monad
import qualified Data.ByteString           as B
import qualified Data.HashTable.IO         as HT
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Parsing
import           Database.KeyValue.Types
import           System.FilePath.Posix
import           System.IO
import           System.IO.Temp

-- | Extension of hint logs
hintExt :: String
hintExt = ".hkv"

-- | Extension of data logs
recordExt :: String
recordExt = ".rkv"

-- | Get key and value locations from hint files
getKeyAndValueLocs :: [FilePath] -> IO [(Key, ValueLoc)]
getKeyAndValueLocs hintFiles = do
  l <- mapM getKeyAndValueLoc hintFiles
  return (concat l)

-- | Get key and value locations from a single hint file
getKeyAndValueLoc :: FilePath -> IO [(Key, ValueLoc)]
getKeyAndValueLoc hintFile = do
  recordHandle <- getRecordHandle hintFile
  c <- B.readFile hintFile
  case runGet parseHintLogs c of
    Left _ -> error "Error occured in parsing hint file."
    Right hintlogs -> return (map ((\(k, o) -> (k, ValueLoc o recordHandle)) . getKeyOffsetPair) hintlogs)

-- | Get the handle of the data log corresponding to a hint log
getRecordHandle :: FilePath -> IO Handle
getRecordHandle hintFile = do
  ht <- openBinaryFile (replaceExtension hintFile recordExt) ReadMode
  hSetBuffering ht NoBuffering
  return ht

-- | Open a new data and the corresponding hint log
addNewRecord :: FilePath -> IO (Handle, Handle)
addNewRecord dir = do
  (hintFile, hintHandle) <- openNewBinaryFile dir hintExt
  recordHandle <- openBinaryFile (replaceExtension hintFile recordExt) ReadWriteMode
  hSetBuffering hintHandle NoBuffering
  hSetBuffering recordHandle NoBuffering
  return (hintHandle, recordHandle)

-- | Get all the keys from the hint files
-- In case of conflict, keep the copy with the latest timestamp
getKeysFromHintFiles :: [FilePath] -> IO KeysTable
getKeysFromHintFiles hintFiles = do
  table <- HT.new
  allKeysList <- fmap concat (mapM getKeysFromHintFile hintFiles)
  mapM_ (checkAndInsert table) allKeysList
  return table

-- | Get all keys and timestamp from a hint file
getKeysFromHintFile :: FilePath -> IO [(Key, KeyUpdateInfo)]
getKeysFromHintFile file = do
  -- TODO: Make this lazy
  c <- B.readFile file
  case runGet parseHintLogs c of
    Left _ -> error "Error occured parsing hint log."
    Right hintLogs -> return (map getKeyUpdateInfo hintLogs) where
      getKeyUpdateInfo :: HintLog -> (Key, KeyUpdateInfo)
      getKeyUpdateInfo l = (hKey l, (hTimestamp l, hOffset l == 0))

-- | Insert the key in hash map after checking for conflicts
checkAndInsert :: KeysTable -> (Key, KeyUpdateInfo) -> IO ()
checkAndInsert table (k, u) = do
  v <- HT.lookup table k
  case v of
    Nothing -> HT.insert table k u
    Just (t', _) -> when (fst u > t') $ HT.insert table k u

-- | Read records from data files and write it to merged data and hint file
putKeysFromRecordFiles :: Handle -> Handle -> KeysTable -> [FilePath] -> IO ()
putKeysFromRecordFiles hintHandle recordHandle table =
  mapM_ (putKeysFromRecordFile hintHandle recordHandle table)

-- | Read record from a single data file and write it to merged data and hint file
putKeysFromRecordFile :: Handle -> Handle -> KeysTable -> FilePath -> IO ()
putKeysFromRecordFile hHt rHt t recordFile = do
  -- TODO: Make this lazy
  c <- B.readFile recordFile
  case runGet parseDataLogs c of
    Left _ -> error "Error occured parsing data log."
    Right dataLogs -> mapM_ (putKeyFromDataLog hHt rHt t) dataLogs

-- | Write a data log to merged data and hint file if it is present in
-- the (Key, KeyUpdateInfo) hashmap obtained from hint files
putKeyFromDataLog :: Handle -> Handle -> KeysTable -> DataLog -> IO ()
putKeyFromDataLog hHt rHt t dataLog = do
  let k = dKey dataLog
      timestamp = dTimestamp dataLog
  v <- HT.lookup t k
  case v of
    Nothing -> return ()
    Just (_, True) -> return ()
    Just (t', False) -> when (t' == timestamp) $ do
      offset <- hFileSize rHt
      B.hPut rHt (runPut (deserializeData k (dHeader dataLog) (dValue dataLog) timestamp))
      B.hPut hHt (runPut (deserializeHint k (offset + 1) timestamp))
      return ()
