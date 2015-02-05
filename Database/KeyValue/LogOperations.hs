module Database.KeyValue.LogOperations where

import           Control.Monad
import qualified Data.ByteString           as B
import qualified Data.HashTable.IO         as HT
import           Data.Serialize.Get
import           Data.Serialize.Put
import           Database.KeyValue.Parsing
import           Database.KeyValue.Types
import           System.FilePath.Find
import           System.FilePath.Posix
import           System.IO
import           System.IO.Temp

getFiles :: String -> FilePath -> IO [FilePath]
getFiles ext = find (depth ==? 0) (extension ==? ext)

hintExt :: String
hintExt = ".hkv"

recordExt :: String
recordExt = ".rkv"

getKeyAndValueLocs :: [FilePath] -> IO [(Key, ValueLoc)]
getKeyAndValueLocs hintFiles = do
  l <- mapM getKeyAndValueLoc hintFiles
  return (concat l) where
    getKeyAndValueLoc hintFile = do
      recordHandle <- getRecordHandle hintFile
      c <- B.readFile hintFile
      case runGet parseHintLogs c of
        Left _ -> error "Error occured in parsing hint file."
        Right hintlogs -> return (map ((\(k, o) -> (k, ValueLoc o recordHandle)) . getKeyOffsetPair) hintlogs)

getRecordHandle :: FilePath -> IO Handle
getRecordHandle hintFile = openBinaryFile (replaceExtension hintFile recordExt) ReadMode

addNewRecord :: FilePath -> IO (Handle, Handle)
addNewRecord dir = do
  (hintFile, hintHandle) <- openNewBinaryFile dir hintExt
  recordHandle <- openBinaryFile (replaceExtension hintFile recordExt) ReadWriteMode
  hSetBuffering hintHandle NoBuffering
  hSetBuffering recordHandle NoBuffering
  return (hintHandle, recordHandle)

getKeysFromHintFiles :: [FilePath] -> IO KeysTable
getKeysFromHintFiles hintFiles = do
  table <- HT.new
  allKeysList <- fmap concat (mapM getKeysFromHintFile hintFiles)
  mapM_ (checkAndInsert table) allKeysList
  return table where
    getKeysFromHintFile :: FilePath -> IO [(Key, Timestamp)]
    getKeysFromHintFile file = do
      c <- B.readFile file
      case runGet parseHintLogs c of
        Left _ -> error "Error occured parsing hint log."
        Right hintLogs -> return (map (\l -> (hKey l, 0)) (filter (\l -> hOffset l /= 0) hintLogs))

    checkAndInsert :: KeysTable -> (Key, Timestamp) -> IO ()
    checkAndInsert table (k, t) = do
      v <- HT.lookup table k
      case v of
        Nothing -> HT.insert table k t
        Just t' -> when (t' > t) $ HT.insert table k t'

putKeysFromRecordFiles :: Handle -> Handle -> KeysTable -> [FilePath] -> IO ()
putKeysFromRecordFiles hintHandle recordHandle table recordFiles =
  mapM_ (putKeysFromRecordFile hintHandle recordHandle table) recordFiles where
    putKeysFromRecordFile :: Handle -> Handle -> KeysTable -> FilePath -> IO ()
    putKeysFromRecordFile hHt rHt t recordFile = do
      c <- B.readFile recordFile
      case runGet parseDataLogs c of
        Left _ -> error "Error occured parsing data log."
        Right dataLogs -> mapM_ (putKeyFromDataLog hHt rHt t) dataLogs

    putKeyFromDataLog :: Handle -> Handle -> KeysTable -> DataLog -> IO ()
    putKeyFromDataLog hHt rHt t dataLog = do
      let k = dKey dataLog
      v <- HT.lookup t k
      case v of
        Nothing -> return ()
        Just _ -> do
          -- TODO: Check time stamps
          offset <- hFileSize rHt
          B.hPut rHt (runPut (deserializeData k (dValue dataLog)))
          B.hPut hHt (runPut (deserializeHint k (offset + 1)))
          return ()
