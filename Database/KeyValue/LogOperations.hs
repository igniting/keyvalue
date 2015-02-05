module Database.KeyValue.LogOperations where

import qualified Data.ByteString           as B
import           Data.Serialize.Get
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
