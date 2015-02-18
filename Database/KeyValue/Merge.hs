{-|
  Module      : Database.KeyValue.Merge
  Description : Merge all non-active data logs
  Stability   : Experimental

  Over a period of time, lot of data logs might be generated, which might
  contain reduntant entries. In the merge process, all the data logs are merged
  into one data log and the duplicate/deleted entries are removed.
-}

module Database.KeyValue.Merge where

import           Control.Exception
import           Database.KeyValue.FileOperations
import           Database.KeyValue.LogOperations
import           System.Directory
import           System.IO

-- | Merge all data logs, given the base directory.
-- Currently the data logs are identified by their extension
mergeDataLogs :: FilePath -> IO ()
mergeDataLogs dir = do
    assertDirectoryExists dir
    hintFiles <- getFiles hintExt dir >>= evaluate
    recordFiles <- getFiles recordExt dir >>= evaluate
    currentKeys <- getKeysFromHintFiles hintFiles
    (newHintHandle, newRecordHandle) <- addNewRecord dir
    putKeysFromRecordFiles newHintHandle newRecordHandle currentKeys recordFiles
    mapM_ hClose [newHintHandle, newRecordHandle]
    mapM_ removeFile (hintFiles ++ recordFiles)
