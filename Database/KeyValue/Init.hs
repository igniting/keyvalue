{-|
  Module      : Database.KeyValue.Init
  Description : Initialize the database
  Stability   : Experimental

  Initialize a keyvalue instance. A keyvalue instance is a directory.
  At any moment, one file is "active" in that directory to be written by
  the keyvalue instance.
-}

module Database.KeyValue.Init(initDB) where

import           Control.Concurrent
import           Control.Monad                    (when)
import qualified Data.HashTable.IO                as HT
import           Database.KeyValue.FileOperations
import           Database.KeyValue.LogOperations
import           Database.KeyValue.Merge
import           Database.KeyValue.Types
import           System.Directory

-- | Initialize the database from the given Config
initDB :: Config -> IO KeyValue
initDB cfg = do
    createDirectoryIfMissing True baseDir
    -- There should be equal number of hint and record files
    filesCount <- fmap (2 *) $ getFilesCount hintExt baseDir
    when (filesCount >= maxOpenFiles cfg) (mergeDataLogs baseDir)
    -- TODO: Add conflict resolution using timestamp
    table <- getFiles hintExt baseDir >>=
      getKeyAndValueLocs >>=
      HT.fromList
    (newHintHandle, newRecordHandle) <- addNewRecord baseDir
    m <- newEmptyMVar
    putMVar m table
    return (KeyValue newHintHandle newRecordHandle m) where
      baseDir = baseDirectory cfg
