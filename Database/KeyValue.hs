-- Simple key value store

module Database.KeyValue ( get
                         , put
                         , delete
                         , initDB
                         ) where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Lazy      as BL
import qualified Data.HashTable.IO         as HT
import           Database.KeyValue.Parsing
import           Database.KeyValue.Types
import           System.Directory
import           System.IO

-- Name of the file containing records
recordsFileName = "records.dat"

-- Name of the file containing offsets
hintFileName = "hint.dat"

-- Initialize the database
initDB :: IO RecordMap
initDB = do
    -- Create the records file if not exists
    check <- doesFileExist recordsFileName
    if check
      then
        do
          allcontent <- BL.readFile hintFileName
          m <- HT.fromList (map getKeyOffsetPair (runGet parseHintLogs allcontent))
          return (RecordMap m)
      else
        do
          writeFile recordsFileName ""
          writeFile hintFileName ""
          m <- HT.new
          return (RecordMap m)

-- Insert a key value in the database
put :: RecordMap -> Key -> Value -> IO ()
put (RecordMap m) k v = do
    ht <- openFile recordsFileName AppendMode
    offset <- hFileSize ht
    BL.hPutStr ht (runPut (deserializeData k v))
    hClose ht
    BL.appendFile hintFileName (runPut (deserializeHint k (offset + 1)))
    HT.insert m k (offset + 1)
    return ()

-- Get the value of the key
get :: RecordMap -> Key -> IO (Maybe Value)
get (RecordMap m) k = do
    maybeOffset <- HT.lookup m k
    case maybeOffset of
      Nothing -> return Nothing
      (Just offset) -> if offset == 0
        then
          return Nothing
        else
          do
            ht <- openFile recordsFileName ReadMode
            hSeek ht AbsoluteSeek (offset - 1)
            l <- BL.hGetContents ht
            return (Just (dValue (runGet parseDataLog l)))

-- Delete key from database
delete :: RecordMap -> Key -> IO ()
delete (RecordMap m) k = do
    HT.delete m k
    BL.appendFile hintFileName (runPut (deserializeHint k 0))
    return ()
