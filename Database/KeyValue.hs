-- Simple key value store

module Database.KeyValue ( get
                         , put
                         , delete
                         , initDB
                         , Config
                         ) where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Lazy      as BL
import qualified Data.HashTable.IO         as HT
import           Database.KeyValue.Parsing
import           Database.KeyValue.Types
import           System.Directory
import           System.IO

-- Initialize the database
initDB :: Config -> IO KeyValue
initDB cfg = do
    -- Create the records file if not exists
    check <- doesFileExist (recordsFileName cfg)
    if check
      then
        do
          allcontent <- BL.readFile (hintFileName cfg)
          m <- HT.fromList (map getKeyOffsetPair (runGet parseHintLogs allcontent))
          return (KeyValue (recordsFileName cfg) (hintFileName cfg) m)
      else
        do
          writeFile (recordsFileName cfg) ""
          writeFile (hintFileName cfg) ""
          m <- HT.new
          return (KeyValue (recordsFileName cfg) (hintFileName cfg) m)

-- Insert a key value in the database
put :: KeyValue -> Key -> Value -> IO ()
put db k v = do
    ht <- openBinaryFile (currRecords db) AppendMode
    offset <- hFileSize ht
    BL.hPutStr ht (runPut (deserializeData k v))
    hClose ht
    htH <- openBinaryFile (currHint db) AppendMode
    BL.hPutStr htH (runPut (deserializeHint k (offset + 1)))
    hClose htH
    HT.insert (offsetTable db) k (offset + 1)
    return ()

-- Get the value of the key
get :: KeyValue -> Key -> IO (Maybe Value)
get db k = do
    maybeOffset <- HT.lookup (offsetTable db) k
    case maybeOffset of
      Nothing -> return Nothing
      (Just offset) -> if offset == 0
        then
          return Nothing
        else
          do
            ht <- openBinaryFile (currRecords db) ReadMode
            hSeek ht AbsoluteSeek (offset - 1)
            l <- BL.hGetContents ht
            return (Just (dValue (runGet parseDataLog l)))

-- Delete key from database
delete :: KeyValue -> Key -> IO ()
delete db k = do
    HT.delete (offsetTable db) k
    ht <- openBinaryFile (currHint db) AppendMode
    BL.hPutStr ht (runPut (deserializeHint k 0))
    hClose ht
    return ()
