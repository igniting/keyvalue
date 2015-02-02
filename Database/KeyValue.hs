-- Simple key value store

module Database.KeyValue ( get
                         , put
                         , delete
                         , initDB
                         , listKeys
                         , closeDB
                         , Config
                         ) where

import qualified Data.ByteString           as B
import qualified Data.ByteString.Lazy      as BL
import qualified Data.HashTable.IO         as HT
import           Data.Serialize.Get
import           Data.Serialize.Put
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
          ht <- openBinaryFile (recordsFileName cfg) ReadWriteMode
          allcontent <- B.readFile (hintFileName cfg)
          m <- case runGet parseHintLogs allcontent of
                 Left _ -> error "Error occured reading hint file"
                 Right logs -> HT.fromList (map getKeyOffsetPair logs)
          return (KeyValue (recordsFileName cfg) ht (hintFileName cfg) m)
      else
        do
          writeFile (recordsFileName cfg) ""
          writeFile (hintFileName cfg) ""
          m <- HT.new
          ht <- openBinaryFile (recordsFileName cfg) ReadWriteMode
          return (KeyValue (recordsFileName cfg) ht (hintFileName cfg) m)

-- Insert a key value in the database
put :: KeyValue -> Key -> Value -> IO ()
put db k v = do
    offset <- hFileSize (currHandle db)
    hSeek (currHandle db) SeekFromEnd 0
    B.hPut (currHandle db) (runPut (deserializeData k v))
    B.appendFile (currHint db) (runPut (deserializeHint k (offset + 1)))
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
            hSeek (currHandle db) AbsoluteSeek (offset - 1)
            l <- BL.hGetContents (currHandle db)
            case runGetLazy parseDataLog l of
              Left _ -> return Nothing
              Right v -> return (Just (dValue v))

-- Delete key from database
delete :: KeyValue -> Key -> IO ()
delete db k = do
    HT.delete (offsetTable db) k
    B.appendFile (currHint db) (runPut (deserializeHint k 0))
    return ()

-- List all keys
listKeys :: KeyValue -> IO [Key]
listKeys db = fmap (map fst) $ (HT.toList . offsetTable) db

closeDB :: KeyValue -> IO ()
closeDB db = hClose (currHandle db)
