-- Simple key value store

import           Control.Monad
import qualified Data.HashTable.IO as HT
import           System.Directory
import           System.IO

-- Store offset of key
data RecordMap = RecordMap (HT.BasicHashTable String Integer)

-- Name of the file containing records
recordsFileName = "records.dat"

-- Name of the file containing offsets
hintFileName = "hint.dat"

-- Delimiter between key value
delim = ' '

getKeyValue :: String -> (String, String)
getKeyValue s = (x, y) where
  x = fst p
  y = (tail . snd) p
  p = span (/= delim) s

getKeyValueAsInt :: String -> (String, Integer)
getKeyValueAsInt s = (x, y) where
  x = fst p
  y = (read . snd) p
  p = getKeyValue s

-- Initialize the database
initDB :: IO RecordMap
initDB = do
    -- Create the records file if not exists
    check <- doesFileExist recordsFileName
    if check
      then
        do
          allcontent <- readFile hintFileName
          m <- HT.fromList (map getKeyValueAsInt (lines allcontent))
          return (RecordMap m)
      else
        do
          writeFile recordsFileName ""
          writeFile hintFileName ""
          m <- HT.new
          return (RecordMap m)

deserialize :: String -> String -> String
deserialize k v = k ++ [delim] ++ v ++ "\n"

-- Insert a key value in the database
put :: RecordMap -> String -> String -> IO RecordMap
put (RecordMap m) k v = do
    ht <- openFile recordsFileName AppendMode
    offset <- hFileSize ht
    hPutStr ht (deserialize k v)
    hClose ht
    appendFile hintFileName (deserialize k (show offset))
    HT.insert m k offset
    return (RecordMap m)

-- Get the value of the key
get :: RecordMap -> String -> IO (Maybe String)
get (RecordMap m) k = do
    maybeOffset <- HT.lookup m k
    case maybeOffset of
      Nothing -> return Nothing
      (Just offset) -> if offset < 0
        then
          return Nothing
        else
          do
            ht <- openFile recordsFileName ReadMode
            hSeek ht AbsoluteSeek offset
            l <- hGetLine ht
            hClose ht
            return (Just ((snd . getKeyValue) l))

-- Delete key from database
delete :: RecordMap -> String -> IO RecordMap
delete (RecordMap m) k = do
    HT.delete m k
    appendFile hintFileName (deserialize k "-1")
    return (RecordMap m)
