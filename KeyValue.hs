-- Simple key value store

import           Control.Monad
import qualified Data.HashTable.IO as HT
import           System.Directory

data RecordMap = RecordMap (HT.BasicHashTable String String)

-- Name of the file containing records
fileName = "records.dat"

-- Delimiter between key value
delim = ' '

createFileIfNotExists :: FilePath -> IO ()
createFileIfNotExists name = do
    check <- doesFileExist name
    unless check $ writeFile name ""

getKeyValue :: String -> (String, String)
getKeyValue s = (x, y) where
  x = fst p
  y = (tail . snd) p
  p = span (/= delim) s

-- Initialize the database
initDB :: IO RecordMap
initDB = do
    -- Create the file if not exists
    createFileIfNotExists fileName
    allcontent <- readFile fileName
    m <- HT.fromList (map getKeyValue (lines allcontent))
    return (RecordMap m)

deserialize :: String -> String -> String
deserialize k v = k ++ [delim] ++ v ++ "\n"

-- Insert a key value in the database
put :: RecordMap -> String -> String -> IO RecordMap
put (RecordMap m) k v = do
    appendFile fileName (deserialize k v)
    HT.insert m k v
    return (RecordMap m)

-- Get the value of the key
get :: RecordMap -> String -> IO (Maybe String)
get (RecordMap m) = HT.lookup m
