{-|
  Module      : Database.KeyValue.FileOperations
  Description : All file related operations
  Stability   : Experimental
-}

module Database.KeyValue.FileOperations where

import           Control.Monad
import           System.Directory
import           System.FilePath.Find

-- | Check if a directory exists, throw an error otherwise
assertDirectoryExists :: FilePath -> IO ()
assertDirectoryExists dir = doesDirectoryExist dir >>=
    (\x -> unless x (error errormsg)) where
      errormsg = "Directory " ++ dir ++ " does not exists."

-- | Get all files ending the a given extension in a given directory
getFiles :: String -> FilePath -> IO [FilePath]
getFiles ext = find (depth ==? 0) (extension ==? ext)

-- | Return number of files with a given extension in the given directory
getFilesCount :: String -> FilePath -> IO Int
getFilesCount ext dir = fmap length $ getFiles ext dir
