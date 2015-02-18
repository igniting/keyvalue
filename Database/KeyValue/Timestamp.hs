{-|
  Module      : Database,KeyValue.Timestamp
  Description : Generate the current timestamp
  Stability   : Experimental
-}

module Database.KeyValue.Timestamp where

import           Data.Time.Clock.POSIX
import           Database.KeyValue.Types

-- | Give the current time
currentTimestamp :: IO Timestamp
currentTimestamp = do
  t <- getPOSIXTime
  return $! round t
