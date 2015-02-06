module Database.KeyValue.Timestamp where

import           Data.Time.Clock.POSIX
import           Database.KeyValue.Types

currentTimestamp :: IO Timestamp
currentTimestamp = do
  t <- getPOSIXTime
  return $! round t
