module Database.KeyValue.Types where

import qualified Data.ByteString   as B
import qualified Data.HashTable.IO as HT
import           Data.Word
import           System.IO

type Key   = B.ByteString
type Value = B.ByteString

-- Store offset of key
type OffsetTable = HT.BasicHashTable Key Integer

-- Hint Log format
data HintLog = HintLog { hKeySize :: Word32
                       , hKey     :: Key
                       , hOffset  :: Word64
                       }

-- Data Log format
data DataLog = DataLog { dKeySize   :: Word32
                       , dKey       :: Key
                       , dValueSize :: Word32
                       , dValue     :: Value
                       }

-- Config for the database
data Config = Config { recordsFileName :: FilePath
                     , hintFileName :: FilePath
                     }

data KeyValue = KeyValue { currRecords :: FilePath
                         , currHint :: FilePath
                         , offsetTable :: OffsetTable
                         }
