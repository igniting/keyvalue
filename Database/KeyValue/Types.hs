module Database.KeyValue.Types where

import qualified Data.ByteString       as B
import qualified Data.HashTable.IO     as HT
import           Data.Word
import           System.IO

type Key   = B.ByteString
type Value = B.ByteString

data ValueLoc = ValueLoc { rOffset :: Integer
                         , rHandle :: Handle
                         }

-- Store record info in a hash table
type OffsetTable = HT.BasicHashTable Key ValueLoc

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
data Config = Config { baseDirectory :: FilePath }

data KeyValue = KeyValue { currHintHandle :: Handle
                         , currRecordHandle :: Handle
                         , offsetTable :: OffsetTable
                         }
