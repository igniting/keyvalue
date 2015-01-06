module Database.KeyValue.Types where

import qualified Data.ByteString   as B
import qualified Data.HashTable.IO as HT
import           Data.Word

type Key   = B.ByteString
type Value = B.ByteString

-- Store offset of key
data RecordMap = RecordMap (HT.BasicHashTable Key Integer)

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
