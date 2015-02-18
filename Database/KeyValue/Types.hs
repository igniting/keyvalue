{-
  Module      : Database.KeyValue.Types
  Description : Contains all the types used in KeyValue
  Stability   : Experimental
-}

module Database.KeyValue.Types where

import qualified Data.ByteString       as B
import qualified Data.HashTable.IO     as HT
import           Data.Word
import           System.IO

-- | Type of the keys stored in the database
type Key   = B.ByteString

-- | Type of the values stored in the database
type Value = B.ByteString

-- | Location of a value in the database
data ValueLoc = ValueLoc { rOffset :: Integer
                         , rHandle :: Handle
                         }

-- | Store record info in a hash table
type OffsetTable = HT.BasicHashTable Key ValueLoc

-- | Timestamp is used in conflict resolution
type Timestamp = Word64

-- | This is used while merging
type KeysTable = HT.BasicHashTable Key Timestamp

-- | Hint Log format
data HintLog = HintLog { hKeySize   :: Word32
                       , hKey       :: Key
                       , hOffset    :: Word64
                       , hTimestamp :: Word64
                       }

-- | Data Log format
data DataLog = DataLog { dKeySize   :: Word32
                       , dKey       :: Key
                       , dValueSize :: Word32
                       , dValue     :: Value
                       , dTimestamp :: Word64
                       }

-- | Config for the database
data Config = Config { baseDirectory :: FilePath }

-- | Current instance of the database
data KeyValue = KeyValue { currHintHandle :: Handle
                         , currRecordHandle :: Handle
                         , offsetTable :: OffsetTable
                         }
