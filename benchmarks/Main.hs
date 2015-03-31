import           Criterion.Main
import qualified Data.ByteString       as B
import           Data.ByteString.Char8 (pack)
import           Database.KeyValue     as KV
import           System.IO.Temp

k :: B.ByteString
k = pack (replicate 100 '0')

v :: B.ByteString
v = pack (replicate 10000 '0')

insertKey :: KV.KeyValue -> IO ()
insertKey db = KV.put db k v

getKey :: KV.KeyValue -> IO (Maybe B.ByteString)
getKey db = KV.get db k

main :: IO ()
main = do
  dir <- createTempDirectory "/tmp" "keyvalue."
  db <- KV.initDB (KV.Config dir 2)
  defaultMain [ bench "put" $ nfIO (insertKey db)
              , bench "get" $ nfIO (getKey db)
              ]
  KV.closeDB db
