import           Control.Concurrent
import           Criterion.Main
import qualified Data.ByteString       as B
import           Data.ByteString.Char8 (pack)
import           Database.KeyValue     as KV
import           System.IO.Temp

k :: B.ByteString
k = pack (replicate 100 '0')

v :: B.ByteString
v = pack (replicate 10000 '0')

insertKey :: MVar KV.KeyValue -> IO ()
insertKey m = KV.put m k v

getKey :: MVar KV.KeyValue -> IO (Maybe B.ByteString)
getKey m = KV.get m k

main :: IO ()
main = do
  dir <- createTempDirectory "/tmp" "keyvalue."
  m <- KV.initDB (KV.Config dir)
  defaultMain [ bench "put" $ nfIO (insertKey m)
              , bench "get" $ nfIO (getKey m)
              ]
  KV.closeDB m
