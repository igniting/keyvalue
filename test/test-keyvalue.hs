import           Control.Concurrent
import           Data.ByteString.Char8
import qualified Database.KeyValue       as KV
import           System.Directory
import           Test.QuickCheck
import           Test.QuickCheck.Monadic

baseDir :: String
baseDir = "/tmp/keyvalue"

initialize :: IO (MVar KV.KeyValue)
initialize = KV.initDB cfg where
  cfg = KV.Config baseDir

cleanup :: MVar KV.KeyValue -> IO ()
cleanup m = do
  KV.closeDB m
  removeDirectoryRecursive baseDir

testDb :: String -> Property
testDb s = monadicIO $ do
  m <- run initialize
  run $ KV.put m (pack s) (pack s)
  v <- run $ KV.get m (pack s)
  assert (v == Just (pack s))
  run $ cleanup m

main :: IO ()
main = quickCheck testDb
