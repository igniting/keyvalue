import           Control.Concurrent
import           Data.ByteString.Char8   (pack, unpack)
import           Data.List
import qualified Database.KeyValue       as KV
import           System.Directory
import           System.IO.Temp
import           Test.QuickCheck
import           Test.QuickCheck.Monadic

initialize :: FilePath -> IO (MVar KV.KeyValue)
initialize dir = KV.initDB cfg where
  cfg = KV.Config dir

cleanup :: FilePath -> MVar KV.KeyValue -> IO ()
cleanup dir m = do
  KV.closeDB m
  removeDirectoryRecursive dir

testGet :: MVar KV.KeyValue -> String -> Property
testGet m s = monadicIO $ do
  run $ KV.put m (pack s) (pack s)
  v <- run $ KV.get m (pack s)
  assert (v == Just (pack s))

testMerge :: [String] -> Property
testMerge xs = monadicIO $ do
  dir <- run $ createTempDirectory "/tmp" "keyvalue."
  run $ mapM_ (insertKey dir) xs
  run $ KV.mergeDataLogs dir
  m <- run $ initialize dir
  keys <- run $ fmap (map unpack) (KV.listKeys m)
  assert (keys `isEquiv` xs)
  run $ cleanup dir m

isEquiv :: [String] -> [String] -> Bool
isEquiv xs ys = (nub . sort) xs == (nub . sort) ys

insertKey :: FilePath -> String -> IO ()
insertKey dir s = do
  m <- initialize dir
  KV.put m (pack s) (pack s)
  KV.closeDB m

main :: IO ()
main = do
  dir <- createTempDirectory "/tmp" "keyvalue."
  m <- initialize dir
  quickCheck (testGet m)
  cleanup dir m
  quickCheck testMerge
  return ()
