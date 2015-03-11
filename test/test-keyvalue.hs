import           Control.Concurrent
import           Data.ByteString.Char8   (pack, unpack)
import           Data.List
import qualified Database.KeyValue       as KV
import           System.Directory
import           System.IO.Temp
import           Test.Hspec
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
  keys <- run $ mergeAndGetKeys dir
  assert (keys `isEquiv` xs)

testDeleteAndMerge :: [String] -> Property
testDeleteAndMerge xs = monadicIO $ do
  dir <- run $ createTempDirectory "/tmp" "keyvalue."
  run $ mapM_ (insertKey dir) xs
  run $ mapM_ (deleteKey dir) xs
  keys <- run $ mergeAndGetKeys dir
  assert (null keys)

isEquiv :: [String] -> [String] -> Bool
isEquiv xs ys = (nub . sort) xs == (nub . sort) ys

insertKey :: FilePath -> String -> IO ()
insertKey dir s = do
  m <- initialize dir
  KV.put m (pack s) (pack s)
  KV.closeDB m

deleteKey :: FilePath -> String -> IO ()
deleteKey dir s = do
  m <- initialize dir
  KV.delete m (pack s)
  KV.closeDB m

mergeAndGetKeys :: FilePath -> IO [String]
mergeAndGetKeys dir = do
  KV.mergeDataLogs dir
  m <- initialize dir
  keys <- fmap (map unpack) (KV.listKeys m)
  cleanup dir m
  return keys

main :: IO ()
main = do
  dir <- createTempDirectory "/tmp" "keyvalue."
  m <- initialize dir
  hspec $ do
    describe "get" $ do
      it "should get an inserted key" $ property (testGet m)
  cleanup dir m
  hspec $ do
    describe "merge" $ do
      it "should merge all files" $ property testMerge
      it "should remove deleted keys" $ property testDeleteAndMerge
  return ()
