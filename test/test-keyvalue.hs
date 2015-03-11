import           Data.ByteString.Char8   (pack, unpack)
import           Data.List
import qualified Database.KeyValue       as KV
import           System.Directory
import           System.IO.Temp
import           Test.Hspec
import           Test.QuickCheck
import           Test.QuickCheck.Monadic

initialize :: FilePath -> IO KV.KeyValue
initialize dir = KV.initDB cfg where
  cfg = KV.Config dir

cleanup :: FilePath -> KV.KeyValue -> IO ()
cleanup dir db = do
  KV.closeDB db
  removeDirectoryRecursive dir

testGet :: KV.KeyValue -> String -> Property
testGet db s = monadicIO $ do
  run $ KV.put db (pack s) (pack s)
  v <- run $ KV.get db (pack s)
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
  db <- initialize dir
  KV.put db (pack s) (pack s)
  KV.closeDB db

deleteKey :: FilePath -> String -> IO ()
deleteKey dir s = do
  db <- initialize dir
  KV.delete db (pack s)
  KV.closeDB db

mergeAndGetKeys :: FilePath -> IO [String]
mergeAndGetKeys dir = do
  KV.mergeDataLogs dir
  db <- initialize dir
  keys <- fmap (map unpack) (KV.listKeys db)
  cleanup dir db
  return keys

main :: IO ()
main = do
  dir <- createTempDirectory "/tmp" "keyvalue."
  db <- initialize dir
  hspec $
    describe "get" $
      it "should get an inserted key" $ property (testGet db)
  cleanup dir db
  hspec $
    describe "merge" $ do
      it "should merge all files" $ property testMerge
      it "should remove deleted keys" $ property testDeleteAndMerge
  return ()
