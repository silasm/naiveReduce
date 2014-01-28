module Main where

import Prelude hiding (length)
import ConcList
import ParSort
import Control.Parallel.Strategies
import Data.Monoid
import Data.Foldable

main :: IO()
main = do
    contents <- readFile "randNum.txt"
    let conc = toConcList . map read . lines $ contents :: ConcList Int
    let list = map read . lines $ contents :: [Int]
    putStrLn . show . parmergesort $ conc
    -- putStrLn . show . fold Sum $ list
