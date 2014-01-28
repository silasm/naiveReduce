module Main where

import System.Random

main :: IO()
main = do
    gen <- getStdGen
    let xs = randoms gen :: [Int]
    mapM (\r -> appendFile "randNum.txt" (show r ++ "\n")) xs
    return ()
