module ParSort (parmergesort) where

import Prelude hiding (init, last, reverse)
import ConcList
import Data.Monoid
import Control.Monad.Writer
import Control.Parallel.Strategies

quicksort :: (Ord a) => ConcList a -> ConcList a
quicksort = mapReduceDef Singleton

-- alternative implementation that tries to expose more parallelism during
-- merging
parmergesort :: (Ord a) => ConcList a -> ConcList a
parmergesort = runEval .
               parMapReduce 
                   (return . Singleton)
                   (\x y -> return $ combine x y)
                   (return Empty)


type MSMonoid a = (ConcList a, ConcList a)

combine :: (Ord a) => ConcList a -> ConcList a -> ConcList a
combine (Empty) b = b
combine a (Empty) = a
combine (Singleton a) (Singleton b) = if a <= b
                                      then Conc (Singleton a) (Singleton b)
                                      else Conc (Singleton b) (Singleton a)
combine (Singleton a) (Conc b c)    = case (first b) of
    (Just b') ->
         if a <= b'
         then addLeft a (Conc b c)
         else addLeft b' (combine (Singleton a) (rest $ Conc b c))
    Nothing -> combine (Singleton a) c
-- merging is commutative, so we'll save ourselves the trouble.
combine (Conc a b) (Singleton c)    = combine (Singleton c) (Conc a b)
-- if we have more than 2 levels, spark a bit deeper.
combine (Conc (Conc a b) (Conc c d)) (Conc (Conc e f) (Conc g h)) = runEval $ do
    a' <- rpar $ lmerge a c
    b' <- rpar $ rmerge b d
    c' <- rpar $ lmerge e g
    d' <- rpar $ rmerge f h
    rseq a'
    rseq b'
    rseq c'
    rseq d'
    x <- rpar $ mmerge a' b'
    y <- rpar $ mmerge c' d'
    rseq x
    rseq y
    let (a'',b'') = gensplit x
    let (c'',d'') = gensplit y
    a''' <- rpar $ lmerge a'' c''
    b''' <- rpar $ rmerge b'' d''
    rseq a'''
    rseq b'''
    return $ mmerge a''' b'''
combine (Conc a b) (Conc c d)       = runEval $ do
    a' <- rpar $ lmerge a c
    b' <- rpar $ rmerge b d
    rseq a'
    rseq b'
    return $ mmerge a' b'

mmerge :: (Ord a) => MSMonoid a -> MSMonoid a -> ConcList a
mmerge (a,b) (c,d) = let bc = combine b c in
    Conc
        (Conc
            a
            (fst . gensplit $ bc)
        )
        (Conc
            (snd . gensplit $ bc)
            d
        )

-- use the writer Monad to accumulate a result of the two lists merged, along
-- with anything remaining in the list which did not empty first (which may
-- be merged into the corresponding part of the right merge)
lmerge :: (Ord a) => ConcList a -> ConcList a -> MSMonoid a
lmerge a b  = (\(a,b) -> (b,a)) . runWriter $ lmerge' a b where
  lmerge' :: (Ord a) => ConcList a -> ConcList a -> Writer (ConcList a) (ConcList a)
  lmerge' a b
    | not . hasElements $ a = return b
    | not . hasElements $ b = return a
    -- using Instance (Ord a) => Ord (Maybe a) just for convenience
    -- we've already checked for the empty case, so not checking for
    -- Nothings shouldn't be a problem
    | first a < first b     = case (first a) of
        (Just a') -> (tell . Singleton $ a') >> (lmerge' (rest a) b)
    | otherwise             = case (first b) of
        (Just b') -> (tell . Singleton $ b') >> (lmerge' a (rest b))

rmerge :: (Ord a) => ConcList a -> ConcList a -> MSMonoid a
rmerge a b  = (\x -> (fst x, reverse . snd $ x)) . runWriter $ rmerge' a b where
  rmerge' :: (Ord a) => ConcList a -> ConcList a -> Writer (ConcList a) (ConcList a)
  rmerge' a b
    | not . hasElements $ a = return b
    | not . hasElements $ b = return a
    -- using Instance (Ord a) => Ord (Maybe a) just for convenience
    -- we've already checked for the empty case, so not checking for
    -- Nothings shouldn't be a problem
    | last a > last b       = case (last a) of
        (Just a') -> (tell . Singleton $ a') >> (rmerge' (init a) b)
    | otherwise             = case (last b) of
        (Just b') -> (tell . Singleton $ b') >> (rmerge' a (init b))
