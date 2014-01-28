module ConcList (ConcList(Empty, Singleton, Conc), ConcList.length, filter, reverse, first, last, hasElements, rest, init, toConcList, cacheMap, uncacheMap, mval, null, singleton, item, split, gensplit, mapReduce, mapReduceDef, mapReduceM, mapReduceMM, parMapReduce, addLeft, addRight) where

import Prelude hiding (head, tail, filter, reverse, last, init, null, singleton)
import Data.Monoid
import Data.Foldable
import Control.Parallel.Strategies
import Control.DeepSeq
import MonadicMonoid

-- Project trying to implement Conc Lists and mapReduce parallelism as
-- highlighted in this talk: http://vimeo.com/6624203

data ConcList a = Empty | Singleton a | Conc (ConcList a) (ConcList a)
     deriving (Show, Eq)

instance Functor ConcList where
    -- "Conc" is used here instead of "append" for "fmap id" correctness, since
    -- "append" can rebalance.
    fmap f = mapReduce (Singleton . f) Conc Empty

instance Foldable ConcList where
    foldMap = mapReduceDef

instance Monoid (ConcList a) where
    mappend = append
    mempty = Empty

-- same as above, but monoid cached as discussed in the linked video.

data CachedConcList a m = CEmpty m
                        | CSingleton a m
                        | CConc (CachedConcList a m) (CachedConcList a m) m

instance NFData a => NFData (ConcList a) where
    rnf Empty = ()
    rnf (Singleton a) = a `seq` ()
    rnf (Conc a b) = rnf (a,b) `seq` ()

toConcList :: [a] -> ConcList a
toConcList xs = let l = Prelude.length xs `div` 2 in smartConc (take l xs) (drop l xs) l

smartConc :: [a] -> [a] -> Int -> ConcList a
smartConc [a] [b] _ = Conc (Singleton a) (Singleton b)
smartConc [a] [ ] _ = Singleton a
smartConc [ ] [b] _ = Singleton b
smartConc [ ] [ ] _ = Empty
smartConc [ ] ys  _ = toConcList ys
smartConc xs  [ ] _ = toConcList xs
smartConc xs  ys  l = runEval $ do
    let l' = l `div` 2
    a' <- rpar $ smartConc (take l' xs) (drop l' xs) l'
    b' <- rpar $ smartConc (take l' ys) (drop l' ys) l'
    return $ Conc a' b'


cacheMap :: (Monoid m) => (a -> m) -> ConcList a -> CachedConcList a m
cacheMap f Empty         = CEmpty mempty
cacheMap f (Singleton a) = CSingleton a (f a)
cacheMap f (Conc a b)    = let a' = cacheMap f a
                               b' = cacheMap f b in
                           CConc a' b' (mval a' `mappend` mval b')

uncacheMap :: CachedConcList a m -> ConcList a
uncacheMap (CEmpty m)       = Empty
uncacheMap (CSingleton a m) = Singleton a
uncacheMap (CConc a b m)    = Conc (uncacheMap a) (uncacheMap b)

mval :: CachedConcList a m -> m
mval (CEmpty m)       = m
mval (CSingleton a m) = m
mval (CConc a b m)    = m

-- optional Conc tree rebalancing function (id for no rebalancing)
rebalance :: ConcList a -> ConcList a
rebalance = id

null :: ConcList a -> Bool
null Empty = True
null _ = False

singleton :: ConcList a -> Bool
singleton (Singleton _) = True
singleton _ = False

item :: ConcList a -> a
item (Singleton a) = a
item _ = undefined

split :: ConcList a -> (ConcList a, ConcList a)
split (Conc a b) = (a,b)
split _ = undefined

gensplit :: ConcList a -> (ConcList a, ConcList a)
gensplit Empty = (Empty, Empty)
gensplit (Singleton a) = (Empty, Singleton a)
gensplit (Conc a b) = (a,b)

head :: ConcList a -> a
head (Empty) = undefined
head (Singleton a) = a
head (Conc a b) = head a

tail :: ConcList a -> ConcList a
tail (Empty) = undefined
tail (Singleton _) = Empty
tail (Conc (Singleton a) b) = b
tail (Conc Empty b) = b
tail (Conc a b) = Conc (tail a) b

append :: ConcList a -> ConcList a -> ConcList a
append a b = rebalance $ Conc a b

addLeft :: a -> ConcList a -> ConcList a
addLeft a Empty         = Singleton a
addLeft a (Singleton b) = Conc (Singleton a) (Singleton b)
addLeft a (Conc b c)    = rebalance $ Conc (addLeft a b) c 

addRight :: ConcList a -> a -> ConcList a
addRight Empty         a = Singleton a
addRight (Singleton b) a = Conc (Singleton b) (Singleton a)
addRight (Conc b c)    a = rebalance $ Conc b (addRight c a)

length :: ConcList a -> Int
length = getSum . mapReduceDef (\x -> Sum 1)

filter :: (a -> Bool) -> ConcList a -> ConcList a
filter p = mapReduce (\x -> if p x then Singleton x else Empty) append Empty

reverse :: ConcList a -> ConcList a
reverse = mapReduce Singleton (\x y -> append y x) Empty

-- head, but well-behaved on "empty" -- returns the value in the first Singleton
-- occuring in the list if any Singletons exist.
first :: ConcList a -> Maybe a
first = getFirst . mapReduceDef (First . Just)

last :: ConcList a -> Maybe a
last = getLast . mapReduceDef (Last . Just)

-- some helpers for the next two functions
isJust :: Maybe a -> Bool
isJust (Just _)  = True
isJust (Nothing) = False

hasElements :: ConcList a -> Bool
hasElements = isJust . getFirst . mapReduceDef (First . Just)

-- tail, but similarly well-behaved on "empty" -- skips over empty elements
-- until it finds one that's nonempty, then returns the rest of the Conc List.
-- This removes all "Empty" values on the left end along with the "first".
--
-- e.g. let xs = Conc (Conc (Conc Empty Empty) (Conc Empty a)) (Conc Empty c)
-- then rest xs = Conc Empty c
rest :: ConcList a -> ConcList a
rest Empty         = error "Empty ConcList!"
rest (Singleton _) = Empty
rest (Conc a b)
    | hasElements a = append (rest a) b
    | hasElements b = rest b
    | otherwise     = error "No elements in ConcList!"

-- same as "rest", but drops the last element.
init :: ConcList a -> ConcList a
init Empty         = error "Empty ConcList!"
init (Singleton _) = Empty
init (Conc a b)
    | hasElements b = append a (init b)
    | hasElements a = (init a)
    | otherwise     = error "No elements in ConcList!"

-- a bunch of different ways one can mapReduce.
mapReduce :: (a -> b) -> (b -> b -> b) -> b -> ConcList a -> b
mapReduce f x id Empty         = id
mapReduce f x id (Singleton a) = f a
mapReduce f x id (Conc a b)    = mapReduce f x id a `x` mapReduce f x id b

mapReduceDef :: (Monoid m) => (a -> m) -> ConcList a -> m
mapReduceDef f Empty         = mempty
mapReduceDef f (Singleton a) = f a
mapReduceDef f (Conc a b)    = mapReduceDef f a `mappend` mapReduceDef f b

mapReduceM :: (Monad m) => (a -> m b) -> (b -> b -> m b) -> m b -> ConcList a -> m b
mapReduceM f x id Empty         = id
mapReduceM f x id (Singleton a) = f a
mapReduceM f x id (Conc a b)    = do 
	a' <- mapReduceM f x id a 
	b' <- mapReduceM f x id b
	a' `x` b'

mapReduceMM :: (MonadicMonoid m b) => (a -> m b) -> ConcList a -> m b
mapReduceMM f Empty         = mId 
mapReduceMM f (Singleton a) = f a
mapReduceMM f (Conc a b)    = do
    a' <- mapReduceMM f a
    b' <- mapReduceMM f b
    a' <<>> b'

parMapReduce :: (a -> Eval b) -> (b -> b -> Eval b) -> Eval b -> ConcList a -> Eval b
parMapReduce f x id Empty         = id
parMapReduce f x id (Singleton a) = f a
parMapReduce f x id (Conc a b)    = do
    a' <- rpar . runEval $ parMapReduce f x id a
    b' <- rpar . runEval $ parMapReduce f x id b
    rseq a'
    rseq b'
    a' `x` b'

