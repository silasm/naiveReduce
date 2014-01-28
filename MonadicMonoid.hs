{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances #-}

module MonadicMonoid (MonadicMonoid, (<<>>), mId) where
import Data.Monoid

class (Monad m) => MonadicMonoid m a where
    (<<>>) :: a -> a -> m a
    mId :: m a

instance (Monad m, Monoid a) => MonadicMonoid m a where
    x <<>> y = return $ x <> y
    mId = return mempty
