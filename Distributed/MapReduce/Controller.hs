
module Controller
   where

import Data.Time


data Node a b = Node
              { identifier :: a
              , currentJob :: Maybe b
              } deriving (Show,Read)


instance Eq a => Eq (Node a b) where
   (==) lhs rhs = (identifier lhs) == (identifier rhs)


instance Ord a => Ord (Node a b) where
   compare lhs rhs = compare (identifier lhs) (identifier rhs)


data Controller a b = Controller
                    { jobs  :: [a]
                    , nodes :: [Node b a]
                    } deriving (Show,Read)


initialize js = Controller { jobs = js, nodes = [] }


peekJob :: Controller a b -> Maybe a
peekJob c =
   case jobs c of
      []    -> Nothing
      (x:_) -> Just x


popJob :: Controller a b -> (Maybe a, Controller a b)
popJob c =
   case jobs c of
      []     -> (Nothing, c)
      (x:xs) -> (Just x, c { jobs = xs })


registerNode :: Eq b => Controller a b -> Node b a -> Controller a b
registerNode c n =
   if any (== n) (nodes c)
     then c
     else c { nodes = (n : nodes c) }


updateNode :: Eq b => Controller a b -> Node b a -> Controller a b
updateNode c n =
   if any (== n) (nodes c)
     then c { nodes = (n : ns') }
     else c
   where ns' = filter (/= n) $ nodes c


unregisterNode :: Eq b => Controller a b -> Node b a -> Controller a b
unregisterNode c n = c { nodes = ns }
   where ns = filter (/= n) $ nodes c


assignJob :: Eq b => Controller a b -> Node b a -> (Controller a b, Node b a)
assignJob c n = (c'', n')
   where (j,c') = popJob c
         n' = n { currentJob = j }
         c'' = updateNode c' n'
