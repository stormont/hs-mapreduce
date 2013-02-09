{-# LANGUAGE DeriveDataTypeable #-}
-- | This module contains the logic for dispensing jobs to appropriate worker nodes.
-- Any given job will only be dispensed to one worker node (unless that node times
-- out) and worker nodes can only retrieve jobs if they have registered themselves
-- with the 'Controller'.
--
-- A simple use case for the 'Controller':
--
-- > -- Create the controller and a worker node
-- > myController = createController js  -- where js is a list of jobs
-- > myNode = createNode "my_node" "some_valid_timestamp"
-- >
-- > -- Register the worker node with the controller
-- > myController' = registerNode myController myNode
-- >
-- > -- While jobs exist to be dispensed, assign jobs to worker nodes...
-- > (myController'',myNode') = assignJob myController' myNode
-- >
-- > -- As worker nodes complete, push the result and assign a new job as above...
-- > myController''' = pushResult myController'' $ result myNode'
-- >
-- > -- Unregister the worker node from the controller
-- > myController'''' = unregisterNode myController''' myNode
--
module Distributed.MapReduce.Controller
   where

import Data.Data (Data, Typeable)


-- | The node used to process an individual job. This is a mapping of jobs to node
-- identifiers.
data Node i j r = Node
                { identifier :: i
                , currentJob :: Maybe j
                , result     :: Maybe r
                , timestamp  :: String
                } deriving (Show,Read,Data,Typeable)


-- | 'Node's are compared by their 'identifier'.
instance Eq i => Eq (Node i j r) where
   (==) lhs rhs = (identifier lhs) == (identifier rhs)


-- | 'Node's are compared by their 'identifier'.
instance Ord i => Ord (Node i j r) where
   compare lhs rhs = compare (identifier lhs) (identifier rhs)


-- | @createNode ident timestamp@ creates an empty 'Node' and assigns its identifier
-- and timestamp.
createNode :: i -> String -> Node i j r
createNode i t = Node { identifier = i, currentJob = Nothing, result = Nothing, timestamp = t }


-- | The controller that distributes jobs to known 'Node's. A 'Node' must be
-- registered with the controller (via 'registerNode') in order to have a job
-- assigned (via 'assignJob').
data Controller j i r = Controller
                      { jobs    :: [j]
                      , nodes   :: [Node i j r]
                      , results :: [r]
                      } deriving (Show,Read,Data,Typeable)


-- | @createController jobList@ creates a 'Controller' with no 'Node's and assigns
-- its list of jobs.
createController :: [j] -> Controller j i r
createController js = Controller { jobs = js, nodes = [], results = [] }


-- | @peekJob controller@ returns the next job to be distributed, if one exists.
peekJob :: Controller j i r -> Maybe j
peekJob c =
   case jobs c of
      []    -> Nothing
      (x:_) -> Just x


-- | @popJob controller@ returns the next job to be distributed and a new 'Controller'
-- with that job removed.
popJob :: Controller j i r -> (Maybe j, Controller j i r)
popJob c =
   case jobs c of
      []     -> (Nothing, c)
      (x:xs) -> (Just x, c { jobs = xs })


-- | @pushJob job controller@ pushes the specified @job@ into the @controller@'s list
-- of jobs. If the @job@ already exists in the @controller@, the @controller@ is
-- returned unchanged.
pushJob :: Eq j => j -> Controller j i r -> Controller j i r
pushJob j c =
   if any (== j) xs
     then c
     else c { jobs = (j : xs) }
   where xs = jobs c


-- | @updateNode controller node@ updates the 'Node' within the given 'Controller'
-- to be replaced by @node@. If a @node@ with the given @identifier@ has not already
-- been registered with the @controller@, the @controller@ is returned unchanged.
updateNode :: Eq i => Controller j i r -> Node i j r -> Controller j i r
updateNode c n =
   if any (== n) (nodes c)
     then c { nodes = (n : ns') }
     else c
   where ns' = filter (/= n) $ nodes c


-- | @registerNode controller node@ registers the given 'Node' with the given
-- 'Controller'. If a @node@ with the given @identifier@ has already been registered
-- with the @controller@, the @controller@ is returned unchanged.
registerNode :: Eq i => Controller j i r -> Node i j r -> Controller j i r
registerNode c n =
   if any (== n) (nodes c)
     then c
     else c { nodes = (n : nodes c) }


-- | @unregisterNode controller node@ unregisters the given 'Node' from the given
-- 'Controller'. If a @node@ with the given @identifier@ has not already been
-- registered with the @controller@, the @controller@ is returned unchanged.
unregisterNode :: Eq i => Controller j i r -> Node i j r -> Controller j i r
unregisterNode c n = c { nodes = ns }
   where ns = filter (/= n) $ nodes c


-- | @pushResult controller result@ adds @result@ to @controller@, if @result@ has
-- a value.
pushResult :: Controller j i r -> Maybe r -> Controller j i r
pushResult c r =
   case r of
      Nothing -> c
      Just x  -> c { results = (x : results c) }


-- | @assignJob controller node@ assigns the next job from @controller@ to @node@.
-- The returned 'Controller' is a copy of @controller@ with the job removed. The
-- returned 'Node' is a copy of @node@ with the job assigned to its @currentJob@.
-- If @node@ has not been registered with @controller@, the returned 'Controller'
-- will be the initial @controller@ passed in and the returned 'Node' will be the
-- initial @node@ passed in, with no changes made.
assignJob :: Eq i => Controller j i r -> Node i j r -> (Controller j i r, Node i j r)
assignJob c n =
   if any (== n) (nodes c)
     then (c'', n')
     else (c, n)
   where (j,c') = popJob c
         n' = n { currentJob = j }
         c'' = updateNode (pushResult c' $ result n) n'


-- | @timeOutNode controller node@ will unregister @node@ from @controller@ and
-- push @node@'s 'currentJob' back to @controller@ as an available job.
timeOutNode :: (Eq i, Eq j) => Controller j i r -> Node i j r -> Controller j i r
timeOutNode c n =
   if all (/= n) (nodes c)
     then c
     else case currentJob n of
            Nothing -> unregisterNode c n
            Just x  -> unregisterNode c' n
               where c' = pushJob x c
