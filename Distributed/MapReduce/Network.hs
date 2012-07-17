
module Network
   where


-- initial job list
-- job controller
-- happstack server
-- register callback
-- unregister callback


data Command = PeekStatus
             | RegisterClient
             | UnregisterClient
             | RequestJob
             deriving (Show,Read,Eq,Ord)
