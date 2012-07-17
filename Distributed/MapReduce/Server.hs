
module Server
   where

import Control.Monad       (msum)
import Control.Monad.Trans (liftIO)
import Control.Exception   (bracket)
import Data.Acid           (openLocalState)
import Data.Acid.Advanced  (query', update')
import Data.Acid.Local     (createCheckpointAndClose)
import Happstack.Server

import Controller
import AcidTypes


myPolicy :: BodyPolicy
myPolicy = (defaultBodyPolicy "/tmp/" 0 1000 1000)


startServer config js = do
   bracket (openLocalState (initDatabase js))
           (createCheckpointAndClose)
           (\acid -> simpleHTTP config (handlers acid))


handlers acid = do
   decodeBody myPolicy
   msum
      [ dir "peek"       $ doPeek acid
      , dir "request"    $ doRequest acid
      , dir "register"   $ doRegister acid
      , dir "unregister" $ doUnregister acid
      ]


doPeek acid = do
   methodM GET
   t <- query' acid PeekNextJob
   rs <- query' acid PeekInstances
   let r =  "Next job: " ++ t ++ "\n"
         ++ "Instances:\n" ++ (show rs)
   ok $ toResponse r


doRequest acid = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   j <- update' acid (PopJob i)
   ok $ toResponse j


doRegister acid = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   mr <- update' acid (RegisterInstance i)
   ok $ toResponse $ "Instance registered: " ++ identifier i


doUnregister acid = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   mr <- update' acid (UnregisterInstance i)
   ok $ toResponse $ "Instance unregistered: " ++ identifier i
