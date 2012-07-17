
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


data Callbacks = Callbacks
               { registerCallback         :: (Instance -> IO ())
               , unregisterCallback       :: (Instance -> IO ())
               , formatPeekResponse       :: (Maybe String -> [Instance] -> String)
               , formatRequestResponse    :: (Maybe String -> String)
               , formatRegisterResponse   :: (Instance -> String)
               , formatUnregisterResponse :: (Instance -> String)
               }


myPolicy :: BodyPolicy
myPolicy = (defaultBodyPolicy "/tmp/" 0 1000 1000)


startServer config cFuncs js = do
   bracket (openLocalState (initDatabase js))
           (createCheckpointAndClose)
           (\acid -> simpleHTTP config (handlers acid cFuncs))


handlers acid cFuncs = do
   decodeBody myPolicy
   msum
      [ dir "peek"       $ doPeek acid cFuncs
      , dir "request"    $ doRequest acid cFuncs
      , dir "register"   $ doRegister acid cFuncs
      , dir "unregister" $ doUnregister acid cFuncs
      ]


doPeek acid cFuncs = do
   methodM GET
   j <- query' acid PeekNextJob
   is <- query' acid PeekInstances
   ok $ toResponse $ (formatPeekResponse cFuncs) j is


doRequest acid cFuncs = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   j <- update' acid (PopJob i)
   ok $ toResponse $ (formatRequestResponse cFuncs) j


doRegister acid cFuncs = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   mr <- update' acid (RegisterInstance i)
   liftIO $ (registerCallback cFuncs) i
   ok $ toResponse $ (formatRegisterResponse cFuncs) i


doUnregister acid cFuncs = do
   methodM POST
   name <- lookRead "id"
   let i = initInstance name
   mr <- update' acid (UnregisterInstance i)
   liftIO $ (unregisterCallback cFuncs) i
   ok $ toResponse $ (formatUnregisterResponse cFuncs) i
