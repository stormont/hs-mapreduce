{-# LANGUAGE CPP,
             DeriveDataTypeable,
             FlexibleContexts,
             FlexibleInstances,
             GeneralizedNewtypeDeriving, 
             MultiParamTypeClasses,
             TemplateHaskell,
             TypeFamilies,
             RecordWildCards,
             TypeSynonymInstances #-}
module Distributed.MapReduce.AcidTypes
   where

import Control.Applicative  ( (<$>) )
import Control.Monad.Reader ( ask )
import Control.Monad.State  ( get, put )
import Data.Data            ( Data, Typeable )
import Data.Acid            ( AcidState, Query, Update, makeAcidic )
import Data.SafeCopy        ( base, deriveSafeCopy )
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB

import qualified Distributed.MapReduce.Controller as C


type Instance = C.Node String String String
type Database = C.Controller String String String


initInstance i = C.Node i Nothing Nothing []

initDatabase ::[String] -> Database
initDatabase xs = C.Controller xs [] []


findInst i xs = filter (== i) xs

findNotInst i xs = filter (/= i) xs


$(deriveSafeCopy 0 'base ''C.Node)

$(deriveSafeCopy 0 'base ''C.Controller)


peekNextJob :: Query Database (Maybe String)
peekNextJob = C.peekJob <$> ask


popJob :: Instance -> Update Database (Maybe String)
popJob i = do
   c@C.Controller{..} <- get
   let (c',n) = C.assignJob c i
   case C.currentJob n of
      Nothing -> return Nothing
      Just x  -> do
            put c'
            return $ Just x


peekInstances :: Query Database [Instance]
peekInstances = C.nodes <$> ask


registerInstance :: Instance -> Update Database ()
registerInstance i = do
   c@C.Controller{..} <- get
   put $ C.registerNode c i
   return ()


unregisterInstance :: Instance -> Update Database ()
unregisterInstance i = do
   c@C.Controller{..} <- get
   put $ C.unregisterNode c i
   return ()


$(makeAcidic ''Database ['peekNextJob, 'peekInstances, 'popJob, 'registerInstance, 'unregisterInstance])
