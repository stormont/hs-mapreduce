{-# LANGUAGE CPP,
             DeriveDataTypeable,
             FlexibleContexts,
             GeneralizedNewtypeDeriving, 
             MultiParamTypeClasses,
             TemplateHaskell,
             TypeFamilies,
             RecordWildCards,
             TypeSynonymInstances #-}
module AcidTypes
   where

import Control.Applicative  ( (<$>) )
import Control.Monad.Reader ( ask )
import Control.Monad.State  ( get, put )
import Data.Data            ( Data, Typeable )
import Data.Acid            ( AcidState, Query, Update, makeAcidic )
import Data.SafeCopy        ( base, deriveSafeCopy )
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB

import Controller


type Instance = Node String String String
type Database = Controller String String String


initInstance i = Node i Nothing Nothing []

initDatabase ::[String] -> Database
initDatabase xs = Controller xs [] []


findInst i xs = filter (== i) xs

findNotInst i xs = filter (/= i) xs


$(deriveSafeCopy 0 'base ''Node)

$(deriveSafeCopy 0 'base ''Controller)


peekNextJob :: Query Database (Maybe String)
peekNextJob = peekJob <$> ask


popJob :: Instance -> Update Database (Maybe String)
popJob i = do
   c@Controller{..} <- get
   let (c',n) = assignJob c i
   case currentJob n of
      Nothing -> return Nothing
      Just x  -> do
            put c'
            return $ Just x


peekInstances :: Query Database [Instance]
peekInstances = nodes <$> ask


registerInstance :: Instance -> Update Database ()
registerInstance i = do
   c@Controller{..} <- get
   put $ registerNode c i
   return ()


unregisterInstance :: Instance -> Update Database ()
unregisterInstance i = do
   c@Controller{..} <- get
   put $ unregisterNode c i
   return ()


$(makeAcidic ''Database ['peekNextJob, 'peekInstances, 'popJob, 'registerInstance, 'unregisterInstance])
