{-# LANGUAGE TemplateHaskell #-}
module Distributed.MapReduce.Tests
   where

import Data.List (nub, sort)
import Test.QuickCheck
import Test.QuickCheck.All
import Language.Haskell.TH

import Distributed.MapReduce.Controller


instance (Arbitrary a, Arbitrary b, Arbitrary c) => Arbitrary (Node a b c) where
   arbitrary = do
      x <- arbitrary
      y <- arbitrary
      z <- arbitrary
      return $ Node x y z "0"


main = $quickCheckAll


-- | @peekJob@ should return only the next job in the controller.
prop_PeekTakesHead xs =
   xs /= [] ==>
   peekJob c == (Just $ head xs)
   where c = createController xs


-- | @popJob@ should return only the next job in the controller.
prop_PopJobTakesHead xs =
   xs /= [] ==>
   j == (Just $ head xs)
   where c = createController xs
         (j,_) = popJob c


-- | @popJob@ should remove the next job from the controller.
prop_PopJobDropsHead xs =
   xs /= [] ==>
   jobs c' == drop 1 xs
   where c = createController xs
         (_,c') = popJob c


-- | @pushJob@ should ignore adding a duplicate job to the controller.
prop_PushJobIgnoresDuplicates xs =
   xs /= [] ==>
   jobs c == jobs c'
   where c = createController xs
         c' = pushJob (head xs) c


-- | @pushJob@ should add a new job to the controller.
prop_PushJobAddsNewJob x xs =
   xs /= [] && all (/= x) xs ==>
   (length $ jobs c') == (length $ jobs c) + 1
   where c = createController xs
         c' = pushJob x c


-- | @registerNode@ should ignore registering a duplicate
-- 'Distributed.MapReduce.Node' to the controller.
prop_NoDuplicateNodes xs =
   all (== 1) $ map fl xs
   where c = foldl registerNode (createController []) xs
         fl x = length $ filter (== x) (nodes c)


-- | @updateNode@ should update an existing 'Distributed.MapReduce.Node'
-- in the controller.
prop_UpdateNode xs =
   xs /= [] ==>
   (sort $ nodes $ updateNode c $ head xs) == (sort $ nub xs)
   where c = foldl registerNode (createController []) xs


-- | @updateNode@ should ignore an unregistered 'Distributed.MapReduce.Node'.
prop_UpdateIgnoresNewNode x xs =
   (xs /= []) && all (/= x) xs ==>
   (sort $ nodes $ updateNode c x) == (sort $ nub xs)
   where c = foldl registerNode (createController []) xs


-- | @unregisterNode@ should remove an existing 'Distributed.MapReduce.Node'
-- from the controller.
prop_DeregisterDropsNode xs =
   xs /= [] ==>
   (sort $ nodes $ unregisterNode c $ head xs) == (sort $ drop 1 $ nub xs)
   where c = foldl registerNode (createController []) xs


-- | @unregisterNode@ should ingore an unregistered 'Distributed.MapReduce.Node'.
prop_DeregisterIgnoresNewNodes x xs =
   (xs /= []) && all (/= x) xs ==>
   ((sort $ nodes c') == (sort $ nub xs)) && (all (/= x) $ nodes c')
   where c = foldl registerNode (createController []) xs
         c' = unregisterNode c x


-- | @pushResult@ should add the result, if it exists.
prop_PushResult r xs =
   xs /= [] ==>
   case r of
      Nothing -> (length $ results c') == (length $ results c)
      _       -> (length $ results c') == (length $ results c) + 1
   where c = foldl registerNode (createController []) xs
         r = result $ head xs
         c' = pushResult c r


-- | @assignJob@ should set the 'Distributed.MapReduce.currentJob' of a
-- registered 'Distributed.MapReduce.Node'.
prop_AssignJobSetsNode js ns =
   ns /= [] ==>
   (currentJob n) == peekJob c
   where c = foldl registerNode (createController js) ns
         (_,n) = assignJob c $ head ns


-- | @assignJob@ should remove the assigned job from the controller.
prop_AssignJobPopsJob js ns =
   (js /= []) && (ns /= []) ==>
   jobs c' == drop 1 js
   where c = foldl registerNode (createController js) ns
         (c',_) = assignJob c $ head ns


-- | @timeOutNode@ should ignore unregistered 'Distributed.MapReduce.Node's.
prop_TimeOutNodeIgnoresNewNodes x xs =
   (xs /= []) && all (/= x) xs ==>
   (sort $ nodes c') == (sort $ nub xs)
   where c = foldl registerNode (createController []) xs
         c' = timeOutNode c x


-- | @timeOutNode@ should unregister existing 'Distributed.MapReduce.Node's.
prop_TimeOutNodeUnregistersNode xs =
   xs /= [] ==>
   (sort $ nodes c') == (sort $ drop 1 nubbed)
   where nubbed = nub xs
         c = foldl registerNode (createController []) nubbed
         x = head nubbed
         c' = timeOutNode c x


-- | @timeOutNode@ should replace the unregistered 'Distributed.MapReduce.Node'
-- 'Distributed.MapReduce.currentJob', if the job was set.
prop_TimeOutNodeReplacesJob xs =
   xs /= [] ==>
   case currentJob x of
      Nothing -> (length $ jobs c') == (length $ jobs c)
      _       -> (length $ jobs c') == (length $ jobs c) + 1
   where nubbed = nub xs
         c = foldl registerNode (createController []) nubbed
         x = head nubbed
         c' = timeOutNode c x
