{-# LANGUAGE TemplateHaskell #-}
module Tests
   where

import Data.List (nub, sort)
import Test.QuickCheck
import Test.QuickCheck.All
import Language.Haskell.TH

import Controller


instance (Arbitrary a, Arbitrary b) => Arbitrary (Node a b) where
   arbitrary = do
      x <- arbitrary
      y <- arbitrary
      return $ Node x y


main = $quickCheckAll


prop_PeekTakesHead xs =
   xs /= [] ==>
   peekJob c == (Just $ head xs)
   where c = initialize xs


prop_PopJobTakesHead xs =
   xs /= [] ==>
   j == (Just $ head xs)
   where c = initialize xs
         (j,_) = popJob c


prop_PopJobDropsHead xs =
   xs /= [] ==>
   jobs c' == drop 1 xs
   where c = initialize xs
         (_,c') = popJob c


prop_NoDuplicateNodes xs =
   all (== 1) $ map fl xs
   where c = foldl registerNode (initialize []) xs
         fl x = length $ filter (== x) (nodes c)


prop_UpdateNode xs =
   xs /= [] ==>
   (sort $ nodes $ updateNode c $ head xs) == (sort $ nub xs)
   where c = foldl registerNode (initialize []) xs


prop_UpdateIgnoresNewNode x xs =
   (xs /= []) && (filter (== x) xs == []) ==>
   (sort $ nodes $ updateNode c x) == (sort $ nub xs)
   where c = foldl registerNode (initialize []) xs


prop_DeregisterDropsNode xs =
   xs /= [] ==>
   (sort $ nodes $ unregisterNode c $ head xs) == (sort $ drop 1 $ nub xs)
   where c = foldl registerNode (initialize []) xs


prop_DeregisterIgnoresNewNodes x xs =
   (xs /= []) && (filter (== x) xs == []) ==>
   ((sort $ nodes c') == (sort $ nub xs)) && (all (/= x) $ nodes c')
   where c = foldl registerNode (initialize []) xs
         c' = unregisterNode c x


prop_AssignJobSetsNode js ns =
   ns /= [] ==>
   (currentJob n) == peekJob c
   where c = foldl registerNode (initialize js) ns
         (_,n) = assignJob c $ head ns


prop_AssignJobPopsJob js ns =
   jobs c' == drop 1 js
   where c = foldl registerNode (initialize js) ns
         (c',_) = assignJob c $ head ns
