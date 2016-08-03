{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}

module Derive(deriveBuildRec) where

import           Common               hiding (Q)
import           Control.Monad
import           Data.Binary
import           Data.Binary.Get
import           Data.ByteString.Lazy as DBL (ByteString)
import           Data.Char            (toLower)
import           Data.Map.Strict      (Map, lookup)
import           Language.Haskell.TH


genNames :: Int -> Q [Name]
genNames n = replicateM n $ newName "x"

statements :: [Name] -> [(Name, Strict, Type)] -> Name -> [StmtQ]
statements names args m = fmap (\(n, (name, _, tpe)) -> do
                            bytesName <- newName "b"
                            bindS
                              (varP n)
                              (infixE
                              (Just (infixE (Just (appE (varE $ mkName "runGet") (sigE (varE $ mkName "get") (appT (conT $ mkName "Get") (pure tpe))))) (varE $ mkName ".") (Just (lamE [tupP [wildP,wildP,wildP, conP (mkName "CQL.Bytes") [varP bytesName]]] (varE bytesName)))))
                              (varE $ mkName "<$>")
                              (Just (appE (appE (varE $ mkName "Data.Map.Strict.lookup") (appE (conE $ mkName "CQLString") (litE (StringL (fmap toLower (nameBase name)))))) (varE m))))
                          ) (names `zip` args)

rtrnStmt names conName = [noBindS (appE (varE $ mkName "return") (applyRec (appE (conE conName) (varE $ head names)) (tail names)) )]

applyRec = foldl (\ex nm -> appE ex (varE nm))

deriveBuildRec a = do
  TyConI (DataD _ cName _ constructors _) <- reify a
  let (RecC conName args) = head constructors
  names <- genNames $ length args
  d <- [d| instance BuildRec $(conT a) where
            fromRow m = $(doE (statements names args (mkName "m") ++ rtrnStmt names conName)) |]
  let    [InstanceD [] (AppT fromRowt (ConT _T1)) [FunD fromRowf _text]] = d
         [Clause _ some thing] = _text
  return [InstanceD [] (AppT fromRowt (ConT a)) [FunD fromRowf [Clause [VarP $ mkName "m"] some thing]]]
