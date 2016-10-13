{-# LANGUAGE CPP               #-}
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
import           Prelude              hiding (lookup)


genNames :: Int -> Q [Name]
genNames n = replicateM n $ newName "x"

statements :: [Name] -> [(Name, Strict, Type)] -> Name -> [StmtQ]
statements names args r = fmap (\(n, (name, _, tpe)) -> do
                            bytesName <- newName "b"
                            bindS
                              (varP n)
                              (infixE
                              (Just (infixE (Just (appE (varE 'runGet) (sigE (varE 'get) (appT (conT ''Get) (pure tpe))))) (varE $ mkName ".") (Just (lamE [tupP [wildP,wildP,wildP, conP (mkName "CQL.Bytes") [varP bytesName]]] (varE bytesName)))))
                              (varE $ mkName "<$>")
                              (Just (appE (appE (varE 'lookup) (appE (conE 'CQLString) (litE (StringL (fmap toLower (nameBase name)))))) (varE r))))
                          ) (names `zip` args)

rtrnStmt names conName = [noBindS (appE (varE $ mkName "return") (applyRec (appE (conE conName) (varE $ head names)) (tail names)) )]

applyRec = foldl (\ex nm -> appE ex (varE nm))

-- | Derives BuildRec instances for record types.
deriveBuildRec a = do
#if __GLASGOW_HASKELL__ >= 800
  TyConI (DataD _ cName _ _ constructors _) <- reify a
#else
  TyConI (DataD _ cName _ constructors _) <- reify a
#endif
  case head constructors of
    (RecC conName args) -> do
      names <- genNames $ length args
      rowName <- newName "r"
      [d| instance BuildRec $(conT a) where
            fromRow = $(lamE [varP rowName] (doE (statements names args rowName ++ rtrnStmt names conName))) |]

    _ -> fail "deriveBuildRec: Only simple records supported, is the type you are deriving for, a record type?"
