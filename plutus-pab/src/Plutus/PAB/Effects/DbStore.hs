{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# LANGUAGE TypeApplications   #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE TypeOperators      #-}
{-# options_ghc -Wno-missing-signatures #-}

{-

A beam-specific effect for writing to a beam database. Here we explicitly construct the
database schema for the effects which we wish to store:

- 'Pluts.PAB.Effects.Contract.ContractStore' effect
- 'Pluts.PAB.Effects.Contract.ContractDefinitionStore' effect

In particular this is specialised to 'Sqlite'; but it could be refactored to
work over a more general type, or changed to Postgres.

The schema we've opted for at present is a very simple one, with no ability to
track changes over time.

-}

module Plutus.PAB.Effects.DbStore where
import           Cardano.BM.Trace                (Trace, logDebug, logError)
import           Control.Exception               (SomeException (..), catch)
import           Control.Monad.Freer             (Eff, LastMember, Member, type (~>))
import           Control.Monad.Freer.Reader      (Reader, ask)
import           Control.Monad.Freer.TH          (makeEffect)
import           Data.Text                       (Text)
import qualified Data.Text                       as Text
import           Database.Beam
import           Database.Beam.Backend.SQL
import           Database.Beam.Migrate
import           Database.Beam.Schema.Tables
import           Database.Beam.Sqlite            (Sqlite, SqliteM, runBeamSqliteDebug)
import           Database.SQLite.Simple          (Connection)
import           Plutus.PAB.Monitoring.PABLogMsg (PABLogMsg (..), PABMultiAgentMsg (..))

data ContractInstanceT f
  = ContractInstance
    { _contractInstanceId         :: Columnar f Text
    , _contractInstanceContractId :: Columnar f Text
    , _contractInstanceWallet     :: Columnar f Text -- Note: Sqlite doesn't have a integer type large enough.
    , _contractInstanceState      :: Columnar f (Maybe Text)
    , _contractInstanceActive     :: Columnar f Bool
    } deriving (Generic, Beamable)

ContractInstance
  (LensFor contractInstanceId)
  (LensFor contractInstanceContractId)
  (LensFor contractInstanceWallet)
  (LensFor contractInstanceState)
  (LensFor contractInstanceActive)
  = tableLenses


type ContractInstance   = ContractInstanceT Identity
type ContractInstanceId = PrimaryKey ContractInstanceT Identity

instance Table ContractInstanceT where
  data PrimaryKey ContractInstanceT f = ContractInstanceId (Columnar f Text) deriving (Generic, Beamable)
  primaryKey = ContractInstanceId . _contractInstanceId

data Db f = Db
    { _contractInstances :: f (TableEntity ContractInstanceT)
    }
    deriving (Generic, Database be)

db :: DatabaseSettings be Db
db = defaultDbSettings

checkedSqliteDb :: CheckedDatabaseSettings Sqlite Db
checkedSqliteDb = defaultMigratableDbSettings

-- | Effect for managing a beam-based database.
data DbStoreEffect r where
  -- | Insert a row into a table.
  AddRow
    ::
    ( Beamable table
    , FieldsFulfillConstraint (BeamSqlBackendCanSerialize Sqlite) table
    )
    => DatabaseEntity Sqlite Db (TableEntity table)
    -> table Identity
    -> DbStoreEffect ()

  UpdateRow
    ::
    ( Beamable table
    )
    => SqlUpdate Sqlite table
    -> DbStoreEffect ()

  SelectList
    ::
    ( Beamable table
    , FromBackendRow Sqlite (table Identity)
    )
    => SqlSelect Sqlite (table Identity)
    -> DbStoreEffect [table Identity]

  SelectOne
    ::
    ( Beamable table
    , FromBackendRow Sqlite (table Identity)
    )
    => SqlSelect Sqlite (table Identity)
    -> DbStoreEffect (Maybe (table Identity))

handleDbStore ::
  forall a effs.
  ( Member (Reader Connection) effs
  , LastMember IO effs
  )
  => Trace IO (PABLogMsg a)
  -> DbStoreEffect
  ~> Eff effs
handleDbStore trace eff = do
  connection <- ask @Connection

  case eff of
    AddRow table record ->
        liftIO
            $ runBeamPAB trace connection ()
            $ runInsert
            $ insert table (insertValues [record])

    SelectList q -> do
        liftIO $ runBeamPAB trace connection [] $ runSelectReturningList q

    SelectOne q ->
        liftIO $ runBeamPAB trace connection Nothing $ runSelectReturningOne q

    UpdateRow q ->
        liftIO $ runBeamPAB trace connection () $ runUpdate q

-- | Same as 'Database.Beam.Sqlite.runBeamSqliteDebug'. But in the case where
-- an IO exception is thrown, it is caught, a message is logged and returns a
-- default value.
--
-- We currently suppose that any thrown exception is because the MIGRATE command
-- was not executed.
runBeamPAB :: Trace IO (PABLogMsg a) -> Connection -> b -> SqliteM b -> IO b
runBeamPAB trace connection d action = do
    let traceSql = logDebug trace . SMultiAgent . SqlLog
    liftIO $
        catch (runBeamSqliteDebug traceSql connection action) handleException

    where
        handleException (SomeException e) = do
            let msg = Text.pack $ show e
            logError trace (SMultiAgent $ MigrationNotDone msg)
            return d

makeEffect ''DbStoreEffect
