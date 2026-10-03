(ns d2pg.snapshot
  "Full load of the mapped tables from a single Datomic db value."
  (:require [clojure.tools.logging :as log]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.sync :as sync]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc]))

(defn- candidate-eids
  "Every entity that might belong in table: those with the first required
  attribute, or with any mapped attribute when the table has no :require."
  [db {:keys [attrs require]}]
  (into #{}
        (comp (mapcat #(d/datoms db {:index :aevt :components [%] :limit -1}))
              (map :e))
        (if require [(first require)] attrs)))

(defn staging-schema
  "The scratch schema a snapshot builds its tables in before swapping them
  into place."
  [replicator-id]
  (str "d2pg_staging_" (schema/sql-name replicator-id)))

(defn snapshot!
  "Rebuilds every mapped table from the current db and records the db's
  basis t as the checkpoint, all in one PostgreSQL transaction. Tables are
  built in a staging schema and swapped into place at the end, so readers
  of the previous tables are blocked only for the swap, not the load.
  previous-tables are the tables an earlier snapshot created; any of them
  the new model no longer maps are dropped. Returns the basis t."
  [datomic-conn ds {:keys [pg-schema] :as model}
   {:keys [replicator-id pull-batch-size]} mapping-hash previous-tables]
  (let [db (d/db datomic-conn)
        basis-t (:t db)
        staging (staging-schema replicator-id)
        staged (assoc model :pg-schema staging)
        names (ddl/table-names model)]
    (log/info "Snapshotting" (count (:tables model)) "tables at t" basis-t)
    (jdbc/with-transaction [tx ds]
      (pg/execute-all! tx (ddl/checkpoint-statements pg-schema))
      (pg/execute-all! tx (ddl/schema-statements staging))
      (pg/execute-all! tx (ddl/statements staged))
      (doseq [[table-key table] (:tables model)]
        (let [eids (candidate-eids db table)]
          (log/info "Loading" (count eids) "candidate entities into" (:sql-name table))
          (sync/sync-entities! tx db staged table-key eids pull-batch-size)))
      (pg/execute-all! tx (ddl/drop-statements pg-schema (distinct (concat previous-tables names))))
      (pg/execute-all! tx (ddl/move-statements staging pg-schema names))
      (pg/write-checkpoint! tx pg-schema replicator-id basis-t mapping-hash names))
    basis-t))
