(ns d2pg.snapshot
  "Full load of the mapped tables from a single Datomic db value."
  (:require [clojure.tools.logging :as log]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
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

(defn snapshot!
  "Drops and recreates every mapped table, loads it from the current db, and
  records the db's basis t as the checkpoint, all in one PostgreSQL
  transaction. Returns the basis t."
  [datomic-conn ds {:keys [pg-schema] :as model}
   {:keys [replicator-id pull-batch-size]} mapping-hash]
  (let [db (d/db datomic-conn)
        basis-t (:t db)]
    (log/info "Snapshotting" (count (:tables model)) "tables at t" basis-t)
    (jdbc/with-transaction [tx ds]
      (pg/execute-all! tx (ddl/checkpoint-statements pg-schema))
      (pg/execute-all! tx (ddl/drop-statements model))
      (pg/execute-all! tx (ddl/statements model))
      (doseq [[table-key table] (:tables model)]
        (let [eids (candidate-eids db table)]
          (log/info "Loading" (count eids) "candidate entities into" (:sql-name table))
          (sync/sync-entities! tx db model table-key eids pull-batch-size)))
      (pg/write-checkpoint! tx pg-schema replicator-id basis-t mapping-hash))
    basis-t))
