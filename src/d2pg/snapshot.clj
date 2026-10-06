(ns d2pg.snapshot
  "Full load of the mapped tables from a single Datomic db value."
  (:require [clojure.tools.logging :as log]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.sync :as sync]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc])
  (:import (org.postgresql.util PSQLException)))

(defn- sorted-union
  "Lazily merges ascending seqs into one ascending seq without duplicates."
  [seqs]
  (lazy-seq
   (when-let [seqs (not-empty (into [] (keep seq) seqs))]
     (let [m (apply min (map first seqs))]
       (cons m (sorted-union (mapv (fn [s] (drop-while #(= m %) s)) seqs)))))))

(defn- candidate-eids
  "Every entity that might belong in table, ascending and without
  duplicates: those with the first required attribute, or with any mapped
  attribute when the table has no :require. Streamed from the AEVT index,
  which orders each attribute's entities, so memory use stays flat."
  [db {:keys [attrs require]}]
  (sorted-union (for [a (if require [(first require)] (sort attrs))]
                  (map :e (d/datoms db {:index :aevt :components [a] :limit -1})))))

(defn staging-schema
  "The scratch schema a snapshot builds its tables in before swapping them
  into place."
  [replicator-id]
  (str "d2pg_staging_" (schema/sql-name replicator-id)))

(defn- check-ownership!
  "Fails if any of names exists in pg-schema without being one of the tables
  this replicator's previous snapshot created: a snapshot replaces its
  tables, and must never replace someone else's."
  [conn pg-schema replicator-id names previous]
  (when-let [foreign (seq (sort (remove (set previous) (pg/existing-tables conn pg-schema names))))]
    (throw (ex-info (str "Tables " (vec foreign) " already exist in schema " pg-schema
                         " but replicator " replicator-id " did not create them;"
                         " drop them or map different table names")
                    {:type ::foreign-tables :tables (vec foreign)}))))

(defn- swap-in!
  "Replaces the previous tables with the staged ones and writes checkpoint,
  in one transaction."
  [conn pg-schema replicator-id staging previous names checkpoint]
  (try
    (jdbc/with-transaction [tx conn]
      (pg/execute-all! tx (ddl/drop-statements pg-schema previous))
      (pg/execute-all! tx (ddl/move-statements staging pg-schema names))
      (pg/write-checkpoint! tx pg-schema replicator-id checkpoint))
    (catch PSQLException e
      (throw (if (= "2BP01" (.getSQLState e)) ; dependent_objects_still_exist
               (ex-info (str "Cannot replace the tables of replicator " replicator-id
                             " because other objects, such as views, depend on them;"
                             " drop those objects and recreate them after the snapshot")
                        {:type ::dependent-objects}
                        e)
               e)))))

(defn snapshot!
  "Rebuilds every mapped table from db and records db's basis t as the
  checkpoint, with checkpoint's :mapping-hash and :source-id. Tables are
  loaded into a staging schema batch by batch, then swapped into place in
  one transaction with the checkpoint, so readers of the previous tables are
  blocked only for the swap. Only tables this replicator created are
  replaced; those the model no longer maps are dropped. Returns the basis t."
  [ds db {:keys [pg-schema] :as model} {:keys [replicator-id pull-batch-size]} checkpoint]
  (let [basis-t (:t db)
        staging (staging-schema replicator-id)
        staged (assoc model :pg-schema staging)
        names (ddl/table-names model)]
    (with-open [conn (jdbc/get-connection ds)]
      ;; Snapshots sharing a staging schema must not interleave.
      (pg/with-advisory-lock
        conn (str "d2pg:" staging)
        (fn []
          (pg/execute-all! conn (ddl/checkpoint-statements pg-schema))
          (let [previous (:tables (pg/read-checkpoint conn pg-schema replicator-id))]
            (check-ownership! conn pg-schema replicator-id names previous)
            (log/info "Snapshotting" (count (:tables model)) "tables at t" basis-t)
            (jdbc/with-transaction [tx conn]
              (pg/execute-all! tx (ddl/schema-statements staging))
              (pg/execute-all! tx (ddl/statements staged)))
            (doseq [[table-key table] (:tables model)]
              (let [n (reduce (fn [n batch]
                                (jdbc/with-transaction [tx conn]
                                  (sync/sync-batch! tx db staged table-key batch {:fresh? true}))
                                (+ n (count batch)))
                              0
                              (partition-all pull-batch-size (candidate-eids db table)))]
                (log/info "Loaded" (:sql-name table) "from" n "candidate entities")))
            (swap-in! conn pg-schema replicator-id staging previous names
                      (assoc checkpoint :basis-t basis-t :tables names :shape (ddl/shape model)))
            basis-t))))))
