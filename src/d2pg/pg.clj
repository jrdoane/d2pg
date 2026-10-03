(ns d2pg.pg
  "PostgreSQL side: applying DDL, writing rows, and the checkpoint."
  (:require [clojure.string :as str]
            [d2pg.ddl :as ddl :refer [qualified quote-ident]]
            [d2pg.schema :as schema]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(defn execute-all! [conn statements]
  (doseq [s statements]
    (jdbc/execute! conn [s])))

;; ---------------------------------------------------------------------------
;; Checkpoint

(defn read-checkpoint
  "Returns {:basis-t :mapping-hash} for replicator-id, or nil if this
  replicator has never completed a snapshot."
  [conn pg-schema replicator-id]
  (when-let [row (jdbc/execute-one!
                  conn
                  [(str "SELECT basis_t, mapping_hash FROM "
                        (qualified pg-schema ddl/checkpoint-table)
                        " WHERE replicator_id = ?")
                   replicator-id]
                  {:builder-fn rs/as-unqualified-maps})]
    {:basis-t (:basis_t row)
     :mapping-hash (:mapping_hash row)}))

(defn write-checkpoint! [conn pg-schema replicator-id basis-t mapping-hash]
  (jdbc/execute! conn
                 [(str "INSERT INTO " (qualified pg-schema ddl/checkpoint-table)
                       " (replicator_id, basis_t, mapping_hash, updated_at)"
                       " VALUES (?, ?, ?, now())"
                       " ON CONFLICT (replicator_id) DO UPDATE SET"
                       " basis_t = EXCLUDED.basis_t,"
                       " mapping_hash = EXCLUDED.mapping_hash,"
                       " updated_at = EXCLUDED.updated_at")
                  replicator-id basis-t mapping-hash]))

;; ---------------------------------------------------------------------------
;; Rows

(defn- upsert-sql [pg-schema {:keys [sql-name columns]}]
  (let [cols (cons schema/pk-column (map :column columns))]
    (str "INSERT INTO " (qualified pg-schema sql-name)
         " (" (str/join ", " (map quote-ident cols)) ")"
         " VALUES (" (str/join ", " (repeat (count cols) "?")) ")"
         " ON CONFLICT (" (quote-ident schema/pk-column) ") DO "
         (if (seq columns)
           (str "UPDATE SET "
                (str/join ", " (for [{:keys [column]} columns]
                                 (str (quote-ident column) " = EXCLUDED." (quote-ident column)))))
           "NOTHING"))))

(defn upsert-rows!
  "rows: seqs of parameter values, entity id first, then one per column."
  [conn pg-schema table rows]
  (when (seq rows)
    (jdbc/execute-batch! conn (upsert-sql pg-schema table) rows {})))

(defn- delete-by-ids! [conn pg-schema table-name ids]
  (jdbc/execute! conn [(str "DELETE FROM " (qualified pg-schema table-name)
                            " WHERE " (quote-ident schema/pk-column) " = ANY(?)")
                       (long-array ids)]))

(defn delete-rows!
  "Removes entities from a table and all of its join tables."
  [conn pg-schema {:keys [sql-name join-tables]} ids]
  (when (seq ids)
    (doseq [t (cons sql-name (map :join-table join-tables))]
      (delete-by-ids! conn pg-schema t ids))))

(defn replace-join-values!
  "Replaces the values of one cardinality-many attribute for the given
  entities. rows: [entity-id value] pairs."
  [conn pg-schema {:keys [join-table]} ids rows]
  (when (seq ids)
    (delete-by-ids! conn pg-schema join-table ids)
    (when (seq rows)
      (jdbc/execute-batch! conn
                           (str "INSERT INTO " (qualified pg-schema join-table)
                                " (" (quote-ident schema/pk-column) ", " (quote-ident "value") ")"
                                " VALUES (?, ?) ON CONFLICT DO NOTHING")
                           rows {}))))
