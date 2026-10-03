(ns d2pg.pg
  "PostgreSQL side: applying DDL, writing rows, and the checkpoint."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [d2pg.ddl :as ddl :refer [qualified quote-ident]]
            [d2pg.schema :as schema]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (org.postgresql.util PGobject)))

(defn datasource
  "A datasource for the :postgres config map. Multi-row batch inserts are
  enabled unless the config says otherwise; without them the driver sends
  one INSERT per row."
  [postgres]
  (jdbc/get-datasource (merge {:reWriteBatchedInserts true} postgres)))

(defn execute-all! [conn statements]
  (doseq [s statements]
    (jdbc/execute! conn [s])))

(defn- jsonb [v]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/write-str v))))

;; ---------------------------------------------------------------------------
;; Checkpoint

(defn read-checkpoint
  "Returns {:basis-t :mapping-hash :tables} for replicator-id, or nil if this
  replicator has never completed a snapshot. :tables lists the tables the
  last snapshot created."
  [conn pg-schema replicator-id]
  (when-let [row (jdbc/execute-one!
                  conn
                  [(str "SELECT basis_t, mapping_hash, tables FROM "
                        (qualified pg-schema ddl/checkpoint-table)
                        " WHERE replicator_id = ?")
                   replicator-id]
                  {:builder-fn rs/as-unqualified-maps})]
    {:basis-t (:basis_t row)
     :mapping-hash (:mapping_hash row)
     :tables (vec (json/read-str (.getValue ^PGobject (:tables row))))}))

(defn write-checkpoint!
  "Records the applied t. Pass table-names after a snapshot to record which
  tables it created; omit it to leave the recorded tables unchanged."
  ([conn pg-schema replicator-id basis-t mapping-hash]
   (write-checkpoint! conn pg-schema replicator-id basis-t mapping-hash nil))
  ([conn pg-schema replicator-id basis-t mapping-hash table-names]
   (jdbc/execute! conn
                  [(str "INSERT INTO " (qualified pg-schema ddl/checkpoint-table)
                        " (replicator_id, basis_t, mapping_hash, tables, updated_at)"
                        " VALUES (?, ?, ?, COALESCE(CAST(? AS jsonb), '[]'::jsonb), now())"
                        " ON CONFLICT (replicator_id) DO UPDATE SET"
                        " basis_t = EXCLUDED.basis_t,"
                        " mapping_hash = EXCLUDED.mapping_hash,"
                        " tables = COALESCE(CAST(? AS jsonb), " (quote-ident ddl/checkpoint-table) ".tables),"
                        " updated_at = EXCLUDED.updated_at")
                   replicator-id basis-t mapping-hash
                   (some-> table-names jsonb) (some-> table-names jsonb)])))

(defn missing-tables
  "Which of table-names do not exist in pg-schema."
  [conn pg-schema table-names]
  (let [present (into #{}
                      (map :table_name)
                      (jdbc/execute! conn
                                     [(str "SELECT table_name FROM information_schema.tables"
                                           " WHERE table_schema = ? AND table_name = ANY(?)")
                                      pg-schema (into-array String table-names)]
                                     {:builder-fn rs/as-unqualified-maps}))]
    (remove present table-names)))

;; ---------------------------------------------------------------------------
;; Rows

(defn- cast-param
  "A placeholder cast to the column's declared type, so :pg-type overrides
  work for any cast PostgreSQL allows from the natural parameter type."
  [pg-type]
  (str "CAST(? AS " pg-type ")"))

(defn- upsert-sql [pg-schema {:keys [sql-name columns]}]
  (let [cols (cons schema/pk-column (map :column columns))]
    (str "INSERT INTO " (qualified pg-schema sql-name)
         " (" (str/join ", " (map quote-ident cols)) ")"
         " VALUES (" (str/join ", " (cons "?" (map (comp cast-param :pg-type) columns))) ")"
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
  [conn pg-schema {:keys [join-table pg-type]} ids rows]
  (when (seq ids)
    (delete-by-ids! conn pg-schema join-table ids)
    (when (seq rows)
      (jdbc/execute-batch! conn
                           (str "INSERT INTO " (qualified pg-schema join-table)
                                " (" (quote-ident schema/pk-column) ", " (quote-ident "value") ")"
                                " VALUES (?, " (cast-param pg-type) ") ON CONFLICT DO NOTHING")
                           rows {}))))
