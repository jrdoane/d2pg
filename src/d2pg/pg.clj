(ns d2pg.pg
  "PostgreSQL side: applying DDL, writing rows, and the checkpoint."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [d2pg.ddl :as ddl :refer [qualified quote-ident]]
            [d2pg.schema :as schema]
            [d2pg.types :as types]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs])
  (:import (org.postgresql.util PGobject PSQLException)))

(defn datasource
  "A datasource for the :postgres config map. Multi-row batch inserts are
  enabled unless the config says otherwise; without them the driver sends
  one INSERT per row."
  [postgres]
  (jdbc/get-datasource (merge {:reWriteBatchedInserts true} postgres)))

(defn execute-all! [conn statements]
  (doseq [s statements]
    (jdbc/execute! conn [s])))

(defn with-advisory-lock
  "Calls f holding a session-level advisory lock on key over conn, waiting
  while another session holds it."
  [conn key f]
  (jdbc/execute! conn ["SELECT pg_advisory_lock(hashtext(?))" key])
  (try
    (f)
    (finally
      ;; Closing the session releases the lock too, so a failure here is moot.
      (try (jdbc/execute! conn ["SELECT pg_advisory_unlock(hashtext(?))" key])
           (catch Exception _)))))

(defn- read-json [^PGobject o]
  (some-> o .getValue json/read-str))

;; ---------------------------------------------------------------------------
;; Checkpoint

(defn read-checkpoint
  "Returns {:basis-t :mapping-hash :source-id :tables :shape} for
  replicator-id, or nil if this replicator has never completed a snapshot.
  :tables lists the tables the last snapshot created and :shape their layout
  (see ddl/shape), nil if never recorded. With :lock?, the row stays locked
  until the transaction ends."
  [conn pg-schema replicator-id & {:keys [lock?]}]
  (when-let [row (jdbc/execute-one!
                  conn
                  [(str "SELECT basis_t, mapping_hash, source_id, tables, shape FROM "
                        (qualified pg-schema ddl/checkpoint-table)
                        " WHERE replicator_id = ?"
                        (when lock? " FOR UPDATE"))
                   replicator-id]
                  {:builder-fn rs/as-unqualified-maps})]
    {:basis-t (:basis_t row)
     :mapping-hash (:mapping_hash row)
     :source-id (:source_id row)
     :tables (vec (read-json (:tables row)))
     :shape (read-json (:shape row))}))

(defn write-checkpoint!
  "Records the applied t. :tables and :shape are written when given (after a
  snapshot or a schema change); otherwise the recorded ones are kept."
  [conn pg-schema replicator-id {:keys [basis-t mapping-hash source-id tables shape]}]
  (let [tables (some-> tables types/jsonb)
        shape (some-> shape types/jsonb)
        current (quote-ident ddl/checkpoint-table)]
    (jdbc/execute! conn
                   [(str "INSERT INTO " (qualified pg-schema ddl/checkpoint-table)
                         " (replicator_id, basis_t, mapping_hash, source_id, tables, shape, updated_at)"
                         " VALUES (?, ?, ?, ?, COALESCE(CAST(? AS jsonb), '[]'::jsonb), CAST(? AS jsonb), now())"
                         " ON CONFLICT (replicator_id) DO UPDATE SET"
                         " basis_t = EXCLUDED.basis_t,"
                         " mapping_hash = EXCLUDED.mapping_hash,"
                         " source_id = EXCLUDED.source_id,"
                         " tables = COALESCE(CAST(? AS jsonb), " current ".tables),"
                         " shape = COALESCE(CAST(? AS jsonb), " current ".shape),"
                         " updated_at = EXCLUDED.updated_at")
                    replicator-id basis-t mapping-hash source-id tables shape tables shape])))

(defn existing-tables
  "Which of table-names exist in pg-schema."
  [conn pg-schema table-names]
  (into #{}
        (map :table_name)
        (jdbc/execute! conn
                       [(str "SELECT table_name FROM information_schema.tables"
                             " WHERE table_schema = ? AND table_name = ANY(?)")
                        pg-schema (into-array String table-names)]
                       {:builder-fn rs/as-unqualified-maps})))

(defn missing-tables
  "Which of table-names do not exist in pg-schema."
  [conn pg-schema table-names]
  (remove (existing-tables conn pg-schema table-names) table-names))

(defn unknown-types
  "Which of the type names do not name a PostgreSQL type."
  [conn type-names]
  (remove (fn [t]
            (try (:ok (jdbc/execute-one! conn ["SELECT to_regtype(?) IS NOT NULL AS ok" t]
                                         {:builder-fn rs/as-unqualified-maps}))
                 (catch PSQLException _ false)))
          type-names))

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

(defn insert-join-values!
  "Adds values of one cardinality-many attribute. rows: [entity-id value]
  pairs."
  [conn pg-schema {:keys [join-table pg-type]} rows]
  (when (seq rows)
    (jdbc/execute-batch! conn
                         (str "INSERT INTO " (qualified pg-schema join-table)
                              " (" (quote-ident schema/pk-column) ", " (quote-ident "value") ")"
                              " VALUES (?, " (cast-param pg-type) ") ON CONFLICT DO NOTHING")
                         rows {})))

(defn replace-join-values!
  "Replaces the values of one cardinality-many attribute for the given
  entities. rows: [entity-id value] pairs."
  [conn pg-schema join-table ids rows]
  (when (seq ids)
    (delete-by-ids! conn pg-schema (:join-table join-table) ids)
    (insert-join-values! conn pg-schema join-table rows)))
