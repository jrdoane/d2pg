(ns d2pg.ddl
  "Pure generation of idempotent DDL from a resolved model. Every statement
  can be re-run safely, so the same statements both create a fresh target and
  evolve an existing one additively (new attributes -> new columns)."
  (:require [clojure.string :as str]
            [d2pg.config :as config]
            [d2pg.schema :as schema]))

(defn quote-ident [s]
  (str \" (str/replace s "\"" "\"\"") \"))

(defn qualified
  "Schema-qualified, quoted table name."
  [pg-schema table]
  (str (quote-ident pg-schema) "." (quote-ident table)))

(def checkpoint-table "d2pg_checkpoint")

(defn table-names
  "Every table the model owns: the mapped tables and their join tables."
  [{:keys [tables]}]
  (vec (for [{:keys [sql-name join-tables]} (vals tables)
             t (cons sql-name (map :join-table join-tables))]
         t)))

(defn- constraint-name
  "table_column_key, or when that is too long for an identifier, a prefix of
  it plus a hash of the whole. PostgreSQL would truncate it instead, which
  can give two constraints one name, and the second would never be added."
  [table column]
  (let [full (str table "_" column "_key")
        suffix (format "_%08x" (.hashCode full))]
    (if (config/pg-identifier? full)
      full
      (loop [prefix (subs full 0 (- config/max-identifier-bytes (count suffix)))]
        (let [n (str prefix suffix)]
          (if (config/pg-identifier? n) n (recur (subs prefix 0 (dec (count prefix))))))))))

(defn- unique-constraint
  "Adds a deferred unique constraint unless it already exists. Deferred so
  that values moving between entities inside one transaction don't collide
  mid-apply."
  [pg-schema table column]
  (let [constraint (constraint-name table column)]
    (str "DO $$ BEGIN "
         "ALTER TABLE " (qualified pg-schema table)
         " ADD CONSTRAINT " (quote-ident constraint)
         " UNIQUE (" (quote-ident column) ") DEFERRABLE INITIALLY DEFERRED; "
         "EXCEPTION WHEN duplicate_table OR duplicate_object THEN NULL; END $$")))

(defn- table-statements [pg-schema {:keys [sql-name columns join-tables]}]
  (concat
   [(str "CREATE TABLE IF NOT EXISTS " (qualified pg-schema sql-name)
         " (" (quote-ident schema/pk-column) " bigint PRIMARY KEY)")]
   (for [{:keys [column pg-type]} columns]
     (str "ALTER TABLE " (qualified pg-schema sql-name)
          " ADD COLUMN IF NOT EXISTS " (quote-ident column) " " pg-type))
   (for [{:keys [column unique?]} columns
         :when unique?]
     (unique-constraint pg-schema sql-name column))
   (for [{:keys [join-table pg-type]} join-tables]
     (str "CREATE TABLE IF NOT EXISTS " (qualified pg-schema join-table)
          " (" (quote-ident schema/pk-column) " bigint NOT NULL, "
          (quote-ident "value") " " pg-type " NOT NULL, "
          "PRIMARY KEY (" (quote-ident schema/pk-column) ", " (quote-ident "value") "))"))))

(defn checkpoint-statements
  "Creates the target schema and checkpoint table, and migrates older
  checkpoint tables forward."
  [pg-schema]
  (let [checkpoint (qualified pg-schema checkpoint-table)]
    [(str "CREATE SCHEMA IF NOT EXISTS " (quote-ident pg-schema))
     (str "CREATE TABLE IF NOT EXISTS " checkpoint
          " (replicator_id text PRIMARY KEY,"
          " basis_t bigint NOT NULL,"
          " mapping_hash text NOT NULL,"
          " tables jsonb NOT NULL DEFAULT '[]',"
          " updated_at timestamptz NOT NULL DEFAULT now())")
     (str "ALTER TABLE " checkpoint
          " ADD COLUMN IF NOT EXISTS tables jsonb NOT NULL DEFAULT '[]'")]))

(defn statements
  "All table DDL for model, in order. The checkpoint table is separate; see
  checkpoint-statements."
  [{:keys [pg-schema tables]}]
  (vec (mapcat #(table-statements pg-schema %) (vals tables))))

(defn drop-statements
  "Drops the named tables in pg-schema (used before a full re-snapshot)."
  [pg-schema table-names]
  (for [t table-names]
    (str "DROP TABLE IF EXISTS " (qualified pg-schema t))))

(defn schema-statements
  "Creates an empty schema, replacing any leftover one of the same name."
  [pg-schema]
  [(str "DROP SCHEMA IF EXISTS " (quote-ident pg-schema) " CASCADE")
   (str "CREATE SCHEMA " (quote-ident pg-schema))])

(defn move-statements
  "Moves the named tables from one schema to another. Indexes and
  constraints travel with their tables and keep their names."
  [from-schema to-schema table-names]
  (concat
   (for [t table-names]
     (str "ALTER TABLE " (qualified from-schema t) " SET SCHEMA " (quote-ident to-schema)))
   [(str "DROP SCHEMA " (quote-ident from-schema))]))
