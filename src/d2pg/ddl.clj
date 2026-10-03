(ns d2pg.ddl
  "Pure generation of idempotent DDL from a resolved model. Every statement
  can be re-run safely, so the same statements both create a fresh target and
  evolve an existing one additively (new attributes -> new columns)."
  (:require [clojure.string :as str]
            [d2pg.schema :as schema]))

(defn quote-ident [s]
  (str \" (str/replace s "\"" "\"\"") \"))

(defn qualified
  "Schema-qualified, quoted table name."
  [pg-schema table]
  (str (quote-ident pg-schema) "." (quote-ident table)))

(def checkpoint-table "d2pg_checkpoint")

(defn- unique-constraint
  "Adds a deferred unique constraint unless it already exists. Deferred so
  that values moving between entities inside one transaction don't collide
  mid-apply."
  [pg-schema table column]
  (let [constraint (str table "_" column "_key")]
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

(defn checkpoint-statements [pg-schema]
  [(str "CREATE SCHEMA IF NOT EXISTS " (quote-ident pg-schema))
   (str "CREATE TABLE IF NOT EXISTS " (qualified pg-schema checkpoint-table)
        " (replicator_id text PRIMARY KEY,"
        " basis_t bigint NOT NULL,"
        " mapping_hash text NOT NULL,"
        " updated_at timestamptz NOT NULL DEFAULT now())")])

(defn statements
  "All DDL needed for model, in order."
  [{:keys [pg-schema tables]}]
  (into (checkpoint-statements pg-schema)
        (mapcat #(table-statements pg-schema %))
        (vals tables)))

(defn drop-statements
  "Drops every table the model maps (used before a full re-snapshot)."
  [{:keys [pg-schema tables]}]
  (for [{:keys [sql-name join-tables]} (vals tables)
        t (cons sql-name (map :join-table join-tables))]
    (str "DROP TABLE IF EXISTS " (qualified pg-schema t))))
