(ns d2pg.tail
  "Incremental replication by tailing the Datomic transaction log."
  (:require [clojure.tools.logging :as log]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.sync :as sync]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc]))

(def ^:private schema-attrs
  "Attributes whose assertion may install or change an attribute definition.
  :db/ident is deliberately absent: it is asserted for every enum value, and
  an attribute installation always carries :db/valueType as well."
  #{:db/valueType :db/cardinality :db/unique :db.install/attribute})

(defn- schema-change? [model datoms]
  (let [id->ident (get-in model [:schema :id->ident])]
    (some #(schema-attrs (id->ident (:a %))) datoms)))

(defn touched
  "Groups the entities touched by datoms by the tables they map to:
  {table-key #{eid ...}}."
  [{:keys [schema attr->tables]} datoms]
  (let [id->ident (:id->ident schema)]
    (reduce (fn [acc {:keys [e a]}]
              (reduce #(update %1 %2 (fnil conj #{}) e)
                      acc
                      (attr->tables (id->ident a))))
            {}
            datoms)))

(defn step!
  "Applies up to :batch-txs transactions after the last applied t, in one
  PostgreSQL transaction together with the new checkpoint. Touched entities
  are re-pulled from the db as of the last tx in the batch, so the result is
  exactly that db's current state for those entities. Returns the number of
  transactions applied (0 when caught up)."
  [{:keys [datomic-conn ds config state mapping-hash]}]
  (let [{:keys [model t]} @state
        {:keys [pg-schema replicator-id batch-txs pull-batch-size]} config
        txs (vec (d/tx-range datomic-conn {:start (inc t) :limit batch-txs}))]
    (if (empty? txs)
      0
      (let [last-t (:t (peek txs))
            latest-db (d/sync datomic-conn last-t)
            db (d/as-of latest-db last-t)
            datoms (mapcat :data txs)
            schema-changed? (schema-change? model datoms)
            ;; Schema is read from the latest db, not as-of last-t: it is a
            ;; superset, so explicitly mapped attrs installed later in the log
            ;; still resolve (their columns are simply created early).
            model (if schema-changed?
                    (schema/resolve-model config (schema/read-schema latest-db))
                    model)]
        (jdbc/with-transaction [tx ds]
          (when schema-changed?
            (log/info "Schema changed by t" last-t "; applying additive DDL")
            (pg/execute-all! tx (ddl/statements model)))
          (doseq [[table-key eids] (touched model datoms)]
            (sync/sync-entities! tx db model table-key eids pull-batch-size))
          (pg/write-checkpoint! tx pg-schema replicator-id last-t mapping-hash))
        (swap! state assoc :model model :t last-t)
        (log/debug "Applied" (count txs) "transactions through t" last-t)
        (count txs)))))
