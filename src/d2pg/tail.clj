(ns d2pg.tail
  "Incremental replication by tailing the Datomic transaction log."
  (:require [clojure.tools.logging :as log]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.snapshot :as snapshot]
            [d2pg.sync :as sync]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc]))

(def ^:private schema-attrs
  "Attributes whose assertion may install or change an attribute definition.
  :db/ident is handled apart: it is asserted for every enum value, and an
  attribute installation always carries :db/valueType as well."
  #{:db/valueType :db/cardinality :db/unique :db.install/attribute})

(defn- schema-change? [model datoms]
  (let [id->ident (get-in model [:schema :id->ident])]
    (some (fn [{:keys [e a]}]
            (let [attr (id->ident a)]
              (or (schema-attrs attr)
                  ;; An existing attribute renamed.
                  (and (= :db/ident attr) (contains? id->ident e)))))
          datoms)))

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

(defn- ident-referrers
  "Entities whose :as :ident columns render an entity (an enum value) whose
  :db/ident changed in datoms, grouped by table: {table-key #{eid ...}}."
  [db {:keys [schema tables]} datoms]
  (let [id->ident (:id->ident schema)
        renamed (into #{}
                      (comp (filter #(= :db/ident (id->ident (:a %))))
                            (map :e)
                            (remove id->ident))
                      datoms)]
    (apply merge-with into {}
           (for [e renamed
                 [table-key {:keys [columns join-tables]}] tables
                 {:keys [attr as]} (concat columns join-tables)
                 :when (= :ident as)]
             {table-key (into #{} (map :e) (d/datoms db {:index :vaet :components [e attr] :limit -1}))}))))

(defn- verify-checkpoint!
  "Fails unless the checkpoint is where this replicator left it, locking it
  for the rest of the transaction. If it moved, another instance with the
  same :replicator-id is running."
  [tx {:keys [pg-schema replicator-id]} t mapping-hash]
  (let [checkpoint (pg/read-checkpoint tx pg-schema replicator-id :lock? true)]
    (when-not (and (= t (:basis-t checkpoint)) (= mapping-hash (:mapping-hash checkpoint)))
      (throw (ex-info (str "The checkpoint of replicator " replicator-id " is at t "
                           (:basis-t checkpoint) ", not at t " t " where this process left it;"
                           " is another instance with the same :replicator-id running?")
                      {:type ::checkpoint-moved :expected-t t :checkpoint checkpoint})))))

(defn step!
  "Applies up to :batch-txs transactions after the last applied t, in one
  PostgreSQL transaction together with the new checkpoint. Touched entities
  are re-pulled from the db as of the last tx in the batch, so the result is
  exactly that db's current state for those entities. A schema change that
  cannot be applied by adding columns and tables takes a full snapshot
  instead. Returns the number of transactions applied (0 when caught up)."
  [{:keys [datomic-conn ds config state mapping-hash source-id]}]
  (let [{:keys [model t]} @state
        {:keys [pg-schema replicator-id batch-txs pull-batch-size]} config
        txs (vec (d/tx-range datomic-conn {:start (inc t) :limit batch-txs}))]
    (if (empty? txs)
      0
      (let [last-t (:t (peek txs))
            latest-db (d/sync datomic-conn last-t)
            db (d/as-of latest-db last-t)
            datoms (mapcat :data txs)
            ;; Schema is read from the latest db, not as-of last-t: it is a
            ;; superset, so explicitly mapped attrs installed later in the log
            ;; still resolve (their columns are simply created early).
            new-model (if (schema-change? model datoms)
                        (schema/resolve-model config (schema/read-schema latest-db))
                        model)
            before (ddl/shape model)
            after (ddl/shape new-model)
            checkpoint {:mapping-hash mapping-hash :source-id source-id}]
        (if-not (ddl/additive? before after)
          (do (log/warn "Schema change by t" last-t "cannot be applied in place; taking a full snapshot")
              (swap! state assoc
                     :model new-model
                     :t (snapshot/snapshot! ds latest-db new-model config checkpoint)))
          (do (jdbc/with-transaction [tx ds]
                (verify-checkpoint! tx config t mapping-hash)
                (when (not= before after)
                  (log/info "Schema changed by t" last-t "; adding columns and tables")
                  (pg/execute-all! tx (ddl/evolve-statements new-model before)))
                (doseq [[table-key eids] (merge-with into
                                                     (touched new-model datoms)
                                                     (ident-referrers db new-model datoms))]
                  (sync/sync-entities! tx db new-model table-key eids pull-batch-size))
                (pg/write-checkpoint! tx pg-schema replicator-id
                                      (assoc checkpoint
                                             :basis-t last-t
                                             :shape (when (not= before after) after))))
              (swap! state assoc :model new-model :t last-t)))
        (log/debug "Applied" (count txs) "transactions through t" last-t)
        (count txs)))))
