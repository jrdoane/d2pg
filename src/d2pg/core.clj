(ns d2pg.core
  "Public API: build a replicator from config, step it by hand, or run it in
  the background."
  (:require [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [d2pg.config :as config]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.snapshot :as snapshot]
            [d2pg.tail :as tail]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc])
  (:import (java.nio.charset StandardCharsets)
           (java.security MessageDigest)
           (java.time Instant)
           (java.util HexFormat)))

(def format-version
  "Bump when d2pg changes the tables it derives from an unchanged mapping.
  It is part of the mapping hash, so existing targets are rebuilt."
  1)

(defn mapping-hash
  "Identifies the parts of config that determine table shape and content.
  When it differs from the checkpoint's, the target is rebuilt by snapshot.
  Maps are printed in sorted key order so equal configs hash equally
  regardless of how they were built."
  [config]
  (let [canonical (walk/postwalk #(if (map? %) (into (sorted-map) %) %)
                                 (assoc (select-keys config [:tables :pg-schema])
                                        :format-version format-version))
        digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str canonical) StandardCharsets/UTF_8))]
    (.formatHex (HexFormat/of) digest)))

(defn- source-id
  "Identifies the Datomic database db belongs to, where the client says."
  [db]
  (some-> (or (:database-id db) (:id db)) str))

(defn- check-pg-types! [ds config]
  (when-let [unknown (seq (pg/unknown-types ds (distinct (for [table (vals (:tables config))
                                                                override (vals (:columns table))
                                                                :when (:pg-type override)]
                                                            (:pg-type override)))))]
    (throw (ex-info (str "Unknown PostgreSQL types in :pg-type overrides: " (vec unknown))
                    {:type ::unknown-pg-types :types (vec unknown)}))))

(defn- rebuild-hint [pg-schema replicator-id]
  (str "UPDATE " (ddl/qualified pg-schema ddl/checkpoint-table)
       " SET mapping_hash = '' WHERE replicator_id = '" replicator-id "'"))

(defn- connect
  "Connects to both databases and brings PostgreSQL to a consistent
  checkpoint, recording the model and t in state."
  [config state]
  (let [{:keys [datomic postgres pg-schema replicator-id]} config
        client (d/client (:client datomic))
        datomic-conn (d/connect client {:db-name (:db-name datomic)})
        ds (pg/datasource postgres)
        _ (check-pg-types! ds config)
        ;; One db value for the model and any snapshot, so the snapshot
        ;; can't hold attributes the model lacks.
        db (d/db datomic-conn)
        model (schema/resolve-model config (schema/read-schema db))
        shape (ddl/shape model)
        hash (mapping-hash config)
        source (source-id db)
        checkpoint (do (pg/execute-all! ds (ddl/checkpoint-statements pg-schema))
                       (pg/read-checkpoint ds pg-schema replicator-id))
        missing (when checkpoint (pg/missing-tables ds pg-schema (ddl/table-names model)))
        reason (cond
                 (nil? checkpoint) "No checkpoint;"
                 (not= hash (:mapping-hash checkpoint)) "Mapping changed;"
                 (nil? (:shape checkpoint)) "No recorded table shape;"
                 (not (ddl/additive? (:shape checkpoint) shape)) "Schema changed in a way columns can't follow;"
                 (seq missing) (str "Tables " (vec missing) " are missing;"))
        _ (when (and (not reason) source (:source-id checkpoint)
                     (not= source (:source-id checkpoint)))
            (throw (ex-info (str "Replicator " replicator-id " was replicating Datomic database "
                                 (:source-id checkpoint) " but is connected to " source
                                 "; to rebuild from the new database, run: "
                                 (rebuild-hint pg-schema replicator-id))
                            {:type ::source-changed})))
        checkpoint-fields {:mapping-hash hash :source-id source}
        t (if reason
            (do (log/info reason "taking a full snapshot for replicator" replicator-id)
                (snapshot/snapshot! ds db model config checkpoint-fields))
            (let [t (:basis-t checkpoint)]
              (log/info "Resuming replicator" replicator-id "from t" t)
              (when (not= shape (:shape checkpoint))
                (log/info "Adding columns and tables for attributes installed since the last run")
                (jdbc/with-transaction [tx ds]
                  (pg/execute-all! tx (ddl/evolve-statements model (:shape checkpoint)))
                  (pg/write-checkpoint! tx pg-schema replicator-id
                                        (assoc checkpoint-fields :basis-t t :shape shape))))
              t))]
    (swap! state assoc :model model :t t)
    {:config config
     :client client
     :datomic-conn datomic-conn
     :ds ds
     :mapping-hash hash
     :source-id source
     :state state}))

(defn replicator
  "Connects to both databases and brings PostgreSQL to a consistent
  checkpoint: a full snapshot if this replicator has never run, its mapping
  changed, its tables can't follow a schema change by adding columns, or any
  of its tables is missing; otherwise additive DDL only. Returns a replicator
  for step! and catch-up!."
  [config]
  (connect (config/validate config) (atom {})))

(defn step!
  "Applies the next batch of transactions. Returns how many were applied."
  [rep]
  (let [n (tail/step! rep)]
    (swap! (:state rep) #(-> %
                             (assoc :last-step-at (Instant/now) :caught-up? (zero? n))
                             (update :txs-applied (fnil + 0) n)))
    n))

(defn catch-up!
  "Steps until no transactions remain. Returns the total applied."
  [rep]
  (loop [total 0]
    (let [n (step! rep)]
      (if (zero? n) total (recur (+ total n))))))

(defn basis-t
  "The last Datomic t replicated, or nil before the first snapshot or resume."
  [rep]
  (:t @(:state rep)))

(defn last-error
  "The exception from the most recent failed background step, or nil once a
  step has succeeded since."
  [rep]
  (:error @(:state rep)))

(defn status
  "A summary for monitoring: :basis-t, :caught-up? (the last step found
  nothing to apply), :txs-applied by this process, :last-step-at (the last
  successful step) and :error (see last-error)."
  [rep]
  (let [{:keys [t] :as state} @(:state rep)]
    (assoc (select-keys state [:caught-up? :txs-applied :last-step-at :error])
           :basis-t t)))

(defn- retry-delay
  "The wait after a failure: :poll-interval-ms at first, then doubling, capped
  at :max-backoff-ms, with ±20% jitter so instances don't retry in step."
  [previous {:keys [poll-interval-ms max-backoff-ms]}]
  (let [base (min max-backoff-ms (if previous (* 2 previous) poll-interval-ms))]
    [base (long (* base (+ 0.8 (rand 0.4))))]))

(defn start!
  "Tails the log on a background thread, polling every :poll-interval-ms
  once caught up. Connecting and the initial snapshot also happen there, so
  this returns at once. A failure, there or in a step, is retried with
  exponential backoff up to :max-backoff-ms; see last-error and status.
  Returns a handle for basis-t, last-error, status and stop!."
  [config]
  (let [config (config/validate config)
        state (atom {})
        stopped (promise)
        attempt (fn [f]
                  (try (let [v (f)] (swap! state dissoc :error) v)
                       (catch Throwable e (swap! state assoc :error e) nil)))
        thread (Thread.
                ^Runnable
                (fn []
                  (loop [rep nil, backoff-ms nil]
                    (when-not (realized? stopped)
                      (let [rep (or rep (attempt #(connect config state)))
                            applied (when rep (attempt #(step! rep)))]
                        (cond
                          (nil? applied)
                          (let [[base wait] (retry-delay backoff-ms config)
                                e (:error @state)]
                            ;; The stack trace once per run of failures.
                            (if backoff-ms
                              (log/warn "Replication still failing:" (ex-message e) "; retrying in" wait "ms")
                              (log/error e "Replication failed; retrying in" wait "ms"))
                            (deref stopped wait nil)
                            (recur rep base))

                          (zero? applied)
                          (do (deref stopped (:poll-interval-ms config) nil)
                              (recur rep nil))

                          :else (recur rep nil))))))
                "d2pg-replicator")]
    (.start thread)
    {:config config :state state :stopped stopped :thread thread}))

(defn stop!
  "Stops a replicator started with start!, waiting for the in-flight step."
  [{:keys [stopped ^Thread thread]}]
  (deliver stopped true)
  (.join thread))
