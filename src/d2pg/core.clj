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
           (java.util HexFormat)))

(defn mapping-hash
  "Identifies the parts of config that determine table shape and content.
  When it differs from the checkpoint's, the target is rebuilt by snapshot.
  Maps are printed in sorted key order so equal configs hash equally
  regardless of how they were built."
  [config]
  (let [canonical (walk/postwalk #(if (map? %) (into (sorted-map) %) %)
                                 (select-keys config [:tables :pg-schema]))
        digest (.digest (MessageDigest/getInstance "SHA-256")
                        (.getBytes (pr-str canonical) StandardCharsets/UTF_8))]
    (.formatHex (HexFormat/of) digest)))

(defn replicator
  "Connects to both databases and brings PostgreSQL to a consistent
  checkpoint: a full snapshot if this replicator has never run, its mapping
  changed, or any of its tables is missing; otherwise additive DDL only.
  Returns a replicator for step!, catch-up! and start!."
  [config]
  (let [{:keys [datomic postgres pg-schema replicator-id] :as config} (config/validate config)
        client (d/client (:client datomic))
        datomic-conn (d/connect client {:db-name (:db-name datomic)})
        ds (pg/datasource postgres)
        model (schema/resolve-model config (schema/read-schema (d/db datomic-conn)))
        hash (mapping-hash config)
        checkpoint (do (pg/execute-all! ds (ddl/checkpoint-statements pg-schema))
                       (pg/read-checkpoint ds pg-schema replicator-id))
        missing (when checkpoint (pg/missing-tables ds pg-schema (ddl/table-names model)))
        reason (cond
                 (nil? checkpoint) "No checkpoint;"
                 (not= hash (:mapping-hash checkpoint)) "Mapping changed;"
                 (seq missing) (str "Tables " (vec missing) " are missing;"))
        t (if reason
            (do (log/info reason "taking a full snapshot for replicator" replicator-id)
                (snapshot/snapshot! datomic-conn ds model config hash (:tables checkpoint)))
            (do (log/info "Resuming replicator" replicator-id "from t" (:basis-t checkpoint))
                (jdbc/with-transaction [tx ds]
                  (pg/execute-all! tx (ddl/statements model)))
                (:basis-t checkpoint)))]
    {:config config
     :client client
     :datomic-conn datomic-conn
     :ds ds
     :mapping-hash hash
     :state (atom {:model model :t t})}))

(defn step!
  "Applies the next batch of transactions. Returns how many were applied."
  [rep]
  (tail/step! rep))

(defn catch-up!
  "Steps until no transactions remain. Returns the total applied."
  [rep]
  (loop [total 0]
    (let [n (step! rep)]
      (if (zero? n) total (recur (+ total n))))))

(defn basis-t
  "The last Datomic t replicated."
  [rep]
  (:t @(:state rep)))

(defn last-error
  "The exception from the most recent failed background step, or nil once a
  step has succeeded since."
  [rep]
  (:error @(:state rep)))

(defn start!
  "Creates a replicator and tails the log on a background thread, polling
  every :poll-interval-ms once caught up. A failed step is retried with
  exponential backoff up to :max-backoff-ms; see last-error. Returns the
  replicator; pass it to stop!."
  [config]
  (let [rep (replicator config)
        {:keys [poll-interval-ms max-backoff-ms]} (:config rep)
        state (:state rep)
        stopped (promise)
        thread (Thread.
                ^Runnable
                (fn []
                  (loop [backoff-ms nil]
                    (when-not (realized? stopped)
                      (let [applied (try
                                      (let [n (step! rep)]
                                        (swap! state dissoc :error)
                                        n)
                                      (catch Throwable e
                                        (swap! state assoc :error e)
                                        nil))]
                        (cond
                          (nil? applied)
                          (let [wait (if backoff-ms
                                       (min max-backoff-ms (* 2 backoff-ms))
                                       poll-interval-ms)]
                            (log/error (:error @state) "Replication step failed; retrying in" wait "ms")
                            (deref stopped wait nil)
                            (recur wait))

                          (zero? applied)
                          (do (deref stopped poll-interval-ms nil)
                              (recur nil))

                          :else (recur nil))))))
                "d2pg-replicator")]
    (.start thread)
    (assoc rep :stopped stopped :thread thread)))

(defn stop!
  "Stops a replicator started with start!, waiting for the in-flight step."
  [{:keys [stopped ^Thread thread]}]
  (deliver stopped true)
  (.join thread))
