(ns d2pg.core
  "Public API: build a replicator from config, step it by hand, or run it in
  the background."
  (:require [clojure.tools.logging :as log]
            [d2pg.config :as config]
            [d2pg.ddl :as ddl]
            [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.snapshot :as snapshot]
            [d2pg.tail :as tail]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc]))

(defn mapping-hash
  "Identifies the parts of config that determine table shape and content.
  When it differs from the checkpoint's, the target is rebuilt by snapshot."
  [config]
  (str (hash (select-keys config [:tables :pg-schema]))))

(defn replicator
  "Connects to both databases and brings PostgreSQL to a consistent
  checkpoint: a full snapshot if this replicator has never run or its
  mapping changed, otherwise additive DDL only. Returns a replicator for
  step!, catch-up! and start!."
  [config]
  (let [{:keys [datomic postgres pg-schema replicator-id] :as config} (config/validate config)
        client (d/client (:client datomic))
        datomic-conn (d/connect client {:db-name (:db-name datomic)})
        ds (jdbc/get-datasource postgres)
        model (schema/resolve-model config (schema/read-schema (d/db datomic-conn)))
        hash (mapping-hash config)
        checkpoint (do (pg/execute-all! ds (ddl/checkpoint-statements pg-schema))
                       (pg/read-checkpoint ds pg-schema replicator-id))
        t (if (= hash (:mapping-hash checkpoint))
            (do (log/info "Resuming replicator" replicator-id "from t" (:basis-t checkpoint))
                (jdbc/with-transaction [tx ds]
                  (pg/execute-all! tx (ddl/statements model)))
                (:basis-t checkpoint))
            (do (log/info (if checkpoint "Mapping changed;" "No checkpoint;")
                          "taking a full snapshot for replicator" replicator-id)
                (snapshot/snapshot! datomic-conn ds model config hash)))]
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

(defn start!
  "Creates a replicator and tails the log on a background thread, polling
  every :poll-interval-ms once caught up. Returns the replicator; pass it to
  stop!."
  [config]
  (let [rep (replicator config)
        poll-ms (get-in rep [:config :poll-interval-ms])
        stopped (promise)
        thread (Thread.
                ^Runnable
                (fn []
                  (while (not (realized? stopped))
                    (let [applied (try
                                    (step! rep)
                                    (catch Throwable e
                                      (log/error e "Replication step failed; retrying")
                                      0))]
                      (when (zero? applied)
                        (deref stopped poll-ms nil)))))
                "d2pg-replicator")]
    (.start thread)
    (assoc rep :stopped stopped :thread thread)))

(defn stop!
  "Stops a replicator started with start!, waiting for the in-flight step."
  [{:keys [stopped ^Thread thread]}]
  (deliver stopped true)
  (.join thread))
