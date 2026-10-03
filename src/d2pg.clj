(ns d2pg
  "Command-line entry point: clojure -M -m d2pg path/to/config.edn"
  (:require [clojure.tools.logging :as log]
            [d2pg.config :as config]
            [d2pg.core :as core])
  (:gen-class))

(defn -main [& [config-path]]
  (when-not config-path
    (binding [*out* *err*]
      (println "Usage: clojure -M -m d2pg <config.edn>"))
    (System/exit 1))
  (let [rep (core/start! (config/load-config config-path))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn []
                                           (log/info "Stopping replicator")
                                           (core/stop! rep))))
    (log/info "Replicating; Ctrl-C to stop")
    (.join ^Thread (:thread rep))))
