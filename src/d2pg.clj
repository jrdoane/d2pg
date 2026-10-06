(ns d2pg
  "Command-line entry point: clojure -M:run path/to/config.edn, or java -jar
  the standalone jar from clojure -T:build uber."
  (:require [clojure.tools.logging :as log]
            [d2pg.config :as config]
            [d2pg.core :as core])
  (:gen-class))

(defn -main [& [config-path]]
  (when-not config-path
    (binding [*out* *err*]
      (println "Usage: d2pg <config.edn>  (clojure -M:run <config.edn>, or java -jar the standalone jar)"))
    (System/exit 1))
  (let [rep (core/start! (config/load-config config-path))]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn []
                                           (log/info "Stopping replicator")
                                           (core/stop! rep))))
    (log/info "Replicating; Ctrl-C to stop")
    (.join ^Thread (:thread rep))))
