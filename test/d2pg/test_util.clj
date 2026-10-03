(ns d2pg.test-util
  "Fixtures for integration tests: an in-memory Datomic Local db per test and
  a throwaway schema in a local PostgreSQL database."
  (:require [clojure.string :as str]
            [datomic.client.api :as d]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def pg-url
  (or (System/getenv "D2PG_TEST_JDBC_URL")
      "jdbc:postgresql://localhost:5432/d2pg_test"))

(def pg-schema "d2pg_it")

(defn ensure-database!
  "Creates the test database named in pg-url if it doesn't exist."
  []
  (let [[_ base db-name query] (re-matches #"(.*/)([^/?]+)(\?.*)?" pg-url)
        ds (jdbc/get-datasource {:jdbcUrl (str base "postgres" query)})]
    (when-not (jdbc/execute-one! ds ["SELECT 1 FROM pg_database WHERE datname = ?" db-name])
      (jdbc/execute! ds [(str "CREATE DATABASE \"" db-name "\"")]))))

(def ds (delay (ensure-database!) (jdbc/get-datasource {:jdbcUrl pg-url})))

(defn reset-pg! []
  (jdbc/execute! @ds [(str "DROP SCHEMA IF EXISTS \"" pg-schema "\" CASCADE")]))

(defn query
  "Runs sql against the test database, returning unqualified maps."
  [sql & params]
  (jdbc/execute! @ds (into [(str/replace sql "$s" (str "\"" pg-schema "\""))] params)
                 {:builder-fn rs/as-unqualified-maps}))

(defn datomic-client-config []
  {:server-type :datomic-local
   :system (str "d2pg-test-" (random-uuid))
   :storage-dir :mem})

(defn fresh-datomic
  "Creates an empty in-memory Datomic db with tx-data schema transacted.
  Returns {:client-config :conn}."
  [schema-tx]
  (let [client-config (datomic-client-config)
        client (d/client client-config)]
    (d/create-database client {:db-name "app"})
    (let [conn (d/connect client {:db-name "app"})]
      (d/transact conn {:tx-data schema-tx})
      {:client-config client-config :conn conn})))

(defn config [client-config tables]
  {:datomic {:client client-config :db-name "app"}
   :postgres {:jdbcUrl pg-url}
   :pg-schema pg-schema
   :replicator-id "test"
   :poll-interval-ms 50
   :tables tables})

(defn tx! [conn tx-data]
  (d/transact conn {:tx-data tx-data}))
