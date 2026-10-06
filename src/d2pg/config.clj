(ns d2pg.config
  "Loading and validating the replication config: the mapping from Datomic
  attributes to PostgreSQL tables, expressed as data."
  (:require [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import (java.nio.charset StandardCharsets)))

(defn wildcard?
  "True for attribute patterns like :person/* that select a whole namespace."
  [k]
  (and (qualified-keyword? k) (= "*" (name k))))

(def max-identifier-bytes
  "PostgreSQL silently truncates longer identifiers (NAMEDATALEN - 1)."
  63)

(defn pg-identifier?
  "Does s fit in a PostgreSQL identifier without being truncated?"
  [s]
  (<= (count (.getBytes ^String s StandardCharsets/UTF_8)) max-identifier-bytes))

(s/def ::attr qualified-keyword?)
(s/def ::non-blank-string (s/and string? (complement str/blank?)))
(s/def ::pg-identifier (s/and ::non-blank-string pg-identifier?))

(s/def :d2pg.column/name ::pg-identifier)
(s/def :d2pg.column/as #{:ident})
(s/def :d2pg.column/pg-type ::non-blank-string)
(s/def ::column-override
  (s/keys :opt-un [:d2pg.column/name :d2pg.column/as :d2pg.column/pg-type]))

(s/def :d2pg.table/attrs (s/coll-of ::attr :kind sequential? :min-count 1))
(s/def :d2pg.table/exclude (s/coll-of ::attr :kind sequential?))
(s/def :d2pg.table/require (s/coll-of ::attr :kind sequential? :min-count 1))
(s/def :d2pg.table/columns (s/map-of ::attr ::column-override))
(s/def :d2pg.table/name ::pg-identifier)
(s/def ::table
  (s/keys :req-un [:d2pg.table/attrs]
          :opt-un [:d2pg.table/exclude :d2pg.table/require
                   :d2pg.table/columns :d2pg.table/name]))

(s/def ::tables (s/map-of keyword? ::table :min-count 1))

(s/def :d2pg.datomic/client map?)
(s/def :d2pg.datomic/db-name ::non-blank-string)
(s/def ::datomic (s/keys :req-un [:d2pg.datomic/client :d2pg.datomic/db-name]))
(s/def ::postgres map?)
(s/def ::pg-schema ::pg-identifier)
;; It names the staging schema, d2pg_staging_<id>, which must fit too.
(s/def ::replicator-id (s/and ::non-blank-string
                              #(pg-identifier? (str "d2pg_staging_" %))))
(s/def ::poll-interval-ms pos-int?)
(s/def ::batch-txs pos-int?)
(s/def ::pull-batch-size pos-int?)
(s/def ::max-backoff-ms pos-int?)

(s/def ::config
  (s/keys :req-un [::datomic ::postgres ::tables]
          :opt-un [::pg-schema ::replicator-id ::poll-interval-ms
                   ::batch-txs ::pull-batch-size ::max-backoff-ms]))

(def defaults
  {:pg-schema "public"
   :replicator-id "default"
   :poll-interval-ms 1000
   :batch-txs 100
   :pull-batch-size 1000
   :max-backoff-ms 60000})

(defn validate
  "Returns config with defaults applied, or throws ex-info describing the
  spec failure."
  [config]
  (if (s/valid? ::config config)
    (merge defaults config)
    (throw (ex-info (str "Invalid d2pg config:\n" (s/explain-str ::config config))
                    {:type ::invalid-config
                     :explain (s/explain-data ::config config)}))))

(defn load-config
  "Reads and validates an EDN config file."
  [path]
  (validate (edn/read-string (slurp path))))
