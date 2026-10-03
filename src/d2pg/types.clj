(ns d2pg.types
  "Mapping Datomic value types to PostgreSQL column types, and Datomic values
  to JDBC parameters."
  (:require [clojure.data.json :as json])
  (:import (java.sql Timestamp)
           (java.util Date)
           (org.postgresql.util PGobject)))

(def value-type->pg-type
  {:db.type/string  "text"
   :db.type/keyword "text"
   :db.type/symbol  "text"
   :db.type/uri     "text"
   :db.type/long    "bigint"
   :db.type/ref     "bigint"
   :db.type/boolean "boolean"
   :db.type/instant "timestamptz"
   :db.type/uuid    "uuid"
   :db.type/double  "double precision"
   :db.type/float   "real"
   :db.type/bigint  "numeric"
   :db.type/bigdec  "numeric"
   :db.type/bytes   "bytea"
   :db.type/tuple   "jsonb"})

(defn pg-type
  "PostgreSQL type for a resolved attribute. `as` is the column's :as
  override (currently only :ident, which renders refs as their ident text)."
  [value-type as]
  (if (= :ident as)
    "text"
    (or (value-type->pg-type value-type)
        (throw (ex-info (str "Unsupported Datomic value type " value-type)
                        {:type ::unsupported-value-type :value-type value-type})))))

(defn keyword->text
  "Renders :ns/name as \"ns/name\"."
  [k]
  (subs (str k) 1))

(defn- json-value [v]
  (cond
    (keyword? v) (keyword->text v)
    (symbol? v) (str v)
    (instance? Date v) (.toString (.toInstant ^Date v))
    (uuid? v) (str v)
    (instance? java.net.URI v) (str v)
    :else v))

(defn- jsonb [v]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/write-str (mapv json-value v)))))

(defn ->jdbc
  "Coerces a pulled Datomic value to something the PostgreSQL driver accepts
  for a column of the given value type."
  [value-type as v]
  (when (some? v)
    (if (= :ident as)
      (some-> (:db/ident v) keyword->text)
      (case value-type
        :db.type/ref     (:db/id v)
        :db.type/keyword (keyword->text v)
        :db.type/symbol  (str v)
        :db.type/uri     (str v)
        :db.type/instant (Timestamp. (.getTime ^Date v))
        :db.type/bigint  (bigdec v)
        :db.type/tuple   (jsonb v)
        v))))
