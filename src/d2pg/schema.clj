(ns d2pg.schema
  "Reads the attribute schema from a Datomic db and resolves the user's table
  mapping against it into a model: the concrete tables, columns, join tables
  and PostgreSQL types that replication works from."
  (:require [clojure.string :as str]
            [d2pg.config :as config]
            [d2pg.types :as types]
            [datomic.client.api :as d]))

;; ---------------------------------------------------------------------------
;; Reading the Datomic schema

(def ^:private attr-pattern
  [:db/id :db/ident
   {:db/valueType [:db/ident]}
   {:db/cardinality [:db/ident]}
   {:db/unique [:db/ident]}
   :db/isComponent])

(defn read-schema
  "Returns {:attrs {ident attr-info} :id->ident {eid ident}} for every
  attribute installed in db, plus an eid->ident entry for every ident (so
  tx-log datoms, whose attributes are eids, can be resolved)."
  [db]
  (let [attrs (->> (d/q {:query '[:find (pull ?a pattern)
                                  :in $ pattern
                                  :where [?a :db/valueType]]
                         :args [db attr-pattern]})
                   (map first)
                   (map (fn [a]
                          {:ident (:db/ident a)
                           :id (:db/id a)
                           :value-type (get-in a [:db/valueType :db/ident])
                           :cardinality (get-in a [:db/cardinality :db/ident])
                           :unique (get-in a [:db/unique :db/ident])
                           :component? (boolean (:db/isComponent a))})))]
    {:attrs (into {} (map (juxt :ident identity)) attrs)
     :id->ident (into {} (map (juxt :id :ident)) attrs)}))

;; ---------------------------------------------------------------------------
;; Resolving the mapping

(def pk-column
  "Every replicated table (and join table) is keyed by the Datomic entity id."
  "db_id")

(defn sql-name
  "Derives a PostgreSQL identifier from a Datomic name: lower-cased, ? and !
  dropped, everything else non-alphanumeric collapsed to _."
  [s]
  (-> (str/lower-case s)
      (str/replace #"[?!]" "")
      (str/replace #"[^a-z0-9_]+" "_")
      (str/replace #"^_+|_+$" "")))

(defn- table-ns
  "The attribute namespace considered 'home' for a table key: :person -> \"person\"."
  [table-key]
  (subs (str table-key) 1))

(defn- column-name
  "Attributes from the table's own namespace use their bare name; any other
  attribute is prefixed with its namespace to stay unambiguous."
  [table-key attr]
  (sql-name (if (= (namespace attr) (table-ns table-key))
              (name attr)
              (str (namespace attr) "_" (name attr)))))

(defn- fail [msg data]
  (throw (ex-info msg (assoc data :type ::invalid-mapping))))

(defn- expand-attrs
  "Expands :ns/* wildcards against the schema, removes exclusions and returns
  a distinct, ordered seq of attribute idents."
  [table-key {:keys [attrs exclude]} schema]
  (let [by-ns (group-by namespace (sort (keys (:attrs schema))))
        excluded (set exclude)]
    (->> attrs
         (mapcat (fn [pattern]
                   (cond
                     (config/wildcard? pattern)
                     (get by-ns (namespace pattern))

                     (contains? (:attrs schema) pattern)
                     [pattern]

                     :else
                     (fail (str "Table " table-key " maps " pattern
                                ", which is not an attribute in the source database")
                           {:table table-key :attr pattern}))))
         (remove excluded)
         distinct)))

(defn- resolve-attr [table-key table-sql-name overrides schema attr]
  (let [{:keys [value-type cardinality unique]} (get-in schema [:attrs attr])
        {:keys [as] :as override} (get overrides attr)
        _ (when (and as (not= :db.type/ref value-type))
            (fail (str "Column override :as " as " on " attr
                       " requires a ref attribute, but it is " value-type)
                  {:table table-key :attr attr}))
        column (or (:name override) (column-name table-key attr))
        many? (= :db.cardinality/many cardinality)]
    {:attr attr
     :column column
     :value-type value-type
     :as as
     :pg-type (or (:pg-type override) (types/pg-type value-type as))
     :many? many?
     :unique? (and (not many?) (some? unique))
     :join-table (when many? (str table-sql-name "_" column))}))

(defn- check-distinct! [what where names data]
  (doseq [[n freq] (frequencies names)
          :when (> freq 1)]
    (fail (str "Duplicate " what " \"" n "\"" where
               "; use a :name override to disambiguate")
          (assoc data :name n))))

(defn- pull-pattern [columns]
  (into [:db/id]
        (map (fn [{:keys [attr value-type as]}]
               (cond
                 (= :ident as) {attr [:db/ident]}
                 (= :db.type/ref value-type) {attr [:db/id]}
                 :else attr)))
        columns))

(defn- resolve-table [schema [table-key {:keys [columns require] :as table}]]
  (let [table-name (or (:name table) (sql-name (table-ns table-key)))
        attrs (expand-attrs table-key table schema)
        attr-set (set attrs)
        cols (mapv #(resolve-attr table-key table-name columns schema %) attrs)]
    (when (empty? attrs)
      (fail (str "Table " table-key " maps no attributes") {:table table-key}))
    (doseq [attr (concat (keys columns) require)
            :when (not (attr-set attr))]
      (fail (str "Table " table-key " refers to " attr
                 " in :columns or :require, but does not map it")
            {:table table-key :attr attr}))
    (check-distinct! "column" (str " in table " table-key)
                     (cons pk-column (map :column (remove :many? cols)))
                     {:table table-key})
    {:key table-key
     :sql-name table-name
     :attrs attr-set
     :columns (filterv (complement :many?) cols)
     :join-tables (filterv :many? cols)
     :require (some-> require set)
     :pull-pattern (pull-pattern cols)}))

(defn resolve-model
  "Resolves validated config against a schema (from read-schema) into the
  replication model."
  [config schema]
  (let [tables (mapv #(resolve-table schema %) (:tables config))]
    (check-distinct! "table name" ""
                     (concat (map :sql-name tables)
                             (mapcat #(map :join-table (:join-tables %)) tables))
                     {})
    {:pg-schema (:pg-schema config)
     :schema schema
     :tables (into {} (map (juxt :key identity)) tables)
     :attr->tables (reduce (fn [m {:keys [key attrs]}]
                             (reduce #(update %1 %2 (fnil conj #{}) key) m attrs))
                           {}
                           tables)}))

(defn member?
  "Does a pulled entity belong in table? With :require, it must have every
  required attribute; otherwise any mapped attribute will do."
  [{:keys [attrs require]} pulled]
  (if require
    (every? #(contains? pulled %) require)
    (boolean (some #(contains? pulled %) attrs))))
