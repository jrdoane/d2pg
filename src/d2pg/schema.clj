(ns d2pg.schema
  "Reads the attribute schema from a Datomic db and resolves the user's table
  mapping against it into a model: the concrete tables, columns, join tables
  and PostgreSQL types that replication works from."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [d2pg.config :as config]
            [d2pg.types :as types]
            [datomic.client.api :as d]))

;; ---------------------------------------------------------------------------
;; Reading the Datomic schema

(def ^:private attr-pattern
  [:db/id :db/ident
   {:db/valueType [:db/ident]}
   {:db/cardinality [:db/ident]}
   {:db/unique [:db/ident]}])

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
                           :unique (get-in a [:db/unique :db/ident])})))]
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
  (let [{:keys [id value-type cardinality unique]} (get-in schema [:attrs attr])
        {:keys [as] :as override} (get overrides attr)
        _ (when (and as (not= :db.type/ref value-type))
            (fail (str "Column override :as " as " on " attr
                       " requires a ref attribute, but it is " value-type)
                  {:table table-key :attr attr}))
        column (or (:name override) (column-name table-key attr))
        many? (= :db.cardinality/many cardinality)]
    {:attr attr
     :id id
     :column column
     :value-type value-type
     :as as
     :pg-type (or (:pg-type override) (types/pg-type value-type as))
     :many? many?
     :unique? (and (not many?) (some? unique))
     :join-table (when many? (str table-sql-name "_" column))}))

(defn- pull-pattern [columns]
  (into [:db/id]
        (map (fn [{:keys [attr value-type as]}]
               (cond
                 (= :ident as) {attr [:db/ident]}
                 (= :db.type/ref value-type) {attr [:db/id]}
                 :else attr)))
        columns))

(def ^:private too-long
  (str " exceeds PostgreSQL's " config/max-identifier-bytes "-byte identifier limit"))

(defn- resolve-table
  "Resolves a table's name and its candidate columns. Which candidates it
  keeps is decided across all tables; see select-columns."
  [schema [table-key {:keys [attrs columns require] :as table}]]
  (let [table-name (or (:name table) (sql-name (table-ns table-key)))
        expanded (expand-attrs table-key table schema)
        mapped (set expanded)
        named (set (concat (remove config/wildcard? attrs) (keys columns) require))]
    (when (empty? expanded)
      (fail (str "Table " table-key " maps no attributes") {:table table-key}))
    (doseq [attr (concat (keys columns) require)
            :when (not (mapped attr))]
      (fail (str "Table " table-key " refers to " attr
                 " in :columns or :require, but does not map it")
            {:table table-key :attr attr}))
    (when-not (config/pg-identifier? table-name)
      (fail (str "Table name \"" table-name "\" for " table-key too-long
                 "; use :name to shorten it")
            {:table table-key}))
    {:key table-key
     :sql-name table-name
     :require (some-> require set)
     :candidates (mapv #(assoc (resolve-attr table-key table-name columns schema %)
                               :named? (contains? named %))
                       expanded)}))

(defn- conflict
  "Why candidate column c cannot be mapped, given the column names its table
  has taken and the table names taken overall, or nil if it can."
  [{:keys [attr value-type pg-type many? column join-table]} table-key columns table-names]
  (let [n (if many? join-table column)]
    (cond
      (nil? pg-type)
      (str "Unsupported Datomic value type " value-type " for " attr " in table " table-key)

      (not (config/pg-identifier? n))
      (str "Name \"" n "\" for " attr " in table " table-key too-long
           "; use a :name override to shorten it")

      (and many? (table-names n))
      (str "Duplicate table name \"" n "\" for " attr
           "; use a :name override to disambiguate")

      (and (not many?) (columns n))
      (str "Duplicate column \"" n "\" in table " table-key " for " attr
           "; use a :name override to disambiguate"))))

(defn- select-columns
  "Decides which candidate columns every table keeps. Attributes the config
  names (in :attrs, :columns or :require) claim their names first, and any
  conflict among them is an error. Attributes only a wildcard matched follow
  in installation order (attribute eid), and one that cannot be mapped is
  skipped instead: an attribute installed in the source must not stop
  replication, nor take a name from one already replicated.
  Returns {:kept #{[table-key attr]} :skipped [{:table :attr :reason}]}."
  [tables]
  (let [candidates (sort-by (juxt (complement :named?) :id (comp str :attr))
                            (for [{:keys [key candidates]} tables
                                  c candidates]
                              (assoc c :table key)))]
    (-> (reduce (fn [acc {:keys [table attr named? many? column join-table] :as c}]
                  (if-let [reason (conflict c table (get-in acc [:columns table]) (:table-names acc))]
                    (if named?
                      (fail reason {:table table :attr attr})
                      (update acc :skipped conj {:table table :attr attr :reason reason}))
                    (-> (if many?
                          (update acc :table-names conj join-table)
                          (update-in acc [:columns table] conj column))
                        (update :kept conj [table attr]))))
                {:columns (zipmap (map :key tables) (repeat #{pk-column}))
                 :table-names (set (map :sql-name tables))
                 :kept #{}
                 :skipped []}
                candidates)
        (select-keys [:kept :skipped]))))

(defn resolve-model
  "Resolves validated config against a schema (from read-schema) into the
  replication model. :skipped lists wildcard-matched attributes that could
  not be mapped, and why."
  [config schema]
  (let [resolved (mapv #(resolve-table schema %) (:tables config))
        _ (doseq [[n freq] (frequencies (map :sql-name resolved))
                  :when (> freq 1)]
            (fail (str "Duplicate table name \"" n "\"; use a :name override to disambiguate")
                  {:name n}))
        {:keys [kept skipped]} (select-columns resolved)
        tables (mapv (fn [{:keys [key candidates] :as table}]
                       (let [cols (filterv #(kept [key (:attr %)]) candidates)]
                         (-> (dissoc table :candidates)
                             (assoc :attrs (set (map :attr cols))
                                    :columns (filterv (complement :many?) cols)
                                    :join-tables (filterv :many? cols)
                                    :pull-pattern (pull-pattern cols)))))
                     resolved)]
    (doseq [{:keys [reason]} skipped]
      (log/warn "Not replicating an attribute matched by a wildcard:" reason))
    {:pg-schema (:pg-schema config)
     :schema schema
     :tables (into {} (map (juxt :key identity)) tables)
     :skipped skipped
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
