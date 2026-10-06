(ns d2pg.schema-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [d2pg.config :as config]
            [d2pg.core :as core]
            [d2pg.ddl :as ddl]
            [d2pg.schema :as schema]))

(defn- attr [ident value-type & {:keys [many unique id]}]
  [ident {:ident ident
          :id id
          :value-type value-type
          :cardinality (if many :db.cardinality/many :db.cardinality/one)
          :unique unique}])

(def long-attr
  "Its join table name, widget_<62 chars>, exceeds PostgreSQL's 63 bytes."
  :widget/an-extraordinarily-long-attribute-name-that-overflows-postgres)

(defn- fits? [s]
  (<= (count (.getBytes ^String s "UTF-8")) 63))

(def test-schema
  {:attrs (into {} [(attr :person/name :db.type/string)
                    (attr :person/email :db.type/string :unique :db.unique/identity)
                    (attr :person/active? :db.type/boolean)
                    (attr :person/password-hash :db.type/string)
                    (attr :person/status :db.type/ref)
                    (attr :person/tags :db.type/string :many true)
                    (attr :address/street :db.type/string)
                    (attr :address/name :db.type/string)
                    (attr :order/name :db.type/string)
                    ;; Ids give installation order, which settles wildcard conflicts.
                    (attr :widget/name :db.type/string :id 10)
                    (attr :widget/active? :db.type/boolean :id 11)
                    (attr :widget/active :db.type/boolean :id 12)
                    (attr :widget/blob :db.type/fancy :id 13)
                    (attr :widget/parts :db.type/string :many true :id 14)
                    (attr long-attr :db.type/string :many true :id 15)
                    (attr :gizmo/code-a :db.type/string :unique :db.unique/value)
                    (attr :gizmo/code-b :db.type/string :unique :db.unique/value)])})

(defn- model [tables]
  (schema/resolve-model (config/validate {:datomic {:client {} :db-name "x"}
                                          :postgres {}
                                          :tables tables})
                        test-schema))

(deftest wildcard-expansion
  (let [m (model {:person {:attrs [:person/* :address/street]
                           :exclude [:person/password-hash]}})
        person (get-in m [:tables :person])]
    (is (= #{:person/name :person/email :person/active? :person/status
             :person/tags :address/street}
           (:attrs person)))
    (testing "card-one attrs become columns; foreign namespaces are prefixed"
      (is (= ["active" "email" "name" "status" "address_street"]
             (map :column (:columns person)))))
    (testing "card-many attrs become join tables"
      (is (= ["person_tags"] (map :join-table (:join-tables person)))))
    (testing "unique attrs are flagged"
      (is (= ["email"] (map :column (filter :unique? (:columns person))))))
    (is (= {:person/name #{:person} :address/street #{:person}}
           (select-keys (:attr->tables m) [:person/name :address/street])))))

(deftest column-overrides
  (let [person (get-in (model {:person {:attrs [:person/name :person/status]
                                        :columns {:person/name {:name "full_name"}
                                                  :person/status {:as :ident}}}})
                       [:tables :person])]
    (is (= [["full_name" "text"] ["status" "text"]]
           (map (juxt :column :pg-type) (:columns person))))
    (is (= [:db/id :person/name {:person/status [:db/ident]}]
           (:pull-pattern person)))))

(deftest invalid-mappings
  (testing "unknown attribute"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"not an attribute"
                          (model {:person {:attrs [:person/nope]}}))))
  (testing "column collision"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Duplicate column \"name\""
                          (model {:address {:attrs [:address/name :person/name]
                                            :columns {:person/name {:name "name"}}}}))))
  (testing "table / join table collision"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Duplicate table name \"person_tags\""
                          (model {:person {:attrs [:person/tags]}
                                  :other {:attrs [:order/name] :name "person_tags"}}))))
  (testing "require must be mapped"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"does not map"
                          (model {:person {:attrs [:person/name]
                                           :require [:person/email]}}))))
  (testing ":as :ident only on refs"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"requires a ref"
                          (model {:person {:attrs [:person/name]
                                           :columns {:person/name {:as :ident}}}}))))
  (testing "spec failures"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid d2pg config"
                          (config/validate {:tables {}})))))

(deftest member?
  (let [m (model {:person {:attrs [:person/*] :require [:person/email]}
                  :address {:attrs [:address/*]}})]
    (is (schema/member? (get-in m [:tables :person]) {:db/id 1 :person/email "a"}))
    (is (not (schema/member? (get-in m [:tables :person]) {:db/id 1 :person/name "a"})))
    (is (schema/member? (get-in m [:tables :address]) {:db/id 1 :address/name "a"}))
    (is (not (schema/member? (get-in m [:tables :address]) {:db/id 1})))))

(deftest sql-names
  (is (= "is_active" (schema/sql-name "is-active?")))
  (is (= "app_person" (schema/sql-name "app.person"))))

(deftest ddl
  (let [m (model {:person {:attrs [:person/email :person/tags]}})
        stmts (ddl/statements m)]
    (is (some #(str/includes? % "CREATE TABLE IF NOT EXISTS \"public\".\"person\" (\"db_id\" bigint PRIMARY KEY)") stmts))
    (is (some #(str/includes? % "ADD COLUMN IF NOT EXISTS \"email\" text") stmts))
    (is (some #(str/includes? % "UNIQUE (\"email\") DEFERRABLE INITIALLY DEFERRED") stmts))
    (is (some #(str/includes? % "\"person_tags\" (\"db_id\" bigint NOT NULL, \"value\" text NOT NULL") stmts))
    (is (not-any? #(str/includes? % "d2pg_checkpoint") stmts)
        "table DDL leaves the checkpoint table to checkpoint-statements")
    (is (= ["person" "person_tags"] (ddl/table-names m)))
    (is (= ["DROP TABLE IF EXISTS \"public\".\"old\""] (ddl/drop-statements "public" ["old"])))
    (is (= ["ALTER TABLE \"stage\".\"person\" SET SCHEMA \"public\""
            "DROP SCHEMA \"stage\""]
           (ddl/move-statements "stage" "public" ["person"])))))

(deftest mapping-hash
  (let [cfg {:datomic {:client {} :db-name "x"} :postgres {}
             :tables {:person {:attrs [:person/name] :columns {:person/name {:name "n"}}}
                      :address {:attrs [:address/*]}}}]
    (is (= 64 (count (core/mapping-hash cfg))))
    (testing "depends only on the mapping, not on map construction order or other keys"
      (is (= (core/mapping-hash cfg)
             (core/mapping-hash (-> cfg
                                    (assoc :poll-interval-ms 5 :postgres {:jdbcUrl "x"})
                                    (update :tables #(into (array-map) (reverse %))))))))
    (is (not= (core/mapping-hash cfg) (core/mapping-hash (assoc cfg :pg-schema "other"))))
    (is (not= (core/mapping-hash cfg)
              (core/mapping-hash (assoc-in cfg [:tables :person :columns :person/name :name] "m"))))))

(deftest wildcards-skip-attributes-they-cannot-map
  (let [m (model {:widget {:attrs [:widget/*]}
                  :parts {:attrs [:order/name] :name "widget_parts"}})
        widget (get-in m [:tables :widget])]
    (is (= #{:widget/name :widget/active?} (:attrs widget)))
    (testing "the attribute installed first keeps a contested column"
      (is (= [[:widget/active? "active"] [:widget/name "name"]]
             (map (juxt :attr :column) (:columns widget)))))
    (testing "collisions, unsupported types and over-long names are skipped"
      (is (= #{:widget/active :widget/blob :widget/parts long-attr}
             (set (map :attr (:skipped m)))))
      (is (empty? (:join-tables widget)))
      (is (not (contains? (:attr->tables m) :widget/active))))))

(deftest named-attributes-take-precedence-and-fail-loudly
  (testing "an attribute named in :attrs or :require wins a contested column"
    (doseq [table [{:attrs [:widget/* :widget/active]}
                   {:attrs [:widget/*] :require [:widget/active]}]]
      (let [widget (get-in (model {:widget table}) [:tables :widget])]
        (is (= [:widget/active "active"]
               ((juxt :attr :column) (first (filter #(= "active" (:column %)) (:columns widget))))))
        (is (not (contains? (:attrs widget) :widget/active?))))))
  (testing "a named attribute that cannot be mapped is an error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Duplicate column \"active\""
                          (model {:widget {:attrs [:widget/active? :widget/active]}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported Datomic value type"
                          (model {:widget {:attrs [:widget/blob]}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported Datomic value type"
                          (model {:widget {:attrs [:widget/*] :columns {:widget/blob {:name "b"}}}})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"63-byte"
                          (model {:widget {:attrs [long-attr]}})))))

(deftest long-identifiers
  (testing "a :name override shortens a join table name"
    (is (= ["widget_long"]
           (map :join-table (get-in (model {:widget {:attrs [long-attr]
                                                     :columns {long-attr {:name "long"}}}})
                                    [:tables :widget :join-tables])))))
  (testing "a derived table name that is too long is an error"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"63-byte"
                          (model {(keyword (apply str (repeat 64 "t"))) {:attrs [:order/name]}}))))
  (testing "names given in config must fit"
    (let [too-long (apply str (repeat 64 "x"))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid d2pg config"
                            (model {:person {:attrs [:person/name] :name too-long}})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid d2pg config"
                            (model {:person {:attrs [:person/name]
                                             :columns {:person/name {:name too-long}}}})))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid d2pg config"
                            (config/validate {:datomic {:client {} :db-name "x"} :postgres {}
                                              :pg-schema too-long
                                              :tables {:person {:attrs [:person/name]}}})))))
  (testing "unique constraint names are shortened distinctly instead of truncated"
    (let [stmts (ddl/statements (model {:gizmo {:attrs [:gizmo/*]
                                                :name (apply str (repeat 58 "g"))}}))
          names (keep #(second (re-find #"ADD CONSTRAINT \"([^\"]+)\"" %)) stmts)]
      (is (= 2 (count (distinct names))))
      (is (every? fits? names)))))
