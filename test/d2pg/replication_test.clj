(ns d2pg.replication-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [d2pg.core :as core]
            [d2pg.test-util :as tu :refer [query tx!]]
            [datomic.client.api :as d])
  (:import (java.net URI)
           (java.util Date UUID)))

(use-fixtures :each (fn [t] (tu/reset-pg!) (t)))

(def schema-tx
  [{:db/ident :status/active}
   {:db/ident :status/inactive}
   {:db/ident :role/admin}
   {:db/ident :role/editor}
   {:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :person/email :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :person/id :db/valueType :db.type/uuid :db/cardinality :db.cardinality/one}
   {:db/ident :person/born :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/ident :person/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :person/score :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/ident :person/ratio :db/valueType :db.type/float :db/cardinality :db.cardinality/one}
   {:db/ident :person/big :db/valueType :db.type/bigint :db/cardinality :db.cardinality/one}
   {:db/ident :person/dec :db/valueType :db.type/bigdec :db/cardinality :db.cardinality/one}
   {:db/ident :person/sym :db/valueType :db.type/symbol :db/cardinality :db.cardinality/one}
   {:db/ident :person/home :db/valueType :db.type/uri :db/cardinality :db.cardinality/one}
   {:db/ident :person/kind :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :person/status :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :person/best-friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :person/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/ident :person/roles :db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   {:db/ident :person/coords :db/valueType :db.type/tuple
    :db/tupleTypes [:db.type/long :db.type/long] :db/cardinality :db.cardinality/one}
   {:db/ident :person/password-hash :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :address/street :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :address/city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def tables
  {:person {:attrs [:person/* :address/street]
            :exclude [:person/password-hash]
            :require [:person/email]
            :columns {:person/status {:as :ident}
                      :person/roles {:as :ident}}}
   :address {:attrs [:address/*]}})

(defn- person [email]
  (first (query "SELECT * FROM $s.person WHERE email = ?" email)))

(defn- tags [db-id]
  (set (map :value (query "SELECT value FROM $s.person_tags WHERE db_id = ?" db-id))))

(defn- eid [conn email]
  (ffirst (d/q '[:find ?e :in $ ?email :where [?e :person/email ?email]]
               (d/db conn) email)))

(defn- table-names []
  (set (map :table_name (query "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
                               tu/pg-schema))))

(defn- schema-names []
  (set (map :schema_name (query "SELECT schema_name FROM information_schema.schemata"))))

(defn- checkpoint-t []
  (:basis_t (first (query "SELECT basis_t FROM $s.d2pg_checkpoint"))))

(defn- wait-until
  "Polls pred every 25 ms for up to 5 s; returns its last value."
  [pred]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (or (pred)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 25)
            (recur))))))

(deftest snapshot-then-tail
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        uuid (UUID/randomUUID)
        born (Date. 0)]
    (tx! conn [{:db/id "ada" :person/name "Ada" :person/email "ada@x.org" :person/id uuid
                :person/born born :person/score 1.5 :person/kind :kind/admin
                :person/status :status/active :person/tags ["math" "engines"]
                :person/coords [1 2] :person/password-hash "secret"
                :address/street "1 Analytical Way" :address/city "London"}
               {:db/id "bob" :person/name "Bob" :person/email "bob@x.org" :person/best-friend "ada"}
               {:address/city "Nowhere"}])
    (let [rep (core/replicator (tu/config client-config tables))
          ada-id (eid conn "ada@x.org")]

      (testing "snapshot derives types and values from the schema"
        (let [ada (person "ada@x.org")]
          (is (= ada-id (:db_id ada)))
          (is (= "Ada" (:name ada)))
          (is (= uuid (:id ada)))
          (is (= 0 (.getTime ^Date (:born ada))))
          (is (= 1.5 (:score ada)))
          (is (= "kind/admin" (:kind ada)))
          (is (= "status/active" (:status ada)))
          (is (= "1 Analytical Way" (:address_street ada)))
          (is (= "[1, 2]" (str (:coords ada))))
          (is (not (contains? ada :password_hash))))
        (is (= ada-id (:best_friend (person "bob@x.org"))))
        (is (= #{"math" "engines"} (tags ada-id)))
        (is (= #{"London" "Nowhere"}
               (set (map :city (query "SELECT city FROM $s.address"))))))

      (testing "the staging schema does not outlive the snapshot"
        (is (not (contains? (schema-names) "d2pg_staging_test"))))

      (testing "tail applies updates, card-many changes and enum changes"
        (tx! conn [[:db/add ada-id :person/name "Ada Lovelace"]
                   [:db/retract ada-id :person/tags "math"]
                   [:db/add ada-id :person/tags "poetry"]
                   [:db/add ada-id :person/status :status/inactive]
                   [:db/retract ada-id :person/score 1.5]])
        (is (= 1 (core/catch-up! rep)))
        (let [ada (person "ada@x.org")]
          (is (= "Ada Lovelace" (:name ada)))
          (is (= "status/inactive" (:status ada)))
          (is (nil? (:score ada))))
        (is (= #{"engines" "poetry"} (tags ada-id))))

      (testing "new entities appear"
        (tx! conn [{:person/email "cy@x.org" :person/tags ["new"]}])
        (core/catch-up! rep)
        (is (= #{"new"} (tags (:db_id (person "cy@x.org"))))))

      (testing ":require membership: losing the required attr removes the row"
        (tx! conn [[:db/retract ada-id :person/email "ada@x.org"]])
        (core/catch-up! rep)
        (is (empty? (query "SELECT * FROM $s.person WHERE db_id = ?" ada-id)))
        (is (empty? (tags ada-id)))
        (testing "but tables it still belongs to keep it"
          (is (= 1 (count (query "SELECT * FROM $s.address WHERE db_id = ?" ada-id))))))

      (testing "retracting an entity removes it, its join rows, and refs to it"
        (let [bob-id (eid conn "bob@x.org")]
          (tx! conn [[:db/add ada-id :person/email "ada@x.org"]])
          (tx! conn [[:db/add bob-id :person/best-friend ada-id]])
          (core/catch-up! rep)
          (is (= #{"engines" "poetry"} (tags ada-id)))
          (tx! conn [[:db/retractEntity ada-id]])
          (core/catch-up! rep)
          (is (empty? (query "SELECT * FROM $s.address WHERE db_id = ?" ada-id)))
          (is (nil? (person "ada@x.org")))
          (is (empty? (tags ada-id)))
          (is (nil? (:best_friend (person "bob@x.org"))))))

      (testing "checkpoint tracks the last applied t"
        (is (= (:t (d/db conn)) (core/basis-t rep)))
        (is (= (:t (d/db conn)) (checkpoint-t)))))))

(deftest value-types
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        big 123456789012345678901234567890N]
    (tx! conn [{:person/email "t@x.org" :person/age 42 :person/ratio (float 2.5)
                :person/big big :person/dec 1.25M
                :person/sym 'foo/bar :person/home (URI. "https://x.org/a?b=c")
                :person/roles [:role/admin :role/editor]}])
    (core/replicator (tu/config client-config tables))
    (let [row (person "t@x.org")]
      (is (= 42 (:age row)))
      (is (= 2.5 (double (:ratio row))))
      (is (= big (bigint (:big row))))
      (is (= 1.25M (:dec row)))
      (is (= "foo/bar" (:sym row)))
      (is (= "https://x.org/a?b=c" (:home row)))
      (is (= #{"role/admin" "role/editor"}
             (set (map :value (query "SELECT value FROM $s.person_roles WHERE db_id = ?" (:db_id row)))))))))

(deftest unique-values-can-move-between-entities-in-one-tx
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)]
    (tx! conn [{:person/email "a@x.org" :person/name "A"}
               {:person/email "b@x.org" :person/name "B"}])
    (let [rep (core/replicator (tu/config client-config tables))
          a (eid conn "a@x.org")
          b (eid conn "b@x.org")]
      (tx! conn [[:db/add a :person/email "tmp@x.org"]])
      (tx! conn [[:db/add b :person/email "a@x.org"]
                 [:db/add a :person/email "b@x.org"]])
      (core/catch-up! rep)
      (is (= "A" (:name (person "b@x.org"))))
      (is (= "B" (:name (person "a@x.org")))))))

(deftest small-batches
  (testing "work is correct when split across several steps and several pulls"
    (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)]
      (tx! conn [{:person/email "a@x.org" :person/tags ["x"]}
                 {:person/email "b@x.org" :person/tags ["y"]}
                 {:person/email "c@x.org" :person/tags ["z"]}])
      (let [rep (core/replicator (assoc (tu/config client-config tables)
                                        :batch-txs 1 :pull-batch-size 1))
            [a b c] (map #(eid conn %) ["a@x.org" "b@x.org" "c@x.org"])]
        (is (= 3 (count (query "SELECT * FROM $s.person"))))
        ;; One tx touching every entity, then two more txs.
        (tx! conn [[:db/add a :person/name "A"]
                   [:db/retract b :person/email "b@x.org"]
                   [:db/add c :person/tags "zz"]])
        (tx! conn [[:db/add a :person/name "AA"]])
        (tx! conn [{:person/email "d@x.org"}])
        (is (= 3 (core/catch-up! rep)))
        (is (= "AA" (:name (person "a@x.org"))))
        (is (empty? (query "SELECT * FROM $s.person WHERE db_id = ?" b)))
        (is (empty? (tags b)))
        (is (= #{"z" "zz"} (tags c)))
        (is (some? (person "d@x.org")))
        (is (= (:t (d/db conn)) (checkpoint-t)))))))

(deftest failed-step-changes-nothing
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)]
    (tx! conn [{:person/email "a@x.org" :person/name "fine"}])
    (let [rep (core/replicator (tu/config client-config tables))
          a (eid conn "a@x.org")
          t0 (core/basis-t rep)]
      (query "ALTER TABLE $s.person ADD CONSTRAINT no_evil CHECK (name <> 'evil')")
      (tx! conn [[:db/add a :person/name "evil"]
                 [:db/add a :person/tags "evil"]])
      (is (thrown? Exception (core/step! rep)))
      (testing "neither rows nor checkpoint moved"
        (is (= t0 (core/basis-t rep)))
        (is (= t0 (checkpoint-t)))
        (is (= "fine" (:name (person "a@x.org"))))
        (is (empty? (tags a))))
      (testing "the next step retries the same batch"
        (query "ALTER TABLE $s.person DROP CONSTRAINT no_evil")
        (is (= 1 (core/catch-up! rep)))
        (is (= "evil" (:name (person "a@x.org"))))
        (is (= #{"evil"} (tags a)))))))

(deftest schema-evolution
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        rep (core/replicator (tu/config client-config tables))]
    (tx! conn [{:db/ident :person/nickname :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one}
               {:db/ident :person/aliases :db/valueType :db.type/string
                :db/cardinality :db.cardinality/many}])
    (tx! conn [{:person/email "n@x.org" :person/nickname "Nico" :person/aliases ["N"]}])
    (core/catch-up! rep)
    (is (= "Nico" (:nickname (person "n@x.org"))))
    (is (= ["N"] (map :value (query "SELECT value FROM $s.person_aliases"))))))

(deftest schema-evolution-while-stopped
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        cfg (tu/config client-config tables)]
    (core/catch-up! (core/replicator cfg))
    (query "INSERT INTO $s.address (db_id) VALUES (-1)")
    (tx! conn [{:db/ident :person/nickname :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one}])
    (tx! conn [{:person/email "n@x.org" :person/nickname "Nico"}])
    (testing "restart adds the column and backfills from the log without a snapshot"
      (core/catch-up! (core/replicator cfg))
      (is (= "Nico" (:nickname (person "n@x.org"))))
      (is (= 1 (count (query "SELECT * FROM $s.address WHERE db_id = -1")))))))

(deftest resume-and-remap
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        cfg (tu/config client-config tables)]
    (core/catch-up! (core/replicator cfg))
    ;; A sentinel row survives only if the tables are not rebuilt.
    (query "INSERT INTO $s.address (db_id) VALUES (-1)")
    (tx! conn [{:person/email "late@x.org"}])

    (testing "restart resumes from the checkpoint without re-snapshotting"
      (core/catch-up! (core/replicator cfg))
      (is (some? (person "late@x.org")))
      (is (= 1 (count (query "SELECT * FROM $s.address WHERE db_id = -1")))))

    (testing "a changed mapping triggers a rebuild"
      (core/replicator (assoc-in cfg [:tables :person :columns :person/name] {:name "full_name"}))
      (is (empty? (query "SELECT * FROM $s.address WHERE db_id = -1")))
      (is (contains? (person "late@x.org") :full_name)))

    (testing "a missing table triggers a rebuild"
      (query "INSERT INTO $s.address (db_id) VALUES (-1)")
      (query "DROP TABLE $s.person_tags")
      (core/replicator cfg)
      (is (contains? (table-names) "person_tags"))
      (is (empty? (query "SELECT * FROM $s.address WHERE db_id = -1"))))))

(deftest overrides-and-renames
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        base (tu/config client-config
                        {:person {:attrs [:person/email :person/age :person/born :person/score]
                                  :name "people"
                                  :columns {:person/age {:pg-type "integer"}
                                            :person/born {:pg-type "date"}
                                            :person/score {:pg-type "text"}}}
                         :address {:attrs [:address/*]}})]
    (tx! conn [{:person/email "o@x.org" :person/age 7 :person/born #inst "2001-02-03T12:00:00Z"
                :person/score 1.5}])
    (core/replicator base)
    (testing ":pg-type overrides apply through a cast"
      (let [row (first (query "SELECT * FROM $s.people"))]
        (is (= 7 (:age row)))
        (is (= "2001-02-03" (str (:born row))))
        (is (= "1.5" (:score row)))))

    (testing "renaming a table drops the old one"
      (core/replicator (update-in base [:tables :person] dissoc :name))
      (is (= #{"d2pg_checkpoint" "person" "address"} (table-names))))

    (testing "removing a table from the mapping drops it"
      (core/replicator (update base :tables dissoc :address))
      (is (= #{"d2pg_checkpoint" "people"} (table-names))))))

(deftest background-replication
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        rep (core/start! (assoc (tu/config client-config tables) :max-backoff-ms 100))]
    (try
      (tx! conn [{:person/email "bg@x.org" :person/name "fine"}])
      (is (wait-until #(some? (person "bg@x.org"))))
      (is (nil? (core/last-error rep)))

      (testing "a failing step is reported and retried until it succeeds"
        (let [a (eid conn "bg@x.org")]
          (query "ALTER TABLE $s.person ADD CONSTRAINT no_evil CHECK (name <> 'evil')")
          (tx! conn [[:db/add a :person/name "evil"]])
          (is (instance? Exception (wait-until #(core/last-error rep))))
          (is (= "fine" (:name (person "bg@x.org"))))
          (tx! conn [[:db/add a :person/name "fine again"]])
          (is (wait-until #(and (nil? (core/last-error rep))
                                (= "fine again" (:name (person "bg@x.org"))))))))
      (finally (core/stop! rep)))))

(deftest long-names-do-not-force-a-rebuild
  (let [{:keys [client-config]}
        (tu/fresh-datomic
         [{:db/ident :item/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
          {:db/ident :item/an-extraordinarily-long-attribute-name-that-overflows-postgres
           :db/valueType :db.type/string :db/cardinality :db.cardinality/many}])
        cfg (tu/config client-config {:item {:attrs [:item/*]}})]
    (core/replicator cfg)
    (query "INSERT INTO $s.item (db_id) VALUES (-1)")
    (core/replicator cfg)
    (is (= 1 (count (query "SELECT * FROM $s.item WHERE db_id = -1"))))))

(deftest wildcard-conflicts-do-not-stall-the-tail
  (let [{:keys [client-config conn]}
        (tu/fresh-datomic (conj schema-tx {:db/ident :person/active? :db/valueType :db.type/boolean
                                           :db/cardinality :db.cardinality/one}))
        rep (core/replicator (tu/config client-config tables))]
    ;; Both attributes derive the column name "active".
    (tx! conn [{:db/ident :person/active :db/valueType :db.type/boolean
                :db/cardinality :db.cardinality/one}])
    (tx! conn [{:person/email "a@x.org" :person/active? true :person/active false}])
    (is (= 2 (core/catch-up! rep)))
    (testing "the column keeps replicating the attribute that had it first"
      (is (true? (:active (person "a@x.org")))))))
