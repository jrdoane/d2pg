(ns d2pg.replication-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [d2pg.core :as core]
            [d2pg.test-util :as tu :refer [query tx!]]
            [datomic.client.api :as d])
  (:import (java.util Date UUID)))

(use-fixtures :each (fn [t] (tu/reset-pg!) (t)))

(def schema-tx
  [{:db/ident :status/active}
   {:db/ident :status/inactive}
   {:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :person/email :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :person/id :db/valueType :db.type/uuid :db/cardinality :db.cardinality/one}
   {:db/ident :person/born :db/valueType :db.type/instant :db/cardinality :db.cardinality/one}
   {:db/ident :person/score :db/valueType :db.type/double :db/cardinality :db.cardinality/one}
   {:db/ident :person/kind :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :person/status :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :person/best-friend :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :person/tags :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
   {:db/ident :person/coords :db/valueType :db.type/tuple
    :db/tupleTypes [:db.type/long :db.type/long] :db/cardinality :db.cardinality/one}
   {:db/ident :person/password-hash :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :address/street :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :address/city :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(def tables
  {:person {:attrs [:person/* :address/street]
            :exclude [:person/password-hash]
            :require [:person/email]
            :columns {:person/status {:as :ident}}}
   :address {:attrs [:address/*]}})

(defn- person [email]
  (first (query "SELECT * FROM $s.person WHERE email = ?" email)))

(defn- tags [db-id]
  (set (map :value (query "SELECT value FROM $s.person_tags WHERE db_id = ?" db-id))))

(defn- eid [conn email]
  (ffirst (d/q '[:find ?e :in $ ?email :where [?e :person/email ?email]]
               (d/db conn) email)))

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

      (testing "retracting an entity removes it and nulls refs to it"
        (let [bob-id (eid conn "bob@x.org")]
          (tx! conn [[:db/add ada-id :person/email "ada@x.org"]])
          (tx! conn [[:db/add bob-id :person/best-friend ada-id]])
          (tx! conn [[:db/retractEntity ada-id]])
          (core/catch-up! rep)
          (is (empty? (query "SELECT * FROM $s.address WHERE db_id = ?" ada-id)))
          (is (nil? (person "ada@x.org")))
          (is (nil? (:best_friend (person "bob@x.org"))))))

      (testing "checkpoint tracks the last applied t"
        (is (= (:t (d/db conn)) (core/basis-t rep)))
        (is (= (:t (d/db conn))
               (:basis_t (first (query "SELECT basis_t FROM $s.d2pg_checkpoint")))))))))

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
      (is (contains? (person "late@x.org") :full_name)))))

(deftest background-replication
  (let [{:keys [client-config conn]} (tu/fresh-datomic schema-tx)
        rep (core/start! (tu/config client-config tables))]
    (try
      (tx! conn [{:person/email "bg@x.org"}])
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (while (and (nil? (person "bg@x.org")) (< (System/currentTimeMillis) deadline))
          (Thread/sleep 25)))
      (is (some? (person "bg@x.org")))
      (finally (core/stop! rep)))))
