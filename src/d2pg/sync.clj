(ns d2pg.sync
  "The one path by which Datomic entities become PostgreSQL rows, shared by
  snapshot and tail: pull the entity from a db value, then upsert it if it
  belongs in the table or delete it if it doesn't."
  (:require [d2pg.pg :as pg]
            [d2pg.schema :as schema]
            [d2pg.types :as types]
            [datomic.client.api :as d]))

(defn- pull-many [db pattern eids]
  (map first (d/q {:query '[:find (pull ?e pattern)
                            :in $ pattern [?e ...]]
                   :args [db pattern eids]})))

(defn- row [{:keys [columns]} pulled]
  (into [(:db/id pulled)]
        (map (fn [{:keys [attr value-type as]}]
               (types/->jdbc value-type as (get pulled attr))))
        columns))

(defn- join-rows [{:keys [attr value-type as]} pulled]
  (for [v (get pulled attr)]
    [(:db/id pulled) (types/->jdbc value-type as v)]))

(defn- apply-batch! [conn pg-schema table pulled]
  (let [{members true others false} (group-by #(schema/member? table %) pulled)
        member-ids (map :db/id members)]
    (pg/delete-rows! conn pg-schema table (map :db/id others))
    (pg/upsert-rows! conn pg-schema table (map #(row table %) members))
    (doseq [jt (:join-tables table)]
      (pg/replace-join-values! conn pg-schema jt member-ids
                               (mapcat #(join-rows jt %) members)))))

(defn sync-entities!
  "Brings the rows for eids in table into line with db. conn should be a
  PostgreSQL connection inside a transaction."
  [conn db {:keys [pg-schema] :as model} table-key eids batch-size]
  (let [table (get-in model [:tables table-key])]
    (doseq [batch (partition-all batch-size eids)]
      ;; Entities with none of the pattern's attrs still pull as {:db/id e},
      ;; so every eid comes back and non-members get deleted.
      (apply-batch! conn pg-schema table (pull-many db (:pull-pattern table) batch)))))
