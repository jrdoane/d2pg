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

(defn sync-batch!
  "Brings the rows for eids in table into line with db. With :fresh?, the
  table is known to hold none of them, so nothing needs deleting."
  [conn db {:keys [pg-schema] :as model} table-key eids {:keys [fresh?]}]
  (let [table (get-in model [:tables table-key])
        ;; Entities with none of the pattern's attrs still pull as {:db/id e},
        ;; so every eid comes back and non-members get deleted.
        pulled (pull-many db (:pull-pattern table) eids)
        {members true others false} (group-by #(schema/member? table %) pulled)
        member-ids (map :db/id members)]
    (when-not fresh?
      (pg/delete-rows! conn pg-schema table (map :db/id others)))
    (pg/upsert-rows! conn pg-schema table (map #(row table %) members))
    (doseq [jt (:join-tables table)
            :let [rows (mapcat #(join-rows jt %) members)]]
      (if fresh?
        (pg/insert-join-values! conn pg-schema jt rows)
        (pg/replace-join-values! conn pg-schema jt member-ids rows)))))

(defn sync-entities!
  "sync-batch! over eids in batches of batch-size. conn should be a
  PostgreSQL connection inside a transaction."
  [conn db model table-key eids batch-size]
  (doseq [batch (partition-all batch-size eids)]
    (sync-batch! conn db model table-key batch {})))
