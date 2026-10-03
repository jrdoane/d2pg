# d2pg

Replicates a Datomic database into PostgreSQL. You declare which tables you
want and which attributes go in them, as data; d2pg derives the rest (column
types, join tables, unique constraints, column names) from the schema in the
source Datomic database. It takes an initial snapshot, then tails the
transaction log, keeping PostgreSQL at the **current state** of the source.

The source is reached through the Datomic **Client API**, so it works with
Datomic Local, Datomic Cloud, and Datomic Pro via peer-server. `deps.edn`
includes `com.datomic/local`; add `com.datomic/client-pro` or
`com.datomic/client-cloud` for those deployments.

## Running

```sh
clojure -M -m d2pg example/config.edn
```

Or embed it:

```clojure
(require '[d2pg.core :as d2pg])

(def rep (d2pg/start! config))  ; snapshot if needed, then tail in the background
(d2pg/stop! rep)

;; Or drive it by hand:
(def rep (d2pg/replicator config))
(d2pg/step! rep)       ; apply the next batch, returns the number of txs applied
(d2pg/catch-up! rep)   ; step until caught up
```

## Mapping

See [`example/config.edn`](example/config.edn) for every option. Tables are
explicit; each one lists attribute patterns:

| Key | Meaning |
|---|---|
| `:attrs` | Attributes to map. `:ns/*` selects every attribute in a namespace, including ones installed later. |
| `:exclude` | Attributes to drop after expanding `:attrs`. |
| `:require` | Only entities that have **all** of these get a row. Without it, any mapped attribute is enough. |
| `:columns` | Per-attribute overrides: `:name` (column name), `:as :ident` (render an enum ref as its ident text), `:pg-type`. |
| `:name` | Table name override. The default comes from the table key. |

### What is derived

- **Primary key**: `db_id bigint`, the Datomic entity id.
- **Column names**: snake_case of the attribute name (`:person/is-active?` → `is_active`).
  Attributes from a different namespace than the table are prefixed (`:address/street` in
  `:person` → `address_street`). Collisions are reported at startup.
- **Types**: string/keyword/symbol/uri → `text`, long/ref → `bigint`, boolean → `boolean`,
  instant → `timestamptz`, uuid → `uuid`, double → `double precision`, float → `real`,
  bigint/bigdec → `numeric`, bytes → `bytea`, tuple → `jsonb`. Keywords are stored as
  `"ns/name"`.
- **Cardinality many** → join table `<table>_<column>(db_id, value)`.
- **`:db/unique`** → a deferred `UNIQUE` constraint.
- Refs are plain `bigint` columns; there are no foreign key constraints.

## How it works

Snapshot and tail share one code path: for each entity touched, pull it from a
Datomic db value and upsert its rows if it belongs in the table, or delete them
if not. Retractions, card-many changes, `:require` membership and
`:db/retractEntity` all go through that path.

- **Snapshot** drops and recreates the mapped tables, loads them from one db
  value, and writes the checkpoint, in one PostgreSQL transaction.
- **Tail** reads up to `:batch-txs` transactions from the log, re-pulls the
  touched entities as of the last one, and writes rows and the new checkpoint
  in one PostgreSQL transaction, so a restart never applies a transaction
  twice.
- **Checkpoint**: `<pg-schema>.d2pg_checkpoint` holds the last applied `t` and
  a hash of the mapping. If the mapping changes, the next start takes a fresh
  snapshot.
- **Schema changes**: when the log installs new attributes, the model is
  re-resolved and columns or join tables are added as needed. Changes are
  additive only.

### Not supported yet

History or temporal tables, non-additive schema changes (type changes, renames,
dropped columns), FK constraints, and more than one source database per process.

## Development

Tests need a local PostgreSQL. They use `jdbc:postgresql://localhost:5432/d2pg_test`
(created if missing); set `D2PG_TEST_JDBC_URL` to override. Datomic Local runs in
memory.

```sh
clojure -X:test
```
