# EMQX Mapping Tables Plugin

Named, in-memory mapping tables for rule SQL, seeded from JSON files. Long
`CASE ... WHEN ... THEN` ladders in rules collapse into a single table lookup:

```sql
FOREACH payload.frames AS c
DO
  subbits(hexstr2bin(c), 5, 12) AS item_id,
  maptab_lookup('signals', item_id) AS sig,
  maptab_lookup('signals', item_id, 'signal_name', 'Unknown') AS signal_name,
  CASE WHEN is_map(sig)
    THEN subbits(hexstr2bin(c), sig.start_bit, sig.length, sig.type, sig.signedness, sig.endian)
    ELSE 0.0
  END AS data
FROM "t/#"
```

The `CASE WHEN is_map(sig)` guard matters: on a miss `sig` is `undefined`, and
feeding `undefined` into `subbits` throws — inside a `FOREACH` that error drops
the whole message (every array element), not just the unknown one. Guard any
field access into a lookup result that can miss, or filter misses out in the
`INCASE` clause.

Alternatively, supply a **complete** default row and skip the guard entirely —
every field `subbits` needs is then always present:

```sql
maptab_lookup('signals', item_id,
  json_decode('{"signal_name":"Unknown","start_bit":17,"length":48,"type":"bits","signedness":"unsigned","endian":"big"}')) AS sig,
subbits(hexstr2bin(c), sig.start_bit, sig.length, sig.type, sig.signedness, sig.endian) AS data
```

Note an *empty* default (`map_new()`) does not remove the need for the guard:
the row fields are still missing, so `sig.start_bit` is `undefined` and
`subbits` throws all the same.

## Rule SQL functions

- `maptab_lookup(Table, Key)` — the row's value map, or `undefined` when absent.
- `maptab_lookup(Table, Key, DefaultRow)` — the row's value map, or the given
  map on a miss (a field name is always a string, so a map third argument
  unambiguously selects this form). Build the default with `map_new()`,
  `map_put(...)`, or `json_decode('{...}')`.
- `maptab_lookup(Table, Key, Field)` — a single value field, or `undefined`.
- `maptab_lookup(Table, Key, Field, Default)` — same, with a caller-supplied default.

Lookups never crash a rule: an unknown table, a missing key, or a key of the
wrong type is a normal miss (`undefined`).

Key matching is exact term equality with **no type coercion**: an integer key
only matches an integer argument and a string key only matches a string
argument (`50`, `'50'` are different keys). Cast the argument in the rule when
needed, e.g. an `item_id` extracted with `subbits` is already an integer.

## Access and sharing

Only the administrator manages tables (through the CLI). All tables are
shared: a lookup returns the same rows for every client, whether or not the
client belongs to a multi-tenancy namespace. The plugin does not scope tables
or lookups by tenant namespace.

When rows must differ per tenant, encode the tenant in the data: compose the
lookup key from the `tns` client attribute (for example
`maptab_lookup('signals', concat(client_attrs.tns, ':', item_id))`), or use
one table per tenant and compose the table name the same way. Apply the
convention to every key in the table and to every lookup site: a lookup that
omits the tenant part is a plain miss only when no bare key exists in the
table.

## Table files

A table is a single JSON file; the file name (minus `.json`) is the table
name, charset `[a-zA-Z0-9_-]`. The file is an array of row objects; each row
must have a `key` field, the remaining fields are the row's value map. Native
JSON types are preserved for keys and values:

```json
[
  {"key": 1, "signal_name": "temperature_c", "start_bit": 17, "length": 8,
   "type": "integer", "signedness": "signed", "endian": "big"},
  {"key": 2, "signal_name": "pressure_kpa", "start_bit": 17, "length": 32,
   "type": "float", "signedness": "unsigned", "endian": "big"}
]
```

The `type`/`signedness`/`endian` strings above are exactly what the builtin
`subbits/6` rule function expects, so looked-up fields feed straight into it.

A `key` must be a JSON integer or string. Loading is fail-closed: invalid
JSON, a row without `key`, duplicate keys, or a key of any other type (float,
boolean, null, array, object) rejects the whole file and keeps the previous
version.

## Storage and clustering

The on-disk JSON files under `<data_dir>/plugins/emqx_maptabs/tables/` are the
source of truth; ETS is a per-node read cache. `maptabs load` and
`maptabs delete` run as a `cluster_rpc` transaction: every node re-validates
the content, writes the file atomically and swaps its cache, and a node that
was down during an update replays the transaction when it rejoins. The plugin
must be installed on all nodes of the cluster.

Reloads swap the cache atomically: a concurrent reader sees the old or the new
table, never a partial one.

## Configuration

- `max_tables` (default `100`): maximum number of mapping tables. Loading a
  new table beyond the limit is rejected; replacing an existing table is
  always allowed.
- `max_rows_per_table` (default `10000`): maximum number of rows in a single
  table; a file with more rows is rejected as a whole.
- `max_table_file_bytes` (default `10000000`): maximum size of a table JSON
  file; larger files are rejected before cluster replication.

Limits are checked at load time; changing a limit never drops already-loaded
tables.

## CLI

```
emqx ctl maptabs list             # tables cached on this node (rows, version)
emqx ctl maptabs status           # same, for every running node (drift detection)
emqx ctl maptabs load <file>      # validate + replicate a table file to all nodes
emqx ctl maptabs reload [<name>]  # re-read table files from local disk on all nodes (reconcile)
emqx ctl maptabs get <name>       # print the table file content
emqx ctl maptabs delete <name>    # delete a table on all nodes
```
