# CDC change-stream payload fixtures

Messages that a source database's change stream delivered to Kafka, recorded
one operation sequence per file. Sink tests use them as input, so they run
against observed messages rather than hand-written ones.

These are recorded snapshots. The build does not regenerate them, and a new
recording from a clean source differs only in its timestamps (`lut`, `exp`).

## Layout

`<format>/<NN-case>.json`:

```json
{ "messages": [ { "key": "<raw key>", "value": "<raw value>" } ] }
```

`key` and `value` are the UTF-8 strings exactly as they appeared on the topic,
in topic order. An empty `messages` list means the operations produced no
message.

| Format | Key | Write value | Delete value |
|---|---|---|---|
| `json` | array `[namespace, set, digest, userKey]` | `msg: "write"`, `key`, `gen`, `exp`, `lut` and a typed field list `bins` | `msg: "delete"`, `key`, `gen`, `lut`; no `exp` or `bins` |
| `flat-json` | object `{namespace, set, userKey, digest}` | change metadata under `metadata` (with `msg: "write"`, `gen`, `exp`, `lut`), fields at the top level | only `metadata`, with `msg: "delete"`, `gen`, `lut` and no `exp` |

A delete is never a Kafka tombstone: it is a message with a value. A delete that
leaves a tombstone in the source (cases 06 and 08) also carries `durable: true`;
a delete without one (07) and a shipped expiry (13) omit `durable`, so those two
look the same. In `flat-json` a write that sets no field also carries only
`metadata` (the last message of 04), so `msg` is what tells a write from a delete.

`userKey` is `null` (`json`) or absent (`flat-json`) when the source does not
store it. `exp` is an absolute expiry in Unix epoch seconds, `0` for none.

## Cases

| Case | Operations |
|---|---|
| `01-create` | create a record |
| `02-full-update` | create, then overwrite every field |
| `03-field-removed` | create, change one field, remove another; the source ships whole records |
| `04-field-removed-changed-only` | the same steps while the source ships only changed fields |
| `05-field-added` | create, then add a field |
| `06-durable-delete` | create, then delete leaving a tombstone |
| `07-delete` | create, then delete without a tombstone |
| `08-key-not-stored` | create and delete with the user key not stored |
| `09-collection-element-ops` | create list and map fields, append to the list, put into the map |
| `10-hyperloglog` | add values to a HyperLogLog field |
| `11-geojson` | create a GeoJSON point field |
| `12-expiry-default` | create with a 5 s TTL and let it expire; expiry is not shipped |
| `13-expiry-shipped` | the same with expiry shipping enabled |
| `14-truncate` | create, then truncate the record's set; the truncate is not shipped |
| `15-scalar-types` | create double, boolean and bytes fields |
