# conformance/cases

The language-neutral case files of the SDK conformance suite: one JSON file per
group. Every SDK adapter runs every case its mode lists; the spec, the canonical
value forms and the results contract are in `../SPEC.md`.

| file | group |
|---|---|
| `defaults.json` | DEF-*: a plain default connect is the native wire, protocol 2 multiplexed, with every efficiency/fidelity capability -- asserted from the SERVER (`SHOW pdb.wire_*`) and the client |
| `sql.json` | SQL-*: type round-trips, parameter binding, prepared reuse, batches, streaming, multi-statement strings, SQLSTATE mapping, transactions at RC/RR/SERIALIZABLE, savepoints, cancel / timeout, DDL visibility |
| `concurrency.json` | CONC-*: many concurrent streams over ONE multiplexed connection |
| `kv.json` | KV-*: key-value parity with RESP (same keys, values, TTLs); `notes` lists which Redis commands are and are not reachable from SDKs |
| `durability.json` | DUR-*: an acknowledged write survives `kill -9` |

Each case: `id`, `purpose`, `kind` (`script`: declarative `steps` an adapter
interprets generically; `custom`: prose `steps` an adapter implements by id),
`modes` (`default`, `strict`), `steps`, `expected`.

Constraints:

- **These files are copied verbatim into every SDK bundle** (`<sdk>/conformance/cases/`)
  from the shared SDK conformance suite; change every copy together,
  never one alone.
- **A case id is a contract**: never reuse or renumber one. Retire a case by
  deleting it (its results then fail validation as "not a case id").
- **No literal port numbers.** The native port an SDK must default to is the
  server's own default (`PDB_CONF_DEFAULT_PORT`, read from the binary).
- SQL in `script` steps uses `$1..$N` placeholders; adapters translate to their
  SDK's spelling.
