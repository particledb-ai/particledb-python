# conformance/src

The adapter `run_conformance.sh` runs.

| file | what it is for |
|---|---|
| `harness.py` | The language-neutral half: runner contract (env in, results file out), case files, the `script` interpreter (incl. the `commit_error` write-skew rule), canonical value comparison (`../SPEC.md` §4), a RESP2 client, the `kill9` / `restart` control hook, peak-RSS reader. A port of the Java bundle's `Harness.java`. |
| `adapter.py` | The SDK binding: case-file canonical text -> typed Python parameters (`bind`), typed values -> canonical text with type checks (`canon`), script sessions over `particledb.connect()`, and every `custom` case by id. `adapter.py --stream-child` is the SQL-STREAM-001 probe (fresh process). |

Constraints: a cell is rendered from the TYPED value the SDK returned
(`canon` fails a case when a numeric is not a `Decimal`, a timestamptz has no
zone, an array is not a list ...), never from a string the adapter re-parses.
