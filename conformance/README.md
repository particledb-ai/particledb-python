# conformance

The Python SDK's (`particledb`) SDK-conformance bundle. One command runs the
whole suite against this SDK from source:

```
PDB_BIN=/path/to/particledb ./run_conformance.sh [--mode default|strict] [--results FILE]
PDB_PY=/path/to/python ...   # another interpreter, e.g. a venv without pyarrow
```

| entry | what it is for |
|---|---|
| `run_conformance.sh` | Starts a fresh server (unless `PDB_CONF_PORT` + `PDB_CONF_CONTROL` name one), puts this repository first on `PYTHONPATH`, runs the mode's cases, validates the results file |
| `src/` | The adapter (`README.md` there) |
| `cases/`, `lib/` | The shared SDK conformance suite (`cases`, `lib`, spec in `SPEC.md`), identical in every ParticleDB SDK repository; changes are made to all copies together |

Constraints: the adapter connects with the documented zero-option entry point
(`particledb.connect(host=..., port=...)`; no port when the server is on the
default) because the defaults are what is under test. It REFUSES to run when
the imported `particledb` is not this tree's (an editable install of another
checkout shadows `PYTHONPATH` otherwise). SQL-STREAM-001 runs in a fresh
process per decoder and asserts the peak RSS (<= 256 MiB).
