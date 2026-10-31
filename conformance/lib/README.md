# conformance/lib

Runner plumbing shared by every SDK bundle (identical in every ParticleDB SDK
repository; change every copy together, never one alone).

| file | what it is for |
|---|---|
| `server.sh` | Starts a fresh server (`PDB_BIN`) for one mode (`default` = `--concurrency-control optimistic --durability groupsync`, `strict` = `--concurrency-control strict-optimistic --durability sync`) on the server's DEFAULT native port when free (else a free block), and is the adapters' control hook: `kill9`, `restart` (same ports and data dir), `stop`, `env` (the `PDB_CONF_*` exports). Signals only the PID it started. |
| `conformance.py` | `ids` (the case ids a mode runs), `validate` (a results file is complete: every case reported, statuses pass/fail/skip, every skip has a reason and every fail a detail, no unknown ids), `table` (per-group pass/fail/skip). |

Constraints:

- `server.sh` reads the default native port from `BIN start --help`
  ("--pdb-port ... (default N)") unless `PDB_CONF_DEFAULT_PORT` is set; nothing
  here may hard-code a port.
- A MISSING case in a results file is a failure, never a pass.
