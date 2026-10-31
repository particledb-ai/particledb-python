#!/usr/bin/env python3
"""Case-file reader, results validator and table printer for the SDK conformance suite.

  conformance.py ids      CASES_DIR MODE            -> case ids that run in MODE, one per line
  conformance.py validate CASES_DIR MODE RESULTS    -> exit 1 (and say why) unless RESULTS is complete
  conformance.py release  CASES_DIR MODE RESULTS    -> require every case to pass, with no skips
  conformance.py table    CASES_DIR RESULTS...      -> per-group pass/fail/skip table + every non-pass

Validation is the contract that makes a results file mean something:
  * every case that runs in MODE has an entry -- a MISSING case is a failure, never a pass;
  * status is exactly one of pass / fail / skip;
  * a skip carries a non-empty `reason` and a fail a non-empty `detail`;
  * no entry names a case id the case files do not define (a typo would otherwise hide a case).
"""
import json
import os
import sys


def load_cases(cases_dir):
    cases = []
    seen = set()
    for name in sorted(os.listdir(cases_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(cases_dir, name)) as f:
            doc = json.load(f)
        for c in doc["cases"]:
            cid = c["id"]
            if cid in seen:
                raise SystemExit(f"duplicate case id {cid} in {name}")
            seen.add(cid)
            for k in ("purpose", "kind", "modes", "steps", "expected"):
                if k not in c:
                    raise SystemExit(f"case {cid} in {name} has no '{k}'")
            c["_group"] = doc["group"]
            cases.append(c)
    return cases


def validate(cases, mode, path):
    with open(path) as f:
        res = json.load(f)
    errs = []
    if mode not in ("default", "strict"):
        errs.append(f"unsupported mode {mode!r}")
    results = res.get("results")
    if not isinstance(results, dict):
        return [f"{path}: no 'results' object"], res
    want = [c["id"] for c in cases if mode in c["modes"]]
    known = {c["id"] for c in cases}
    for cid in want:
        r = results.get(cid)
        if r is None:
            errs.append(f"{cid}: MISSING (the adapter never reported it)")
            continue
        st = r.get("status")
        if st not in ("pass", "fail", "skip"):
            errs.append(f"{cid}: bad status {st!r}")
        elif st == "skip" and not str(r.get("reason", "")).strip():
            errs.append(f"{cid}: skip without a reason")
        elif st == "fail" and not str(r.get("detail", "")).strip():
            errs.append(f"{cid}: fail without a detail")
    for cid in results:
        if cid not in known:
            errs.append(f"{cid}: not a case id in the case files")
    for k in ("sdk", "mode"):
        if k not in res:
            errs.append(f"results file has no '{k}'")
    if res.get("mode") not in (None, mode):
        errs.append(f"results are for mode {res.get('mode')!r}, expected {mode!r}")
    return errs, res


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 2
    cmd, cases_dir = argv[1], argv[2]
    cases = load_cases(cases_dir)
    if cmd == "ids":
        mode = argv[3]
        for c in cases:
            if mode in c["modes"]:
                print(c["id"])
        return 0
    if cmd in ("validate", "release"):
        mode, path = argv[3], argv[4]
        errs, res = validate(cases, mode, path)
        if cmd == "release":
            for case in cases:
                if mode in case["modes"]:
                    status = res.get("results", {}).get(case["id"], {}).get("status")
                    if status != "pass":
                        errs.append(f"{case['id']}: release requires pass, got {status!r}")
        for e in errs:
            print(f"INVALID {path}: {e}")
        return 1 if errs else 0
    if cmd == "table":
        group_of = {c["id"]: c["_group"] for c in cases}
        groups = []
        for c in cases:
            if c["_group"] not in groups:
                groups.append(c["_group"])
        rows, notes, bad = [], [], 0
        for path in argv[3:]:
            mode = json.load(open(path)).get("mode", "?")
            errs, res = validate(cases, mode, path)
            bad += len(errs)
            label = f"{res.get('sdk', '?')}/{mode}"
            counts = {g: [0, 0, 0] for g in groups}
            for cid, r in res.get("results", {}).items():
                g = group_of.get(cid)
                if g is None:
                    continue
                i = {"pass": 0, "fail": 1, "skip": 2}.get(r.get("status"), 1)
                counts[g][i] += 1
                if r.get("status") != "pass":
                    why = r.get("detail") or r.get("reason") or ""
                    notes.append(f"  {label:<24} {cid:<15} {r.get('status', '?'):<5} {why[:160]}")
            for e in errs:
                notes.append(f"  {label:<24} INVALID         {e}")
            tot = [sum(counts[g][i] for g in groups) for i in range(3)]
            rows.append((label, counts, tot, len(errs)))
        hdr = f"{'sdk/mode':<24}" + "".join(f"{g:>16}" for g in groups) + f"{'TOTAL p/f/s':>16}{'invalid':>9}"
        print(hdr)
        print("-" * len(hdr))
        for label, counts, tot, ne in rows:
            line = f"{label:<24}" + "".join(f"{'%d/%d/%d' % tuple(counts[g]):>16}" for g in groups)
            print(line + f"{'%d/%d/%d' % tuple(tot):>16}{ne:>9}")
        if notes:
            print("\nnon-pass entries:")
            print("\n".join(notes))
        fails = sum(r[2][1] for r in rows)
        return 1 if (fails or bad) else 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
