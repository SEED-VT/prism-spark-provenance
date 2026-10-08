# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Precision and recall benchmark for fine-grained UDF provenance.

It measures how well each method's reported provenance set matches a ground-truth
influencer set, output by output. Ground truth is a perturbation oracle. An input atom,
which is a field or one element of a list field, influences an output if changing that
atom alone over a declared domain changes the output.

Two methods are compared. The coarse method is row-level lineage, which reports the whole
input record. It never misses an influencer but is imprecise. The fine method reports the
field and element set from the fineprov AST tracer.

Precision is |reported & truth| / |reported| and recall is |reported & truth| / |truth|,
both averaged over outputs. A sound method has recall 1, and the benchmark measures how
much precision fine-grained tracking adds while staying sound.
"""

import os
import random
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_ROOT, "python"))

from fineprov import trace, tag_row, taglist, prov, lineage
from influence import influence

PREFIX = "r."


# A benchmark job is a UDF plus how to build its tagged input and perturb its atoms.
class Job:
    def __init__(self, name, raw_udf, sample, list_fields=()):
        self.name = name
        self.raw_udf = raw_udf                  # maps a dict row to its output, untraced
        self.sample = sample                    # returns a dict row of concrete values
        self.list_fields = set(list_fields)
        self.traced = trace(raw_udf)

    # Every individually perturbable input, with a stable id.
    def atoms(self, row):
        out = []
        for f, v in row.items():
            if f in self.list_fields:
                for i, e in enumerate(v):
                    out.append((PREFIX + f + "[" + str(i), f, i, e))
            else:
                out.append((PREFIX + f, f, None, v))
        return out

    def coarse_ids(self, row):
        # Row-level lineage reports the whole record.
        return {a[0] for a in self.atoms(row)}

    def _with_atom(self, row, field, idx, newval):
        r = dict(row)
        if idx is None:
            r[field] = newval
        else:
            lst = list(r[field])
            lst[idx] = newval
            r[field] = lst
        return r

    # Tagged input for the tracer, using the same id scheme.
    def build_tagged(self, row):
        t = tag_row({k: v for k, v in row.items() if k not in self.list_fields},
                    prefix=PREFIX)
        for f in self.list_fields:
            t.value[f] = taglist(row[f], prefix=PREFIX + f + "[")
        return t

    def fine_ids(self, row):
        # Reproducibility provenance, both data and control.
        return set(prov(self.traced(self.build_tagged(row))))

    def fine_data_ids(self, row):
        # Data lineage only, without control dependencies.
        return set(lineage(self.traced(self.build_tagged(row))))

    # Ground truth by perturbing one atom at a time.
    def truth_ids(self, row, domain):
        base = self.raw_udf(row)
        truth = set()
        for aid, field, idx, val in self.atoms(row):
            for alt in domain(field, val):
                if alt == val:
                    continue
                if self.raw_udf(self._with_atom(row, field, idx, alt)) != base:
                    truth.add(aid)
                    break
        return truth


# Perturbation domains give the candidate alternative values for each field.
def could_domain(field, value):
    """Wide domain for could-influence, where any reachable change counts.

    For max, every element could become the maximum, so all are influencers.
    """
    if isinstance(value, bool):
        return [not value]
    if isinstance(value, (int, float)):
        return [value + 1, value - 1, value * 2 + 1, 0, -value - 1, 99999]
    if isinstance(value, str):
        # Unit strings flip the branch conditions.
        return ["mm", "in", "ft", "X" + value, value + "X", ""]
    return [None]


def does_domain(field, value):
    """Local domain for does-influence, where only a marginal change counts.

    For max, only the current maximum changes the output, which matches what the
    extremal model reports.
    """
    if isinstance(value, bool):
        return [not value]
    if isinstance(value, (int, float)):
        return [value + 1, value - 1]
    if isinstance(value, str):
        return ["mm", "in", "ft"]
    return [None]


default_domain = could_domain


def _weather(row):
    # Weather unit conversion. It depends on value and unit, not station or date.
    v = row["value"]
    return v if row["unit"] == "mm" else v * 304.8


def _layover(row):
    # The layover depends on depart and arrive, not carrier or gate.
    return (row["arrive"] - row["depart"]) if row["arrive"] >= row["depart"] \
        else (row["arrive"] - row["depart"] + 1440)


def _branch(row):
    # The flag selects which branch field becomes the output.
    return row["a"] if row["flag"] else row["b"]


def _maxsnow(row):
    # Extremal aggregate over a list field, which uses the model regime.
    return max(row["readings"])


JOBS = [
    Job("weather_convert", _weather,
        lambda: {"value": random.randint(1, 99), "unit": random.choice(["mm", "in", "ft"]),
                 "station": random.choice(["A", "B", "C"]), "date": "2015/12/25"}),
    Job("transit_layover", _layover,
        lambda: {"depart": random.randint(0, 1439), "arrive": random.randint(0, 1439),
                 "carrier": random.choice(["AA", "UA"]), "gate": random.randint(1, 40)}),
    Job("control_branch", _branch,
        lambda: {"flag": random.random() < 0.5, "a": random.randint(0, 100),
                 "b": random.randint(0, 100), "noise": random.randint(0, 100)}),
    Job("max_snow", _maxsnow,
        lambda: {"readings": [random.randint(0, 100) for _ in range(5)]},
        list_fields=["readings"]),
]


def _pr(reported, truth):
    tp = len(reported & truth)
    precision = tp / len(reported) if reported else (1.0 if not truth else 0.0)
    recall = tp / len(truth) if truth else 1.0
    return precision, recall


def evaluate(job, trials, domain):
    sums = {"coarse": [0.0, 0.0], "fine": [0.0, 0.0]}
    for _ in range(trials):
        row = job.sample()
        truth = job.truth_ids(row, domain)
        for method, ids in (("coarse", job.coarse_ids(row)),
                            ("fine", job.fine_ids(row))):
            p, r = _pr(ids, truth)
            sums[method][0] += p
            sums[method][1] += r
    return {m: (s[0] / trials, s[1] / trials) for m, s in sums.items()}


DATAFLOW = ("weather_convert", "transit_layover", "control_branch")


def main(trials=300, seed=7):
    random.seed(seed)
    by_name = {j.name: j for j in JOBS}
    header = "%-18s %-8s %9s %9s" % ("job", "method", "precision", "recall")

    print("Fine-grained provenance precision/recall  (%d trials/job)\n" % trials)
    print("[1] Data/control-flow UDFs -- could-influence ground truth "
          "(exact regime)\n")
    print(header)
    print("-" * len(header))
    agg = {"coarse": [0.0, 0.0], "fine": [0.0, 0.0]}
    results = {}
    for name in DATAFLOW:
        res = evaluate(by_name[name], trials, could_domain)
        results[name] = res
        for method in ("coarse", "fine"):
            p, r = res[method]
            agg[method][0] += p
            agg[method][1] += r
            print("%-18s %-8s %9.3f %9.3f" % (name, method, p, r))
        print()
    print("-" * len(header))
    for method in ("coarse", "fine"):
        p, r = agg[method][0] / len(DATAFLOW), agg[method][1] / len(DATAFLOW)
        print("%-18s %-8s %9.3f %9.3f" % ("MACRO AVG", method, p, r))

    # The two provenance modes bound the ground truth from each side. Reproducibility
    # provenance has recall 1 against could-influence, and data lineage has precision 1
    # against does-influence. The data lineage recall gap comes from categorical or
    # boolean selectors, which have no marginal change.
    print("\n[1b] Two provenance modes bracket the truth "
          "(the x-if-x>y-else-y distinction)\n")
    hdr2 = "%-18s %-24s %9s %9s" % ("job", "mode vs truth", "precision", "recall")
    print(hdr2)
    print("-" * len(hdr2))
    repro_recalls, data_precisions = [], []
    for name in DATAFLOW:
        job = by_name[name]
        sr = [0.0, 0.0]
        sd = [0.0, 0.0]
        for _ in range(trials):
            row = job.sample()
            p, r = _pr(job.fine_ids(row), job.truth_ids(row, could_domain))
            sr[0] += p
            sr[1] += r
            p, r = _pr(job.fine_data_ids(row), job.truth_ids(row, does_domain))
            sd[0] += p
            sd[1] += r
        rp, rr = sr[0] / trials, sr[1] / trials
        dp, dr = sd[0] / trials, sd[1] / trials
        repro_recalls.append(rr)
        data_precisions.append(dp)
        print("%-18s %-24s %9.3f %9.3f" % (name, "repro vs could (sound)", rp, rr))
        print("%-18s %-24s %9.3f %9.3f" % ("", "data-lin. vs does (precise)", dp, dr))
        print()

    # The extremal model names the one element that drives the max. It has low recall
    # against could-influence and is exact against does-influence, so both are reported.
    print("\n[2] Aggregate UDF (max over a list) -- the model regime\n")
    print(header)
    print("-" * len(header))
    job = by_name["max_snow"]
    could = evaluate(job, trials, could_domain)
    does = evaluate(job, trials, does_domain)
    cp, cr = could["fine"]
    dp, dr = does["fine"]
    bp, br = could["coarse"]
    print("%-18s %-8s %9.3f %9.3f" % ("max_snow/could", "coarse", bp, br))
    print("%-18s %-8s %9.3f %9.3f  <- could-influence: model is high-precision,"
          " low-recall by design" % ("max_snow/could", "fine", cp, cr))
    print("%-18s %-8s %9.3f %9.3f  <- does-influence: extremal model names the"
          " actual cause (residual recall gap = ties at the max)" %
          ("max_snow/does", "fine", dp, dr))

    # Fault localization inserts one outlier into each group and ranks the inputs.
    # Precision@1 is the fraction of groups where the outlier ranks first.
    print("\n[3] Influence fault localization -- precision@1 over %d groups "
          "(one injected outlier each)\n" % trials)
    n = 8
    hit_inf = hit_val = hit_base = 0
    for _ in range(trials):
        normals = [random.uniform(-5, 5) for _ in range(n - 1)]
        outlier = random.choice([-1, 1]) * random.uniform(40, 80)
        ins = random.randint(0, n - 1)
        vals = normals[:ins] + [outlier] + normals[ins:]
        elems = [("e%d" % i, v) for i, v in enumerate(vals)]
        truth = "e%d" % ins
        # Closed-form influence on the sum.
        if influence(elems, "sum").ranked()[0] == truth:
            hit_inf += 1
        # Baseline that ranks by raw value, which misses negative outliers.
        if max(elems, key=lambda kv: kv[1])[0] == truth:
            hit_val += 1
        # Baseline that guesses at random.
        if ("e%d" % random.randrange(n)) == truth:
            hit_base += 1
    p_inf, p_val, p_base = hit_inf / trials, hit_val / trials, hit_base / trials
    print("%-26s %9s" % ("method", "prec@1"))
    print("-" * 36)
    print("%-26s %9.3f" % ("influence(sum)", p_inf))
    print("%-26s %9.3f  (sign-blind: misses negative outliers)" % ("rank by raw value", p_val))
    print("%-26s %9.3f  (= 1/n)" % ("random", p_base))

    # These assertions also serve as a regression test.
    for name in DATAFLOW:
        cprec, _ = results[name]["coarse"]
        fprec, frec = results[name]["fine"]
        assert abs(frec - 1.0) < 1e-9, "%s: fine recall %.3f != 1 (unsound!)" % (name, frec)
        assert fprec > cprec + 1e-6, "%s: fine prec %.3f !> coarse %.3f" % (name, fprec, cprec)
    assert dp > 0.99 and dr > 0.95, \
        "extremal model should be ~exact under does-influence (got P=%.3f R=%.3f)" % (dp, dr)
    assert p_inf > 0.99, "influence should localize the outlier (prec@1=%.3f)" % p_inf
    assert p_inf > p_base + 0.5, "influence should crush the random baseline"
    assert min(repro_recalls) > 0.999, "reproducibility mode must be sound (recall=1)"
    assert min(data_precisions) > 0.999, "data-lineage mode must be precise (precision=1)"
    print("\nOK  data/control-flow: sound (recall=1) and far more precise than "
          "coarse;\n    extremal model exact for does-influence; influence "
          "localizes the outlier (prec@1=%.3f)." % p_inf)
    return results


if __name__ == "__main__":
    main()
