#!/usr/bin/env python3
"""Checks that closed-form influence equals leave-one-out re-execution for every
decomposable aggregate.

Prism computes per-record influence in one O(n) pass without re-running the job. This
script draws many random groups of varying size, scale, sign, and ties, computes both the
closed form and the leave-one-out value, and compares them element by element. It is pure
Python, since the closed forms do not depend on Spark.

  python3 benchmarks/validate_influence.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from influence import leave_one_out, CLOSED_FORMS


class LCG:
    """Small reproducible PRNG that avoids a numpy dependency."""
    def __init__(self, seed):
        self.s = seed & 0x7FFFFFFF

    def rand(self):
        self.s = (1103515245 * self.s + 12345) & 0x7FFFFFFF
        return self.s / 0x7FFFFFFF

    def uniform(self, a, b):
        return a + (b - a) * self.rand()

    def randint(self, a, b):
        return a + int(self.rand() * (b - a + 1))


AGGS = ["sum", "count", "avg", "max", "min"]
TRIALS = 20000


def run():
    rng = LCG(20260713)
    print("%-6s %8s %13s %13s %10s %8s" %
          ("agg", "trials", "max|CF-LOO|", "mean|CF-LOO|", "top-1", "exact"))
    print("-" * 64)
    overall = 0.0
    for agg in AGGS:
        closed_fn, raw_agg = CLOSED_FORMS[agg]
        maxerr = mean_num = 0.0
        nelem = ntrials = top1 = exact = 0
        for _ in range(TRIALS):
            n = rng.randint(2, 40)                       # a single-member group is degenerate
            scale = [1.0, 100.0, 1e6][rng.randint(0, 2)]  # spans six orders of magnitude
            vals = [round(rng.uniform(-scale, scale), 3) for _ in range(n)]
            if rng.rand() < 0.3:                          # ties stress the max/min margin
                vals[rng.randint(0, n - 1)] = vals[rng.randint(0, n - 1)]
            pairs = list(enumerate(vals))

            cf = closed_fn(pairs)                         # closed form, O(n)
            loo = leave_one_out(pairs, raw_agg).scores    # oracle with n recomputations

            terr = 0.0
            for i, _ in pairs:
                e = abs(cf.get(i, 0.0) - loo.get(i, 0.0))
                terr = max(terr, e)
                mean_num += e
                nelem += 1
            maxerr = max(maxerr, terr)
            if terr <= 1e-6 * max(1.0, scale):
                exact += 1
            # Top-1 by absolute influence, the record a developer inspects first.
            tc = max(cf, key=lambda k: abs(cf[k]))
            tl = max(loo, key=lambda k: abs(loo[k]))
            if abs(cf[tc] - loo.get(tc, 0.0)) <= 1e-6 * max(1.0, scale) and \
               abs(loo[tl]) <= abs(loo.get(tc, 0.0)) + 1e-6 * max(1.0, scale):
                top1 += 1
            ntrials += 1
        overall = max(overall, maxerr)
        print("%-6s %8d %13.2e %13.2e %9.2f%% %7.2f%%" %
              (agg, ntrials, maxerr, mean_num / max(1, nelem),
               100.0 * top1 / ntrials, 100.0 * exact / ntrials))
    print("-" * 64)
    print("overall max |closed-form - leave-one-out| = %.2e (relative to value scale)" % overall)
    print("RESULT: %s" % ("PASS -- closed form matches LOO to float tolerance"
                          if overall < 1e-3 else "FAIL"))


if __name__ == "__main__":
    run()
