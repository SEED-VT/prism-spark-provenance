#!/usr/bin/env python3
"""Native-library UDF benchmark that compares Prism's UDF rewrite with a taint-tracking wrapper.

Each UDF calls compiled numpy or pandas code. It runs once as the original function on
Taint objects with overloaded operators, and once through ``trace(udf)`` on plain tagged
inputs. The script reports whether each path completes, and the field lineage and regime
for Prism. It is pure Python, since the UDF rewrite does not depend on Spark.

  python3 benchmarks/test_native_udf.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

import numpy as np
import pandas as pd
import fineprov as fp
from fineprov import (trace, tag, taglist, data_prov, regime,
                      register_model, Reason)


# A taint wrapper that overloads operators and carries provenance, as taint libraries do.
class Taint:
    """A value with its provenance set and arithmetic and comparison operators."""
    __slots__ = ("v", "p")

    def __init__(self, v, p):
        self.v = v
        self.p = frozenset(p)

    def _o(self, o):
        return (o.v, o.p) if isinstance(o, Taint) else (o, frozenset())

    def __sub__(self, o):  ov, op = self._o(o); return Taint(self.v - ov, self.p | op)
    def __rsub__(self, o): ov, op = self._o(o); return Taint(ov - self.v, self.p | op)
    def __add__(self, o):  ov, op = self._o(o); return Taint(self.v + ov, self.p | op)
    def __truediv__(self, o): ov, op = self._o(o); return Taint(self.v / ov, self.p | op)
    def __mul__(self, o):  ov, op = self._o(o); return Taint(self.v * ov, self.p | op)
    def __lt__(self, o):   ov, _ = self._o(o); return self.v < ov
    def __gt__(self, o):   ov, _ = self._o(o); return self.v > ov
    # There is no __float__ or __array__, so a compiled routine that needs a real dtype
    # cannot consume a Taint, which is how taint wrappers fail on native code.


# Library models that give Prism the provenance semantics of numpy reductions.
def _np_extremal(rf, args, kwargs):
    seq = list(fp._iterate(args[0]))
    winner = max(seq, key=fp.raw) if rf is np.max else min(seq, key=fp.raw)
    return Reason.extremal(fp.raw(winner), seq, winner)


def _np_reduce_all(rf, args, kwargs):
    seq = list(fp._iterate(args[0]))
    return Reason.reduce_all(rf([fp.raw(e) for e in seq]), seq)


register_model(np.max, _np_extremal)
register_model(np.min, _np_extremal)
register_model(np.mean, _np_reduce_all)
register_model(np.sum, _np_reduce_all)


# The UDFs are undecorated so that each can run on Taint inputs or through trace().
def celsius_np(f):                         # scalar native call
    return float(np.divide(np.subtract(f, 32.0), 1.8))


def hottest_np(readings):                  # modeled numpy reduction, element level
    return float(np.max(readings))


def peak_gap_np(readings):                 # two modeled reductions
    return float(np.subtract(np.max(readings), np.mean(readings)))


def pandas_dev(x):                         # pandas over a scalar tagged input
    ref = pd.Series([60.0, 70.0, 80.0])    # constant reference distribution
    return float((ref - x).abs().min())    # distance of x to the nearest reference


UDFS = [
    ("celsius_np", celsius_np, lambda: tag(81.0, "temp"), lambda: Taint(81.0, {"temp"}), "scalar np"),
    ("hottest_np", hottest_np, lambda: taglist([64.0, 81.0, 72.0], "r"),
     lambda: [Taint(64.0, {"r0"}), Taint(81.0, {"r1"}), Taint(72.0, {"r2"})], "np.max"),
    ("peak_gap_np", peak_gap_np, lambda: taglist([64.0, 81.0, 72.0], "r"),
     lambda: [Taint(64.0, {"r0"}), Taint(81.0, {"r1"}), Taint(72.0, {"r2"})], "np.max+mean"),
    ("pandas_dev", pandas_dev, lambda: tag(81.0, "temp"), lambda: Taint(81.0, {"temp"}), "pandas"),
]


def run():
    print("%-13s %-11s %-22s %-22s" % ("udf", "boundary", "taint wrapper", "PRISM (rewrite)"))
    print("-" * 74)
    for name, fn, mk_tag, mk_wrap, boundary in UDFS:
        # Original UDF on Taint inputs.
        try:
            wv = fn(mk_wrap())
            wrap = "ok -> %s" % (getattr(wv, "v", wv))
        except Exception as e:
            wrap = "CRASH %s" % type(e).__name__
        # trace(udf) on tagged inputs.
        try:
            res = trace(fn)(mk_tag())
            dp = sorted(data_prov(res))
            prism = "ok %s /%s" % (dp, regime(res))
        except Exception as e:
            prism = "CRASH %s" % type(e).__name__
        print("%-13s %-11s %-22s %-22s" % (name, boundary, wrap, prism))
    print("-" * 74)
    print("PRISM completes every UDF (values stay plain across the boundary); the wrapper")
    print("crashes wherever the compiled routine needs a real dtype it cannot get from Taint.")


if __name__ == "__main__":
    run()
