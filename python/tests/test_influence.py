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

"""Unit tests for influence. They do not need Spark.

The closed forms must equal leave-one-out exactly for the decomposable aggregates.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from influence import influence, leave_one_out, register_influence, CLOSED_FORMS
from fineprov import taglist


def _approx(a, b, eps=1e-9):
    return abs(a - b) <= eps


def _same(closed, loo, ids):
    return all(_approx(closed.of(i), loo.of(i)) for i in ids)


def test_sum_closed_form_equals_loo():
    elems = [("a", 10.0), ("b", 3.0), ("c", -4.0)]
    ids = [i for i, _ in elems]
    c = influence(elems, "sum")
    l = leave_one_out(elems, sum)
    assert c.method == "closed-form" and l.method == "leave-one-out"
    assert _same(c, l, ids)
    assert c.of("a") == 10.0 and c.of("c") == -4.0


def test_avg_closed_form_equals_loo():
    elems = [("a", 2.0), ("b", 4.0), ("c", 6.0), ("d", 8.0)]
    ids = [i for i, _ in elems]
    agg = lambda xs: sum(xs) / len(xs)
    assert _same(influence(elems, "avg"), leave_one_out(elems, agg), ids)


def test_count_closed_form_equals_loo():
    elems = [("a", 7.0), ("b", 1.0), ("c", 9.0)]
    ids = [i for i, _ in elems]
    assert _same(influence(elems, "count"), leave_one_out(elems, len), ids)


def test_max_closed_form_equals_loo():
    elems = [("a", 3.0), ("b", 9.0), ("c", 1.0), ("d", 8.0)]
    ids = [i for i, _ in elems]
    c = influence(elems, "max")
    l = leave_one_out(elems, lambda xs: max(xs))
    assert _same(c, l, ids)
    # Only the arg-max has nonzero influence, equal to max minus the runner-up.
    assert c.of("b") == 1.0
    assert all(c.of(i) == 0.0 for i in ("a", "c", "d"))


def test_min_closed_form_equals_loo():
    elems = [("a", 3.0), ("b", -2.0), ("c", 5.0)]
    ids = [i for i, _ in elems]
    assert _same(influence(elems, "min"), leave_one_out(elems, lambda xs: min(xs)), ids)


def test_influence_consumes_fineprov_tagged():
    # Influence uses the provenance ids of fineprov.Tagged scalars directly.
    elems = list(taglist([10.0, 50.0, 5.0], prefix="r.snow[").value)
    inf = influence(elems, "max")
    assert inf.top(1) == [("r.snow[1", 40.0)]      # element 1 drives the max


def test_ranked_orders_by_absolute_influence():
    elems = [("a", 1.0), ("b", -100.0), ("c", 20.0)]
    assert influence(elems, "sum").ranked() == ["b", "c", "a"]


def test_programmable_custom_aggregate_via_loo():
    # An unregistered aggregate (sum of squares) uses leave-one-out.
    ssq = lambda xs: sum(x * x for x in xs)
    elems = [("a", 1.0), ("b", 2.0), ("c", 3.0)]
    inf = influence(elems, ssq)
    assert inf.method == "leave-one-out"
    # Removing c (value 3) drops the sum of squares by 9.
    assert _approx(inf.of("c"), 9.0)


def test_register_closed_form_for_user_aggregate():
    # Removing x_i drops the sum of squares by x_i squared.
    register_influence("sumsq", lambda pairs: {i: float(v * v) for i, v in pairs},
                       aggregate=lambda xs: sum(x * x for x in xs))
    elems = [("a", 1.0), ("b", 2.0), ("c", 3.0)]
    closed = influence(elems, "sumsq")
    loo = leave_one_out(elems, CLOSED_FORMS["sumsq"][1])
    assert closed.method == "closed-form"
    assert _same(closed, loo, ["a", "b", "c"])


def test_loo_cost_vs_closed_form_calls():
    # Count aggregate invocations for leave-one-out and the closed form.
    calls = {"n": 0}

    def counting_sum(xs):
        calls["n"] += 1
        return sum(xs)

    elems = [("e%d" % i, float(i)) for i in range(200)]
    leave_one_out(elems, counting_sum)
    assert calls["n"] == len(elems) + 1          # n removals plus the full run

    calls["n"] = 0
    influence(elems, "sum")                        # the closed form runs no aggregate
    assert calls["n"] == 0


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("ok  " + fn.__name__)
    print("\n%d passed" % len(fns))
