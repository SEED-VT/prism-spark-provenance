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

"""Unit tests for the fineprov UDF provenance tracer. They do not need Spark."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fineprov import (trace, tag, tag_row, taglist, raw, prov, lineage,
                      data_prov, control_prov, regime, register_model)


def test_scalar_arithmetic_is_field_level():
    @trace
    def f(row):
        return row["a"] * 2 + row["b"]

    out = f(tag_row({"a": 10, "b": 3}, prefix="r1."))
    assert raw(out) == 23
    # The constant contributes nothing, so the result depends on exactly a and b.
    assert prov(out) == {"r1.a", "r1.b"}
    assert regime(out) == "exact"


def test_unused_field_not_in_provenance():
    @trace
    def f(row):
        return row["a"] + 1            # b is read into the row but never used

    out = f(tag_row({"a": 5, "b": 99}, prefix="r."))
    assert raw(out) == 6
    assert prov(out) == {"r.a"}        # b is excluded at field level


def test_conditional_threads_control_dependency():
    # Unit conversion where inches are wrongly scaled as feet.
    @trace
    def to_mm(row):
        v = row["value"]
        return v if row["unit"] == "mm" else v * 304.8

    bad = to_mm(tag_row({"value": 90, "unit": "in"}, prefix="r7."))
    assert abs(raw(bad) - 27432.0) < 1e-6
    # value flows as data, and unit is a control dependency because it picked the branch.
    assert prov(bad) == {"r7.value", "r7.unit"}

    good = to_mm(tag_row({"value": 90, "unit": "mm"}, prefix="r8."))
    assert raw(good) == 90
    assert prov(good) == {"r8.value", "r8.unit"}


def test_true_lineage_vs_reproducibility():
    # In `x if x > y else y`, x is data and the test makes both x and y control.
    @trace
    def pick(row):
        x = row["x"]
        y = row["y"]
        return x if x > y else y

    out = pick(tag_row({"x": 5, "y": 3}, prefix="r."))
    assert raw(out) == 5
    # Data lineage holds only x, whose value flowed into the output.
    assert lineage(out) == {"r.x"}
    assert data_prov(out) == {"r.x"}
    # y steered the branch without flowing in, so it is only a control dependency.
    assert control_prov(out) == {"r.x", "r.y"}      # the test x>y reads both
    # Reproducibility lineage holds everything needed to re-derive the output.
    assert prov(out) == {"r.x", "r.y"}


def test_extremal_data_lineage_is_the_winner():
    # The data lineage of a max is the winning element, as the extremal model gives.
    @trace
    def hottest(row):
        return max(row["t"])

    row = tag_row({"t": []}, prefix="r.")
    row.value["t"] = taglist([3, 9, 1], prefix="r.t[")
    out = hottest(row)
    assert lineage(out) == {"r.t[1"}                 # only the max element's value


def test_null_source_is_traced():
    # A None input propagates through arithmetic as SQL NULL, and the provenance
    # still reaches the null source field.
    @trace
    def f(row):
        return row["a"] + row["b"] * 2

    out = f(tag_row({"a": 10, "b": None}, prefix="r."))
    assert raw(out) is None                    # NULL propagates without an error
    assert "r.b" in lineage(out)               # the null source is in the trace
    assert lineage(out) == {"r.a", "r.b"}


def test_null_in_branch_test_takes_else():
    @trace
    def f(row):
        return row["x"] if row["g"] > 5 else row["y"]

    # A NULL g makes the comparison unknown, so the else branch runs with g as control.
    out = f(tag_row({"x": 1, "y": 2, "g": None}, prefix="r."))
    assert raw(out) == 2
    assert lineage(out) == {"r.y"}
    assert "r.g" in control_prov(out)


def test_chained_compare_shortcircuit_provenance():
    @trace
    def f(row):
        return row["a"] < row["b"] < row["c"]

    # a < b is False, so c is never evaluated and is not in the provenance.
    out = f(tag_row({"a": 5, "b": 1, "c": 100}, prefix="r."))
    assert raw(out) is False
    assert prov(out) == {"r.a", "r.b"}


def test_boolop_is_lazy_and_tracks_decider():
    @trace
    def f(row):
        return row["a"] and row["b"]

    out = f(tag_row({"a": 0, "b": 7}, prefix="r."))
    assert raw(out) == 0                 # short-circuits on a
    assert prov(out) == {"r.a"}          # b never evaluated


def test_extremal_model_attributes_to_winner_only():
    @trace
    def hottest(row):
        return max(row["temps"])

    row = tag_row({"temps": [3, 9, 1]}, prefix="r.")
    # Tag the inner list elements so the model can name the winning element.
    row.value["temps"] = taglist([3, 9, 1], prefix="r.temps[")
    out = hottest(row)
    assert raw(out) == 9
    assert prov(out) == {"r.temps[1"}    # only the max element, not all three
    assert regime(out) == "model"


def test_sum_model_is_reduce_all():
    @trace
    def total(row):
        return sum(row["xs"])

    row = tag_row({"xs": []}, prefix="r.")
    row.value["xs"] = taglist([2, 4, 6], prefix="r.xs[")
    out = total(row)
    assert raw(out) == 12
    assert prov(out) == {"r.xs[0", "r.xs[1", "r.xs[2"}
    assert regime(out) == "model"


def test_comprehension_threads_each_element():
    @trace
    def doubled_first(row):
        ys = [x * 2 for x in row["xs"]]
        return ys[0]

    row = tag_row({"xs": []}, prefix="r.")
    row.value["xs"] = taglist([10, 20], prefix="r.xs[")
    out = doubled_first(row)
    assert raw(out) == 20
    assert prov(out) == {"r.xs[0"}       # only element 0 reached the output


def test_unmodeled_call_falls_back_to_coarse_union():
    def blackbox(a, b):                  # stands in for a C or numpy call
        return a * 1000 + b

    @trace
    def f(row):
        return blackbox(row["a"], row["b"])

    out = f(tag_row({"a": 2, "b": 3}, prefix="r."))
    assert raw(out) == 2003
    assert prov(out) == {"r.a", "r.b"}   # union of the arguments
    assert regime(out) == "coarse"


def test_registering_a_model_recovers_exactness():
    def first(seq):
        return seq[0]

    register_model(first, lambda rf, args, kw: (
        raw(list(args[0].value)[0]), prov(list(args[0].value)[0]), "model"))

    @trace
    def f(row):
        return first(row["xs"])

    row = tag_row({"xs": []}, prefix="r.")
    row.value["xs"] = taglist([42, 7], prefix="r.xs[")
    out = f(row)
    assert raw(out) == 42
    assert prov(out) == {"r.xs[0"}       # the model gives element-level provenance
    assert regime(out) == "model"


def test_augassign_accumulates_provenance():
    @trace
    def f(row):
        t = row["a"]
        t += row["b"]
        t += row["c"]
        return t

    out = f(tag_row({"a": 1, "b": 2, "c": 3}, prefix="r."))
    assert raw(out) == 6
    assert prov(out) == {"r.a", "r.b", "r.c"}


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print("ok  " + fn.__name__)
    print("\n%d passed" % len(fns))
