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

"""Read aggregate functions from a DataFrame's query plan for influence.

The functions here parse the analyzed Catalyst plan, so the user does not name the
aggregate. They keep the ``influence`` module free of Spark dependencies.
"""

import re

from influence import influence, leave_one_out

# Matched against function calls on a Catalyst Aggregate line, e.g.
# `Aggregate [k#0L], [k#0L, sum(v#1) AS __agg#2]`.
_AGG_TOKENS = [
    ("average", "avg"), ("avg", "avg"), ("mean", "avg"),
    ("sum", "sum"), ("count", "count"), ("max", "max"), ("min", "min"),
    ("median", "median"), ("percentile", "percentile"),
    ("percentile_approx", "percentile"), ("approx_percentile", "percentile"),
]


def _agg_name(fn, args):
    """Return the influence name for one aggregate call.

    A percentile carries its fraction, e.g. ``percentile(v#3, 0.9, 1, 0, 0)`` becomes
    ``percentile:0.9``."""
    name = dict(_AGG_TOKENS)[fn]
    if name == "percentile":
        name += ":" + args.split(",")[1].strip()
    return name


def aggregate_of(df):
    """Return the first recognized aggregate name in ``df``'s plan, or None.

    Names are those in ``_AGG_TOKENS``, such as ``sum``, ``avg``, ``count``, ``max``,
    and ``min``."""
    try:
        plan = df._jdf.queryExecution().analyzed().toString()
    except Exception:
        return None
    agg_lines = [ln for ln in plan.split("\n") if "Aggregate" in ln]
    hay = (" ".join(agg_lines) if agg_lines else plan).lower()
    for token, key in _AGG_TOKENS:
        # Require a call `token(` so an alias like `sum_agg` does not match.
        if re.search(r"\b" + token + r"\s*\(", hay):
            return key
    return None


def aggregate_slots(df):
    """Return ``(output_column, agg_type)`` for each aggregate in ``df``'s outermost level.

    The order matches the slots in which capture records contributions."""
    try:
        plan = df._jdf.queryExecution().analyzed().toString()
    except Exception:
        return []
    by_tok = dict(_AGG_TOKENS)
    for ln in plan.split("\n"):
        if "Aggregate" not in ln:
            continue
        slots = []
        for m in re.finditer(r"(\w+)\(([^()]*)\)\s+AS\s+(\w+)#\d+", ln):
            fn, args, name = m.group(1).lower(), m.group(2), m.group(3)
            if fn in by_tok:
                slots.append((name, _agg_name(fn, args)))
        if slots:
            return slots
    return []


def aggregate_levels(df):
    """Return every aggregate level in ``df``'s plan, outermost first.

    Each level is a list of ``(output_column, agg_type)`` in capture order. Plan text
    lists the outermost ``Aggregate`` first, so plan order is level order."""
    try:
        plan = df._jdf.queryExecution().analyzed().toString()
    except Exception:
        return []
    by_tok = dict(_AGG_TOKENS)
    levels = []
    for ln in plan.split("\n"):
        if "Aggregate" not in ln:
            continue
        slots = []
        for m in re.finditer(r"(\w+)\(([^()]*)\)\s+AS\s+(\w+)#\d+", ln):
            fn, args, name = m.group(1).lower(), m.group(2), m.group(3)
            if fn in by_tok:
                slots.append((name, _agg_name(fn, args)))
        if slots:
            levels.append(slots)
    return levels


def column_level(df, column):
    """Locate an aggregate output column across all group-bys.

    Returns ``(depth, slot, agg_type)``, where depth counts aggregates inward from the
    outermost and slot is the position within that level. Returns None if not found."""
    for depth, level in enumerate(aggregate_levels(df)):
        for slot, (name, agg) in enumerate(level):
            if name == column:
                return depth, slot, agg
    return None


def influence_auto(df, elements, fallback_aggregate=None):
    """Compute the influence of ``elements`` on ``df``'s output, with the aggregate from the plan.

    An unrecognized aggregate uses leave-one-out over ``fallback_aggregate`` if given,
    and raises KeyError otherwise. Returns ``(Influence, detected_name_or_None)``."""
    name = aggregate_of(df)
    if name is not None:
        return influence(elements, name), name
    if fallback_aggregate is not None:
        return leave_one_out(elements, fallback_aggregate), None
    raise KeyError("aggregate not recognized in plan and no fallback given")
