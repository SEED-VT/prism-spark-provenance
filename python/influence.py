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

"""Influence of each input on an aggregate output.

Provenance says which inputs reached an output. Influence says how much the output
would move if an input were removed. It is defined as leave-one-out (LOO), the
difference between the aggregate over all inputs and the aggregate without input i.

For the decomposable aggregates (sum, count, avg, max, min), :func:`influence`
computes LOO exactly with a closed form in one O(n) pass. Any other aggregate falls
back to :func:`leave_one_out`, which recomputes the aggregate once per input. Users
can declare a closed form for their own aggregate with :func:`register_influence`.

Elements may be ``(id, value)`` pairs, bare values, or ``fineprov.Tagged`` scalars.
"""

__all__ = [
    "Influence", "influence", "leave_one_out", "register_influence",
    "CLOSED_FORMS",
]


# --------------------------------------------------------------------------- #
# Element adapter: accept (id, value) pairs or fineprov.Tagged scalars.
# --------------------------------------------------------------------------- #
def _as_pairs(elements):
    pairs = []
    for i, e in enumerate(elements):
        if isinstance(e, tuple) and len(e) == 2:
            pairs.append((e[0], e[1]))
        elif hasattr(e, "prov") and hasattr(e, "value"):      # fineprov.Tagged
            p = e.prov
            ident = next(iter(p)) if len(p) == 1 else (frozenset(p) or i)
            pairs.append((ident, e.value))
        else:
            pairs.append((i, e))                              # a bare value uses its index as id
    return pairs


# --------------------------------------------------------------------------- #
# Result.
# --------------------------------------------------------------------------- #
class Influence:
    """Per-input influence scores on an output, with the method that produced
    them (``closed-form`` or ``leave-one-out``)."""

    def __init__(self, scores, method):
        self.scores = dict(scores)            # id to float score
        self.method = method

    def of(self, ident):
        return self.scores.get(ident, 0.0)

    def ranked(self):
        """Return the ids sorted by descending absolute influence."""
        return sorted(self.scores, key=lambda k: abs(self.scores[k]), reverse=True)

    def top(self, k=1):
        r = self.ranked()
        return [(i, self.scores[i]) for i in r[:k]]

    def __repr__(self):
        body = ", ".join("%s=%.4g" % (k, v) for k, v in
                         sorted(self.scores.items(), key=lambda kv: -abs(kv[1])))
        return "Influence(%s | %s)" % (body, self.method)


# --------------------------------------------------------------------------- #
# Leave-one-out over an arbitrary aggregate, the reference the closed forms match.
# --------------------------------------------------------------------------- #
def leave_one_out(elements, agg, distance=None):
    """Influence of each element as ``distance(agg(all), agg(all - i))``.

    ``agg`` maps a list of raw values to a scalar. ``distance`` defaults to the
    signed difference ``full - without_i``, so removing a positive sum contributor
    yields a positive influence."""
    pairs = _as_pairs(elements)
    vals = [v for _, v in pairs]
    if distance is None:
        distance = lambda full, without: full - without
    full = agg(vals)
    scores = {}
    for i, (ident, _) in enumerate(pairs):
        without = agg(vals[:i] + vals[i + 1:]) if len(vals) > 1 else agg([])
        scores[ident] = float(distance(full, without))
    return Influence(scores, "leave-one-out")


# --------------------------------------------------------------------------- #
# Closed forms. Each returns the same scores as LOO in one O(n) pass.
# --------------------------------------------------------------------------- #
def _sum_influence(pairs):
    # Removing i drops the sum by value_i.
    return {ident: float(v) for ident, v in pairs}


def _count_influence(pairs):
    # Removing any one element drops the count by exactly 1.
    return {ident: 1.0 for ident, _ in pairs}


def _avg_influence(pairs):
    # Removing i moves the mean by (value_i - mean) / (n - 1), using the LOO sign
    # convention full - without.
    n = len(pairs)
    if n <= 1:
        return {ident: 0.0 for ident, _ in pairs}
    mean = sum(v for _, v in pairs) / n
    return {ident: float((v - mean) / (n - 1)) for ident, v in pairs}


def _max_influence(pairs):
    # Removing the arg-max drops the max to the runner-up. Removing any other
    # element leaves the max unchanged.
    if not pairs:
        return {}
    order = sorted(range(len(pairs)), key=lambda i: pairs[i][1], reverse=True)
    top = order[0]
    runner = pairs[order[1]][1] if len(pairs) > 1 else pairs[top][1]
    scores = {ident: 0.0 for ident, _ in pairs}
    scores[pairs[top][0]] = float(pairs[top][1] - runner)
    return scores


def _min_influence(pairs):
    if not pairs:
        return {}
    order = sorted(range(len(pairs)), key=lambda i: pairs[i][1])
    bot = order[0]
    runner = pairs[order[1]][1] if len(pairs) > 1 else pairs[bot][1]
    scores = {ident: 0.0 for ident, _ in pairs}
    scores[pairs[bot][0]] = float(pairs[bot][1] - runner)   # signed, min - runner <= 0
    return scores


# Maps a name to (closed form over pairs, raw aggregate used for the LOO reference).
CLOSED_FORMS = {
    "sum": (_sum_influence, sum),
    "count": (_count_influence, len),
    "avg": (_avg_influence, lambda xs: sum(xs) / len(xs) if xs else 0.0),
    "mean": (_avg_influence, lambda xs: sum(xs) / len(xs) if xs else 0.0),
    "max": (_max_influence, lambda xs: max(xs) if xs else 0.0),
    "min": (_min_influence, lambda xs: min(xs) if xs else 0.0),
}


def register_influence(name, closed_form, aggregate=None):
    """Declare influence semantics for a user aggregate.

    ``closed_form(pairs)`` takes ``[(id, value)]`` and returns ``{id: score}``.
    ``aggregate`` is the optional raw reducer, recorded for cross-checking against
    :func:`leave_one_out`.
    """
    CLOSED_FORMS[name] = (closed_form, aggregate)


def influence(elements, agg):
    """Influence of each element on ``agg`` over them.

    ``agg`` is a registered name, computed by its closed form in O(n), or a
    callable aggregate, computed by :func:`leave_one_out` with n recomputations.
    """
    pairs = _as_pairs(elements)
    if isinstance(agg, str):
        if agg not in CLOSED_FORMS:
            raise KeyError("no influence model for %r; register one or pass a "
                           "callable aggregate for the LOO fallback" % agg)
        closed, _ = CLOSED_FORMS[agg]
        return Influence(closed(pairs), "closed-form")
    return leave_one_out(elements, agg)
