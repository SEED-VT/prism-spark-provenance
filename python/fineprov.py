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

"""Field-level provenance through Python UDFs by syntax-tree rewriting.

:func:`trace` rewrites every operation site in a UDF's source (binary and unary
operators, comparisons, subscripts, attribute reads, calls, conditional
expressions, and iteration) into a call to a helper. Each helper computes the
real value and combines the provenance of its operands. Values flow as an inert
``Tagged(value, dprov, cprov, regime)`` carrier with no operator overloads, so
every operation is covered by the rewrite itself.

Provenance is split into data dependencies (inputs whose value flowed into the
result) and control dependencies (inputs that only selected it, such as a branch
test or a subscript index).

Calls into library or C code are handled by a model registry that declares each
function's provenance semantics. For example, ``max`` attributes its result to the
winning element alone. An unmodeled call falls back to the union of its arguments,
and every result records the regime (``exact``, ``model``, or ``coarse``) that
produced it.

Statement-level ``if`` and ``while`` bodies do not add control dependencies in the
per-row tracer. Only the conditional expression ``a if t else b`` threads its test.
Calls to other pure-Python functions are not followed, so they use a model or the fallback.

Example::

    from fineprov import trace, tag_row, prov

    @trace
    def to_mm(row):
        v = row["value"]
        return v if row["unit"] == "mm" else v * 304.8

    out = to_mm(tag_row({"value": 90, "unit": "in"}, prefix="r7."))
    prov(out)           # frozenset({'r7.value', 'r7.unit'})
"""

import ast
import inspect
import operator
import textwrap

__all__ = [
    "Tagged", "trace", "tag", "tag_row", "taglist", "raw", "prov", "lineage",
    "data_prov", "control_prov", "regime", "register_model", "Reason",
]


# --------------------------------------------------------------------------- #
# The inert carrier. It defines no operator methods because the rewritten UDF
# routes every operation through a helper below.
#
# dprov holds data dependencies, the atoms whose value flowed into this value.
# cprov holds control dependencies, the atoms that steered this value (a branch
# test or a subscript index) without their value flowing in. Data lineage is
# dprov, and reproducibility lineage is the union of both.
# --------------------------------------------------------------------------- #
class Tagged:
    __slots__ = ("value", "dprov", "cprov", "regime")

    def __init__(self, value, dprov, cprov=frozenset(), regime="exact"):
        self.value = value
        self.dprov = frozenset(dprov)
        self.cprov = frozenset(cprov)
        # "exact" means tracked through pure Python, "model" means resolved by a
        # registered library model, and "coarse" means the union-of-arguments fallback.
        self.regime = regime

    @property
    def prov(self):
        """Reproducibility provenance: data + control (the default question)."""
        return self.dprov | self.cprov

    def __repr__(self):
        c = (" |ctrl %s" % sorted(self.cprov)) if self.cprov else ""
        return "Tagged(%r, %s%s, %s)" % (
            self.value, sorted(self.dprov), c, self.regime)


def raw(x):
    """Underlying concrete value of a (possibly untracked) value."""
    return x.value if isinstance(x, Tagged) else x


def data_prov(x):
    """Data-dependency ids: atoms whose value flowed into ``x``."""
    return x.dprov if isinstance(x, Tagged) else frozenset()


def control_prov(x):
    """Control-dependency ids: atoms that steered ``x`` (branch tests, indices)."""
    return x.cprov if isinstance(x, Tagged) else frozenset()


def lineage(x):
    """Data lineage of ``x``, the inputs that produced its value.

    It excludes the control dependencies that only selected the value."""
    return x.dprov if isinstance(x, Tagged) else frozenset()


def prov(x):
    """Reproducibility provenance of ``x``, the data and control ids needed to re-derive it."""
    return (x.dprov | x.cprov) if isinstance(x, Tagged) else frozenset()


def regime(x):
    """Granularity regime that produced ``x``, one of ``exact``, ``model``, or ``coarse``."""
    return x.regime if isinstance(x, Tagged) else "exact"


def _merge_regime(*vals):
    """The weakest regime among the operands wins (exact < model < coarse)."""
    order = {"exact": 0, "model": 1, "coarse": 2}
    worst = max((order[regime(v)] for v in vals), default=0)
    return ("exact", "model", "coarse")[worst]


def _union(*provs):
    out = frozenset()
    for p in provs:
        out |= p
    return out


# --------------------------------------------------------------------------- #
# Input tagging helpers.
# --------------------------------------------------------------------------- #
def tag(value, ident):
    """Tag a single value with a provenance id (e.g. a source row/field id)."""
    return Tagged(value, frozenset([ident]))


def tag_row(d, prefix=""):
    """Tag a dict-like row: each field gets id ``f"{prefix}{field}"``.

    Subscripting the result (``row["x"]``) yields a ``Tagged`` carrying only
    that field's id. ``prefix`` carries the source row id, e.g. ``f"r{rid}."``.
    """
    inner = {k: Tagged(v, frozenset([prefix + str(k)])) for k, v in d.items()}
    return Tagged(inner, frozenset())


def taglist(values, prefix=""):
    """Tag each element of a list with id ``f"{prefix}{index}"``.

    Lets aggregate models (``max``/``min``/``sum``) attribute the result to the
    individual elements that produced it.
    """
    elems = [Tagged(v, frozenset([prefix + str(i)])) for i, v in enumerate(values)]
    return Tagged(elems, frozenset())


# --------------------------------------------------------------------------- #
# Runtime operation helpers called by the rewritten UDF.
# --------------------------------------------------------------------------- #
_BIN = {
    "add": operator.add, "sub": operator.sub, "mul": operator.mul,
    "truediv": operator.truediv, "floordiv": operator.floordiv,
    "mod": operator.mod, "pow": operator.pow, "matmul": operator.matmul,
    "and": operator.and_, "or": operator.or_, "xor": operator.xor,
    "lshift": operator.lshift, "rshift": operator.rshift,
}
_UN = {"pos": operator.pos, "neg": operator.neg,
       "invert": operator.invert, "not": operator.not_}
_CMP = {
    "eq": operator.eq, "ne": operator.ne, "lt": operator.lt, "le": operator.le,
    "gt": operator.gt, "ge": operator.ge, "is": operator.is_,
    "isnot": operator.is_not, "in": lambda a, b: a in b,
    "notin": lambda a, b: a not in b,
}


def _D(*xs):
    return _union(*[data_prov(x) for x in xs])


def _C(*xs):
    return _union(*[control_prov(x) for x in xs])


def _bin(op, a, b):
    # Both operands are data dependencies, and their control dependencies carry
    # through. None propagates as SQL NULL but keeps the provenance of its source.
    ra, rb = raw(a), raw(b)
    val = None if (ra is None or rb is None) else _BIN[op](ra, rb)
    return Tagged(val, _D(a, b), _C(a, b), _merge_regime(a, b))


def _un(op, a):
    ra = raw(a)
    val = None if ra is None else _UN[op](ra)
    return Tagged(val, _D(a), _C(a), regime(a))


def _cmp(ops, vals):
    # Chained comparisons short-circuit. The evaluated operands are data
    # dependencies of the result, and _ifexp turns them into control dependencies
    # when the result is used as a branch test.
    res = True
    dp = frozenset()
    cp = frozenset()
    parts = [vals[0]]
    for i, op in enumerate(ops):
        a, b = vals[i], vals[i + 1]
        dp = _union(dp, _D(a, b))
        cp = _union(cp, _C(a, b))
        parts.append(b)
        ra, rb = raw(a), raw(b)
        if (ra is None or rb is None) and op not in ("is", "isnot"):
            res = None              # a comparison with SQL NULL is unknown
            break
        if not _CMP[op](ra, rb):
            res = False
            break
    return Tagged(res, dp, cp, _merge_regime(*parts))


def _bool(op, thunks):
    # Lazy: thunks preserve Python's short-circuit and value-returning semantics.
    dp = frozenset()
    cp = frozenset()
    last = None
    for t in thunks:
        v = t()
        last = v
        dp = _union(dp, _D(v))
        cp = _union(cp, _C(v))
        if (op == "and") != bool(raw(v)):      # "and" stops on falsy, "or" on truthy
            return Tagged(raw(v), dp, cp, regime(v))
    return Tagged(raw(last), dp, cp, regime(last))


def _sub(c, i):
    # The element's value flows in as data, with the container. The index only
    # selects the element, so it is a control dependency.
    rc, ri = raw(c), raw(i)
    elem = None if (rc is None or ri is None) else rc[ri]
    dp = _union(_D(c, elem))
    cp = _union(_C(c, elem), _D(i), _C(i))
    return Tagged(raw(elem), dp, cp, _merge_regime(c, i, elem))


def _attr(o, name):
    return Tagged(getattr(raw(o), name), _D(o), _C(o), regime(o))


def _ifexp(test, body_thunk, else_thunk):
    r = body_thunk() if raw(test) else else_thunk()
    # The chosen branch is data. The test only selected the branch, so its
    # dependencies become control dependencies of the result.
    dp = _D(r)
    cp = _union(_C(r), _D(test), _C(test))
    return Tagged(raw(r), dp, cp, _merge_regime(r, test))


def _iterate(x):
    """Iterate a (possibly Tagged) iterable, pushing container provenance onto
    each element so data flow survives ``for``/comprehensions."""
    if isinstance(x, Tagged):
        cd, cc = x.dprov, x.cprov
        for e in x.value:
            if isinstance(e, Tagged):
                yield Tagged(e.value, _union(e.dprov, cd), _union(e.cprov, cc), e.regime)
            else:
                yield Tagged(e, cd, cc, x.regime)
    else:
        for e in x:
            yield e


# ----- library-model registry for calls outside pure Python ---------------- #
class Reason:
    """Convenience constructors for a model's ``(value, data_provenance, regime)`` result.

    The data provenance is the set of atoms whose value flows into the result.
    ``_call`` threads the arguments' control dependencies separately."""

    @staticmethod
    def reduce_all(value, args):
        """Attribute the result to every element, as for sum, len, or mean."""
        return value, _union(*[data_prov(a) for a in args]), "model"

    @staticmethod
    def extremal(value, args, winner):
        """Attribute the result to the winning element alone, as for max or min."""
        return value, data_prov(winner), "model"


_MODELS = {}


def register_model(fn, model):
    """Register provenance semantics for a boundary function ``fn``.

    ``model(raw_fn, tagged_args, tagged_kwargs)`` returns ``(value, provenance, regime)``.
    """
    _MODELS[fn] = model


def _model_max(rf, args, kwargs):
    seq = list(_iterate(args[0])) if len(args) == 1 else list(args)
    winner = rf(seq, key=raw)
    return Reason.extremal(raw(winner), seq, winner)


def _model_min(rf, args, kwargs):
    seq = list(_iterate(args[0])) if len(args) == 1 else list(args)
    winner = rf(seq, key=raw)
    return Reason.extremal(raw(winner), seq, winner)


def _model_sum(rf, args, kwargs):
    seq = list(_iterate(args[0]))
    return Reason.reduce_all(rf([raw(e) for e in seq]), seq)


def _model_len(rf, args, kwargs):
    x = args[0]
    seq = list(x.value) if isinstance(x, Tagged) else list(x)
    # Length depends on the existence of every element.
    return len(seq), _union(data_prov(x), *[data_prov(e) for e in seq]), "model"


def _model_passthrough(rf, args, kwargs):
    val = rf(*[raw(a) for a in args], **{k: raw(v) for k, v in kwargs.items()})
    return val, _union(*[data_prov(a) for a in args]), "model"


register_model(max, _model_max)
register_model(min, _model_min)
register_model(sum, _model_sum)
register_model(len, _model_len)
for _f in (abs, round, float, int, str):
    register_model(_f, _model_passthrough)


def _call(f, args, kwargs):
    rf = raw(f)
    # Control dependencies of the callee and arguments carry through any call.
    cc = _union(_C(f), *[_C(a) for a in args], *[_C(v) for v in kwargs.values()])
    model = _MODELS.get(rf)
    if model is not None:
        value, dp, reg = model(rf, args, kwargs)
        return Tagged(value, _union(_D(f), dp), cc, reg)
    # An unmodeled call conservatively depends on every argument's data, and the
    # result is marked coarse.
    rargs = [raw(a) for a in args]
    rkwargs = {k: raw(v) for k, v in kwargs.items()}
    value = rf(*rargs, **rkwargs)
    dp = _union(_D(f), *[_D(a) for a in args], *[_D(v) for v in kwargs.values()])
    return Tagged(value, dp, cc, "coarse" if dp else "exact")


_HELPERS = {
    "__fp_bin": _bin, "__fp_un": _un, "__fp_cmp": _cmp, "__fp_bool": _bool,
    "__fp_sub": _sub, "__fp_attr": _attr, "__fp_ifexp": _ifexp,
    "__fp_call": _call, "__fp_iter": _iterate,
}


# --------------------------------------------------------------------------- #
# AST rewriting.
# --------------------------------------------------------------------------- #
_BINOP_NAME = {
    ast.Add: "add", ast.Sub: "sub", ast.Mult: "mul", ast.Div: "truediv",
    ast.FloorDiv: "floordiv", ast.Mod: "mod", ast.Pow: "pow",
    ast.MatMult: "matmul", ast.BitAnd: "and", ast.BitOr: "or",
    ast.BitXor: "xor", ast.LShift: "lshift", ast.RShift: "rshift",
}
_UNOP_NAME = {ast.UAdd: "pos", ast.USub: "neg", ast.Invert: "invert", ast.Not: "not"}
_CMPOP_NAME = {
    ast.Eq: "eq", ast.NotEq: "ne", ast.Lt: "lt", ast.LtE: "le", ast.Gt: "gt",
    ast.GtE: "ge", ast.Is: "is", ast.IsNot: "isnot", ast.In: "in", ast.NotIn: "notin",
}


def _name(n):
    return ast.Name(id=n, ctx=ast.Load())


def _const(v):
    return ast.Constant(value=v)


def _call_node(fn, *args):
    return ast.Call(func=_name(fn), args=list(args), keywords=[])


def _thunk(expr):
    """Wrap an expression in a zero-arg lambda (for lazy helpers)."""
    empty = ast.arguments(posonlyargs=[], args=[], vararg=None, kwonlyargs=[],
                          kw_defaults=[], kwarg=None, defaults=[])
    return ast.Lambda(args=empty, body=expr)


class _Tracer(ast.NodeTransformer):
    def visit_BinOp(self, node):
        self.generic_visit(node)
        op = _BINOP_NAME.get(type(node.op))
        if op is None:
            return node
        return _call_node("__fp_bin", _const(op), node.left, node.right)

    def visit_UnaryOp(self, node):
        self.generic_visit(node)
        op = _UNOP_NAME.get(type(node.op))
        if op is None:
            return node
        return _call_node("__fp_un", _const(op), node.operand)

    def visit_Compare(self, node):
        self.generic_visit(node)
        ops = ast.List(elts=[_const(_CMPOP_NAME[type(o)]) for o in node.ops],
                       ctx=ast.Load())
        vals = ast.List(elts=[node.left] + list(node.comparators), ctx=ast.Load())
        return _call_node("__fp_cmp", ops, vals)

    def visit_BoolOp(self, node):
        self.generic_visit(node)
        op = "and" if isinstance(node.op, ast.And) else "or"
        thunks = ast.List(elts=[_thunk(v) for v in node.values], ctx=ast.Load())
        return _call_node("__fp_bool", _const(op), thunks)

    def visit_Subscript(self, node):
        self.generic_visit(node)
        if isinstance(node.slice, ast.Slice):       # leave a[i:j] alone
            return node
        idx = node.slice.value if isinstance(node.slice, ast.Index) else node.slice
        if not isinstance(node.ctx, ast.Load):      # only rewrite reads
            return node
        return _call_node("__fp_sub", node.value, idx)

    def visit_Attribute(self, node):
        self.generic_visit(node)
        if not isinstance(node.ctx, ast.Load):
            return node
        return _call_node("__fp_attr", node.value, _const(node.attr))

    def visit_IfExp(self, node):
        self.generic_visit(node)
        return _call_node("__fp_ifexp", node.test,
                          _thunk(node.body), _thunk(node.orelse))

    def visit_Call(self, node):
        self.generic_visit(node)
        if any(isinstance(a, ast.Starred) for a in node.args):
            return node                              # *args calls are not rewritten
        if any(k.arg is None for k in node.keywords):
            return node                              # **kwargs calls are not rewritten
        args = ast.List(elts=list(node.args), ctx=ast.Load())
        kw = ast.Dict(keys=[_const(k.arg) for k in node.keywords],
                      values=[k.value for k in node.keywords])
        return _call_node("__fp_call", node.func, args, kw)

    def visit_AugAssign(self, node):
        self.generic_visit(node)
        op = _BINOP_NAME.get(type(node.op))
        if op is None or not isinstance(node.target, ast.Name):
            return node
        load = ast.Name(id=node.target.id, ctx=ast.Load())
        return ast.Assign(targets=[node.target],
                          value=_call_node("__fp_bin", _const(op), load, node.value))

    def visit_For(self, node):
        self.generic_visit(node)
        node.iter = _call_node("__fp_iter", node.iter)
        return node

    def _wrap_comp(self, node):
        self.generic_visit(node)
        for gen in node.generators:
            gen.iter = _call_node("__fp_iter", gen.iter)
        return node

    visit_ListComp = _wrap_comp
    visit_SetComp = _wrap_comp
    visit_GeneratorExp = _wrap_comp

    def visit_DictComp(self, node):
        return self._wrap_comp(node)


class _Unanalyzable(Exception):
    pass


class _ColumnProv:
    """Static intra-procedural taint analysis over a UDF's syntax tree.

    Each parameter, or each constant subscript key of a row parameter, is an atom.
    The analyzer tracks which atoms reach each local as data and as control, and
    unions the result over all paths. It costs nothing at run time but is coarser
    than the per-row tracer."""

    def __init__(self, params):
        self.params = set(params)
        self.env = {p: (frozenset([p]), frozenset()) for p in params}
        self.data = set()
        self.control = set()
        self.regime = "exact"

    def run(self, fndef):
        for stmt in fndef.body:
            self.stmt(stmt, frozenset())

    def stmt(self, node, ctrl):
        if isinstance(node, ast.Return):
            if node.value is not None:
                d, c = self.expr(node.value)
                self.data |= d
                self.control |= c | ctrl
        elif isinstance(node, ast.Assign):
            d, c = self.expr(node.value)
            for t in node.targets:
                if isinstance(t, ast.Name):
                    self.env[t.id] = (d, c | ctrl)
                else:
                    raise _Unanalyzable()
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            pd, pc = self.env.get(node.target.id, (frozenset(), frozenset()))
            d, c = self.expr(node.value)
            self.env[node.target.id] = (pd | d, pc | c | ctrl)
        elif isinstance(node, ast.If):
            td, tc = self.expr(node.test)
            inner = ctrl | td | tc
            for s in node.body:
                self.stmt(s, inner)
            for s in node.orelse:
                self.stmt(s, inner)
        elif isinstance(node, ast.For) and isinstance(node.target, ast.Name):
            id_, ic = self.expr(node.iter)
            self.env[node.target.id] = (id_, ic)
            for s in node.body:
                self.stmt(s, ctrl | ic)
        elif isinstance(node, (ast.Expr, ast.Pass)):
            pass
        else:
            raise _Unanalyzable()

    def _key(self, sl):
        node = sl.value if isinstance(sl, ast.Index) else sl   # ast.Index before Python 3.9
        return node.value if isinstance(node, ast.Constant) else None

    def expr(self, node):
        if isinstance(node, ast.Name):
            return self.env.get(node.id, (frozenset(), frozenset()))
        if isinstance(node, ast.Constant):
            return (frozenset(), frozenset())
        if isinstance(node, ast.BinOp):
            ld, lc = self.expr(node.left)
            rd, rc = self.expr(node.right)
            return (ld | rd, lc | rc)
        if isinstance(node, ast.UnaryOp):
            return self.expr(node.operand)
        if isinstance(node, (ast.BoolOp,)):
            ds = [self.expr(v) for v in node.values]
            return (frozenset().union(*[d for d, _ in ds]),
                    frozenset().union(*[c for _, c in ds]))
        if isinstance(node, ast.Compare):
            ds = [self.expr(node.left)] + [self.expr(c) for c in node.comparators]
            return (frozenset().union(*[d for d, _ in ds]),
                    frozenset().union(*[c for _, c in ds]))
        if isinstance(node, ast.IfExp):
            td, tc = self.expr(node.test)
            bd, bc = self.expr(node.body)
            od, oc = self.expr(node.orelse)
            return (bd | od, bc | oc | td | tc)
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id in self.params:
                key = self._key(node.slice)
                if key is not None:
                    return (frozenset([key]), frozenset())   # row["field"] maps to its field atom
            vd, vc = self.expr(node.value)
            return (vd, vc)
        if isinstance(node, ast.Attribute):
            return self.expr(node.value)
        if isinstance(node, ast.Call):
            ds = [self.expr(a) for a in node.args]
            d = frozenset().union(*[x for x, _ in ds]) if ds else frozenset()
            c = frozenset().union(*[x for _, x in ds]) if ds else frozenset()
            return (d, c)
        if isinstance(node, (ast.List, ast.Tuple)):
            ds = [self.expr(e) for e in node.elts]
            d = frozenset().union(*[x for x, _ in ds]) if ds else frozenset()
            c = frozenset().union(*[x for _, x in ds]) if ds else frozenset()
            return (d, c)
        raise _Unanalyzable()


def column_provenance(fn):
    """Compute static column-level provenance for ``fn`` without executing it.

    Returns ``(data_cols, control_cols, regime)``, the union over every control-flow
    path of the input columns that flow into the output and those that gate it. If
    the source cannot be analyzed, every parameter is reported as both data and
    control with regime ``coarse``."""
    params = list(inspect.signature(fn).parameters)
    try:
        src = textwrap.dedent(inspect.getsource(fn))
        fndef = ast.parse(src).body[0]
        a = _ColumnProv(params)
        a.run(fndef)
        return frozenset(a.data), frozenset(a.control), a.regime
    except (_Unanalyzable, OSError, SyntaxError, TypeError):
        return frozenset(params), frozenset(params), "coarse"


def trace(func):
    """Return a provenance-tracking version of ``func``.

    The returned callable takes ``Tagged`` inputs (see :func:`tag_row`,
    :func:`tag`, :func:`taglist`) and returns a ``Tagged`` output. Its
    :func:`lineage` holds the data dependencies, :func:`control_prov` the control
    dependencies, and :func:`regime` the coarsest granularity encountered.
    """
    src = textwrap.dedent(inspect.getsource(func))
    tree = ast.parse(src)
    fn = tree.body[0]
    fn.decorator_list = []                            # do not re-apply @trace
    _Tracer().visit(tree)
    ast.fix_missing_locations(tree)

    g = dict(func.__globals__)
    if func.__closure__:                              # bind closed-over variables
        for name, cell in zip(func.__code__.co_freevars, func.__closure__):
            try:
                g[name] = cell.cell_contents
            except ValueError:
                pass
    g.update(_HELPERS)
    exec(compile(tree, "<fineprov:%s>" % func.__name__, "exec"), g)
    traced = g[fn.name]
    traced.__fineprov_source__ = func
    return traced
