"""The three namespace Ops methods cannot succeed, and now say so.

`db.ops.create_namespace` / `set_namespace` / `drop_namespace` used to emit
``CREATE NAMESPACE`` / ``SET NAMESPACE`` / ``DROP NAMESPACE IF EXISTS``. No
parser in the engine accepts any of those verbs on any wire, so every call was
refused; their docstrings nevertheless showed a ``0`` return, i.e. documented an
outcome that could not occur.

These tests pin the corrected behaviour:

* the call raises ``ParticleDBError`` (the SDK's own error type, so code that
  already catches SDK errors keeps working),
* it raises **without sending SQL** - asserted by a recording client that fails
  the test if it is ever asked to execute anything, which is what distinguishes
  "fails fast with an explanation" from "sends dead SQL and relays a parse
  error",
* the message names the verb and says namespaces are not implemented
  server-side, so a caller learns the cause rather than reading a syntax error.

Reverting the fix (restoring the ``self._client.execute(sql)`` bodies) turns
every test in this file red: the recording client would see a call, and no
exception would be raised.
"""

import pytest

from particledb.client import ParticleDBError
from particledb.ops import Ops


class _RecordingClient:
    """A stand-in client that fails loudly if any SQL reaches it."""

    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):  # pragma: no cover - must not run
        self.executed.append(sql)
        raise AssertionError(
            "the namespace methods must not send SQL to the server; got: %r" % (sql,)
        )

    def query(self, sql, params=None):  # pragma: no cover - must not run
        self.executed.append(sql)
        raise AssertionError(
            "the namespace methods must not send SQL to the server; got: %r" % (sql,)
        )


CASES = [
    ("create_namespace", ("tenant_acme",), {}, "CREATE NAMESPACE"),
    (
        "create_namespace",
        ("tenant_acme",),
        {"quotas": {"max_tables": 100}},
        "CREATE NAMESPACE",
    ),
    ("set_namespace", ("tenant_acme",), {}, "SET NAMESPACE"),
    ("drop_namespace", ("tenant_acme",), {}, "DROP NAMESPACE"),
]


@pytest.mark.parametrize("method,args,kwargs,verb", CASES)
def test_namespace_method_raises_and_sends_nothing(method, args, kwargs, verb):
    client = _RecordingClient()
    ops = Ops(client)

    with pytest.raises(ParticleDBError) as excinfo:
        getattr(ops, method)(*args, **kwargs)

    message = str(excinfo.value)
    assert verb in message, "the error must name the verb the caller reached for"
    assert "not implemented by any ParticleDB server" in message
    assert "can never succeed" in message
    # The caller is pointed at what to do instead, not left at a parse error.
    assert "separate server per tenant" in message
    assert "databases-and-schemas" in message

    assert client.executed == [], "no SQL may reach the server"


def test_no_namespace_sql_is_built_anywhere_in_ops():
    """The module no longer contains a NAMESPACE statement to send.

    A weaker test (only checking that the call raises) would still pass if the
    methods built the SQL and then threw. This asserts the dead SQL is gone.
    """
    import inspect

    import particledb.ops as ops_module

    source = inspect.getsource(ops_module)
    for verb in ("CREATE NAMESPACE", "SET NAMESPACE", "DROP NAMESPACE"):
        # The verb may appear inside prose (comments/docstrings/messages) but
        # never inside an f-string that would be handed to execute().
        assert 'f"%s' % verb not in source, "%s SQL is still being built" % verb
        assert "f'%s" % verb not in source, "%s SQL is still being built" % verb
