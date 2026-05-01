"""Unit tests for ``migrator.auth``."""

from __future__ import annotations

import pytest
from requests.auth import HTTPBasicAuth

from migrator import auth


# ─────────────────────────────────────────────────────────────────────────────
# parse_auth grammar
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("value", [None, "", "none", "NONE", "  ", "none;none"])
def test_parse_auth_noop(value):
    spec = auth.parse_auth(value)
    assert spec.is_noop
    assert spec.auth is None
    assert spec.headers == {}


def test_parse_auth_basic():
    spec = auth.parse_auth("basic:admin:hunter2")
    assert isinstance(spec.auth, HTTPBasicAuth)
    assert spec.auth.username == "admin"
    assert spec.auth.password == "hunter2"
    assert spec.headers == {}


def test_parse_auth_basic_password_with_colon():
    # Passwords containing ':' must still be parsed correctly via partition.
    spec = auth.parse_auth("basic:user:p:a:s:s")
    assert spec.auth.username == "user"
    assert spec.auth.password == "p:a:s:s"


def test_parse_auth_bearer():
    spec = auth.parse_auth("bearer:eyJhbGciOi...trunc")
    assert spec.auth is None
    assert spec.headers == {"Authorization": "Bearer eyJhbGciOi...trunc"}


def test_parse_auth_header_single():
    spec = auth.parse_auth("header:X-Druid-User=admin")
    assert spec.auth is None
    assert spec.headers == {"X-Druid-User": "admin"}


def test_parse_auth_header_multiple_chained():
    spec = auth.parse_auth(
        "header:X-A=1;header:X-B=two;header:X-C=hello world"
    )
    assert spec.auth is None
    assert spec.headers == {"X-A": "1", "X-B": "two", "X-C": "hello world"}


def test_parse_auth_basic_plus_header():
    spec = auth.parse_auth("basic:u:p;header:X-Tenant=acme")
    assert spec.auth.username == "u"
    assert spec.auth.password == "p"
    assert spec.headers == {"X-Tenant": "acme"}


def test_parse_auth_bearer_plus_header():
    spec = auth.parse_auth("bearer:tok;header:X-Tenant=acme")
    assert spec.auth is None
    assert spec.headers == {
        "Authorization": "Bearer tok",
        "X-Tenant": "acme",
    }


def test_parse_auth_two_basic_clauses_rejected():
    with pytest.raises(auth.AuthConfigError, match="multiple basic/bearer"):
        auth.parse_auth("basic:u:p;basic:u2:p2")


def test_parse_auth_basic_plus_bearer_rejected():
    with pytest.raises(auth.AuthConfigError, match="multiple basic/bearer"):
        auth.parse_auth("basic:u:p;bearer:tok")


def test_parse_auth_unknown_kind_rejected():
    with pytest.raises(auth.AuthConfigError, match="unknown auth kind"):
        auth.parse_auth("kerberos:realm")


def test_parse_auth_basic_missing_password():
    with pytest.raises(auth.AuthConfigError, match="basic auth requires"):
        auth.parse_auth("basic:onlyuser")


def test_parse_auth_bearer_missing_token():
    with pytest.raises(auth.AuthConfigError, match="bearer auth requires"):
        auth.parse_auth("bearer:")


def test_parse_auth_header_missing_value():
    # Empty value is allowed (header:X-Foo= is a real use case, e.g.
    # "delete" pattern), but missing key is not.
    spec = auth.parse_auth("header:X-Foo=")
    assert spec.headers == {"X-Foo": ""}

    with pytest.raises(auth.AuthConfigError, match="header form requires"):
        auth.parse_auth("header:=value")


# ─────────────────────────────────────────────────────────────────────────────
# configure_session
# ─────────────────────────────────────────────────────────────────────────────


def test_configure_session_default():
    s = auth.configure_session()
    assert s.auth is None
    assert s.headers["Content-Type"] == "application/json"
    assert s.verify is True


def test_configure_session_basic():
    s = auth.configure_session(auth_value="basic:admin:secret")
    assert isinstance(s.auth, HTTPBasicAuth)


def test_configure_session_bearer_sets_header():
    s = auth.configure_session(auth_value="bearer:abc")
    assert s.headers["Authorization"] == "Bearer abc"


def test_configure_session_insecure():
    s = auth.configure_session(insecure=True)
    assert s.verify is False


def test_configure_session_ca_bundle():
    s = auth.configure_session(ca_bundle="/etc/ssl/druid-ca.pem")
    assert s.verify == "/etc/ssl/druid-ca.pem"


def test_configure_session_insecure_overrides_ca():
    # Operationally: --insecure means "don't verify", so the CA bundle
    # is ignored even if also passed.
    s = auth.configure_session(ca_bundle="/etc/ssl/some-ca.pem", insecure=True)
    assert s.verify is False


def test_configure_session_invalid_auth_propagates():
    with pytest.raises(auth.AuthConfigError):
        auth.configure_session(auth_value="garbage:value")


# ─────────────────────────────────────────────────────────────────────────────
# session_from_env (CLI > env precedence)
# ─────────────────────────────────────────────────────────────────────────────


def test_session_from_env_uses_env(monkeypatch):
    monkeypatch.setenv("DPM_DRUID_AUTH", "basic:env:user")
    s = auth.session_from_env("DRUID")
    assert isinstance(s.auth, HTTPBasicAuth)
    assert s.auth.username == "env"


def test_session_from_env_cli_wins_over_env(monkeypatch):
    monkeypatch.setenv("DPM_DRUID_AUTH", "basic:env:user")
    s = auth.session_from_env("DRUID", auth_value="bearer:cli-token")
    assert s.auth is None
    assert s.headers["Authorization"] == "Bearer cli-token"


def test_session_from_env_insecure_via_env(monkeypatch):
    monkeypatch.setenv("DPM_PINOT_INSECURE", "1")
    s = auth.session_from_env("PINOT")
    assert s.verify is False


@pytest.mark.parametrize("falsy", ["0", "false", "False", "no", ""])
def test_session_from_env_insecure_env_falsy_values(monkeypatch, falsy):
    monkeypatch.setenv("DPM_PINOT_INSECURE", falsy)
    s = auth.session_from_env("PINOT")
    assert s.verify is True


def test_session_from_env_ca_via_env(monkeypatch, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("dummy")
    monkeypatch.setenv("DPM_DRUID_CA", str(ca))
    s = auth.session_from_env("DRUID")
    assert s.verify == str(ca)


def test_session_from_env_separate_prefixes(monkeypatch):
    monkeypatch.setenv("DPM_DRUID_AUTH", "basic:du:dp")
    monkeypatch.setenv("DPM_PINOT_AUTH", "bearer:ptok")
    druid = auth.session_from_env("DRUID")
    pinot = auth.session_from_env("PINOT")
    assert druid.auth.username == "du"
    assert pinot.auth is None
    assert pinot.headers["Authorization"] == "Bearer ptok"
