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


# ─────────────────────────────────────────────────────────────────────────────
# mTLS — --cert / --key
# ─────────────────────────────────────────────────────────────────────────────


class TestMtls:
    def test_combined_pem(self, tmp_path):
        # Single PEM containing both cert and key (the common
        # `cat client.crt client.key > combined.pem` recipe).
        pem = tmp_path / "combined.pem"
        pem.write_text("dummy")
        s = auth.configure_session(cert=str(pem))
        # requests' contract: a string means a combined PEM file.
        assert s.cert == str(pem)

    def test_separate_cert_and_key(self, tmp_path):
        cert = tmp_path / "client.crt"
        key = tmp_path / "client.key"
        cert.write_text("dummy")
        key.write_text("dummy")
        s = auth.configure_session(cert=str(cert), key=str(key))
        # requests' contract: a tuple is (cert_file, key_file).
        assert s.cert == (str(cert), str(key))

    def test_key_without_cert_rejected(self, tmp_path):
        key = tmp_path / "client.key"
        key.write_text("dummy")
        with pytest.raises(auth.AuthConfigError, match="key supplied without"):
            auth.configure_session(key=str(key))

    def test_no_mtls_when_neither_passed(self):
        s = auth.configure_session()
        # ``requests.Session.cert`` defaults to None; we don't touch it.
        assert s.cert is None

    def test_mtls_combines_with_basic_auth(self, tmp_path):
        # Real-world common case: mTLS + basic auth on the same request.
        # Both should land on the session.
        cert = tmp_path / "c.pem"
        cert.write_text("dummy")
        s = auth.configure_session(
            auth_value="basic:admin:secret", cert=str(cert),
        )
        assert s.cert == str(cert)
        assert isinstance(s.auth, HTTPBasicAuth)
        assert s.auth.username == "admin"

    def test_mtls_combines_with_bearer(self, tmp_path):
        cert = tmp_path / "c.pem"
        cert.write_text("dummy")
        s = auth.configure_session(
            auth_value="bearer:tok", cert=str(cert),
        )
        assert s.cert == str(cert)
        assert s.headers["Authorization"] == "Bearer tok"

    def test_mtls_combines_with_ca_bundle(self, tmp_path):
        # Independent: client cert (mTLS) for proving who we are,
        # CA bundle for verifying the server. Different concerns.
        cert = tmp_path / "client.pem"
        ca = tmp_path / "ca.pem"
        cert.write_text("dummy")
        ca.write_text("dummy")
        s = auth.configure_session(cert=str(cert), ca_bundle=str(ca))
        assert s.cert == str(cert)
        assert s.verify == str(ca)


# ─────────────────────────────────────────────────────────────────────────────
# Env-var fallback for cert/key
# ─────────────────────────────────────────────────────────────────────────────


class TestMtlsEnvVar:
    def test_cert_via_env(self, monkeypatch, tmp_path):
        cert = tmp_path / "c.pem"
        cert.write_text("dummy")
        monkeypatch.setenv("DPM_DRUID_CERT", str(cert))
        s = auth.session_from_env("DRUID")
        assert s.cert == str(cert)

    def test_cert_and_key_via_env(self, monkeypatch, tmp_path):
        cert = tmp_path / "c.pem"
        key = tmp_path / "k.pem"
        cert.write_text("dummy")
        key.write_text("dummy")
        monkeypatch.setenv("DPM_DRUID_CERT", str(cert))
        monkeypatch.setenv("DPM_DRUID_KEY", str(key))
        s = auth.session_from_env("DRUID")
        assert s.cert == (str(cert), str(key))

    def test_cli_cert_overrides_env(self, monkeypatch, tmp_path):
        env_cert = tmp_path / "env.pem"
        cli_cert = tmp_path / "cli.pem"
        env_cert.write_text("dummy")
        cli_cert.write_text("dummy")
        monkeypatch.setenv("DPM_DRUID_CERT", str(env_cert))
        s = auth.session_from_env("DRUID", cert=str(cli_cert))
        assert s.cert == str(cli_cert)

    def test_separate_prefixes_for_mtls(self, monkeypatch, tmp_path):
        druid_cert = tmp_path / "druid.pem"
        pinot_cert = tmp_path / "pinot.pem"
        druid_cert.write_text("dummy")
        pinot_cert.write_text("dummy")
        monkeypatch.setenv("DPM_DRUID_CERT", str(druid_cert))
        monkeypatch.setenv("DPM_PINOT_CERT", str(pinot_cert))
        d = auth.session_from_env("DRUID")
        p = auth.session_from_env("PINOT")
        assert d.cert == str(druid_cert)
        assert p.cert == str(pinot_cert)
