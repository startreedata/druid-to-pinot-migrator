"""
Auth configuration for cluster-bound HTTP clients.

The CLI accepts a small grammar — passed via flag or environment
variable — that maps to a configured ``requests.Session``. Today's
default behaviour ("no auth, default TLS verify") is unchanged when no
flag/env var is set, so this module can be added to existing call sites
without breaking anything.

Grammar (case-insensitive on the kind prefix):

    basic:<user>:<password>
    bearer:<token>
    header:<key>=<value>      # may repeat (semicolon-separated)
    none

The header form is an escape hatch for SPNEGO proxies, mTLS gateways,
and custom enterprise auth setups where neither Basic nor Bearer fits.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import requests
from requests.auth import HTTPBasicAuth


class AuthConfigError(ValueError):
    """Raised when an --auth string can't be parsed."""


@dataclass(frozen=True)
class AuthSpec:
    """Parsed representation of a single ``--auth`` value."""

    auth: HTTPBasicAuth | None = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def is_noop(self) -> bool:
        return self.auth is None and not self.headers


def parse_auth(value: str | None) -> AuthSpec:
    """Parse the ``--auth`` grammar into an ``AuthSpec``.

    Accepts ``None`` / empty / ``"none"`` as a no-op so callers can pass
    the raw flag value through without pre-checking.
    """
    if not value or value.lower() == "none":
        return AuthSpec()

    headers: dict[str, str] = {}
    auth: HTTPBasicAuth | None = None

    # Allow chaining via semicolons so multiple `header:` clauses work
    # without forcing the CLI to accept a list type.
    for clause in value.split(";"):
        clause = clause.strip()
        if not clause:
            continue
        kind, _, rest = clause.partition(":")
        kind = kind.strip().lower()
        if kind == "basic":
            user, _, password = rest.partition(":")
            if not user or not password:
                raise AuthConfigError(
                    f"basic auth requires user:password; got: {clause!r}"
                )
            if auth is not None:
                raise AuthConfigError(
                    "multiple basic/bearer clauses in a single --auth value"
                )
            auth = HTTPBasicAuth(user, password)
        elif kind == "bearer":
            if not rest:
                raise AuthConfigError(
                    f"bearer auth requires a token; got: {clause!r}"
                )
            if auth is not None or "Authorization" in headers:
                raise AuthConfigError(
                    "multiple basic/bearer clauses in a single --auth value"
                )
            headers["Authorization"] = f"Bearer {rest}"
        elif kind == "header":
            key, _, val = rest.partition("=")
            key = key.strip()
            if not key:
                raise AuthConfigError(
                    f"header form requires key=value; got: {clause!r}"
                )
            headers[key] = val
        elif kind == "none":
            continue
        else:
            raise AuthConfigError(
                f"unknown auth kind {kind!r} (expected basic/bearer/header/none)"
            )

    return AuthSpec(auth=auth, headers=headers)


def configure_session(
    *,
    auth_value: str | None = None,
    ca_bundle: str | None = None,
    insecure: bool = False,
) -> requests.Session:
    """Build a configured ``requests.Session`` from CLI/env values.

    The returned session is safe to inject into the existing client
    constructors (``DruidCoordinatorClient(session=...)`` etc.).
    """
    spec = parse_auth(auth_value)

    session = requests.Session()
    # Preserve the prior default — every cluster client used to set this.
    session.headers.update({"Content-Type": "application/json"})

    if spec.auth is not None:
        session.auth = spec.auth
    if spec.headers:
        session.headers.update(spec.headers)
    if insecure:
        session.verify = False
    elif ca_bundle:
        session.verify = ca_bundle

    return session


def session_from_env(
    prefix: str,
    *,
    auth_value: str | None = None,
    ca_bundle: str | None = None,
    insecure: bool | None = None,
) -> requests.Session:
    """Resolve auth/TLS settings from CLI args + env (CLI wins).

    ``prefix`` is the env-var prefix, e.g. ``"DRUID"`` reads
    ``DPM_DRUID_AUTH`` / ``DPM_DRUID_CA`` / ``DPM_DRUID_INSECURE``.
    """
    eauth = os.environ.get(f"DPM_{prefix}_AUTH")
    eca = os.environ.get(f"DPM_{prefix}_CA")
    einsec = os.environ.get(f"DPM_{prefix}_INSECURE")
    return configure_session(
        auth_value=auth_value if auth_value is not None else eauth,
        ca_bundle=ca_bundle if ca_bundle is not None else eca,
        insecure=bool(insecure)
        if insecure is not None
        else (einsec is not None and einsec.lower() not in {"", "0", "false", "no"}),
    )
