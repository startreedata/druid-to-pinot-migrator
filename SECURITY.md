# Security Policy

## Reporting a Vulnerability

If you believe you have found a security vulnerability in
`druid-to-pinot-migrator`, please **do not** open a public GitHub issue.

Email **security@startree.ai** with:

- A description of the issue and its impact
- Steps to reproduce, including a minimal Druid spec or input that
  triggers the behavior (please redact any sensitive values)
- The migrator version (or commit SHA) you tested against
- Any suggested mitigation, if you have one

We will acknowledge your report within **3 business days** and aim to
provide an initial assessment within **10 business days**. Once the
issue is fixed and a release has shipped, we will credit reporters in
the release notes unless they prefer to remain anonymous.

## Scope

In scope:

- Bugs in the migrator that could expose credentials, tokens, or other
  sensitive values from input specs into generated artifacts or logs
- Path-traversal or injection issues in CLI argument handling
- Vulnerabilities in the migrator's HTTP client modules
  (`migrator/druid/`, `migrator/pinot/`) that could be triggered by a
  malicious Druid or Pinot response
- Supply-chain risks in our published Python package

Out of scope:

- Vulnerabilities in upstream dependencies — please report those to the
  upstream maintainers (we'll pull updated versions promptly once
  patches are available)
- Vulnerabilities in Apache Druid or Apache Pinot themselves —
  see https://druid.apache.org/community/#security and
  https://pinot.apache.org/community for the upstream security channels
- Denial-of-service issues caused by intentionally large input specs
  (running the migrator against untrusted input is not a supported
  threat model — the tool is designed to be run by operators on their
  own ingestion specs)

## Supported Versions

We support the latest minor release line on the `main` branch. Older
release tags are not actively patched; users on older versions should
upgrade to receive security fixes.
