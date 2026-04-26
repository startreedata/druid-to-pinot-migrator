# Contributing

Thanks for your interest in `druid-to-pinot-migrator`! Bug reports,
feature requests, and pull requests are all welcome.

## Reporting bugs

Open a [GitHub Issue](https://github.com/startreedata/druid-to-pinot-migrator/issues)
with:

- The version of the migrator (`dpm --version` once published; otherwise
  the commit SHA you're on)
- The Druid version you're migrating from and the target Pinot version
- A minimal Druid spec that reproduces the problem (redact any sensitive
  values — schema names, hostnames)
- The exact CLI command you ran and the error / unexpected output

For security-sensitive reports (e.g. anything that could leak credentials
or compromise a cluster), use the channel in [SECURITY.md](SECURITY.md)
instead of a public issue.

## Suggesting a feature

Open an issue describing:

- The migration scenario the current tool can't handle
- Whether you can share a sample spec / dataset
- Whether you'd be willing to send a PR

Real-world Druid → Pinot migration patterns we don't cover well yet are
the most useful kind of feature request.

## Pull requests

### Dev setup

```bash
git clone git@github.com:startreedata/druid-to-pinot-migrator.git
cd druid-to-pinot-migrator
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Tests must pass

```bash
# Fast loop — unit + integration (no Docker required)
pytest tests/unit tests/integration -q
```

For changes that touch live cluster paths (Druid HTTP clients, Pinot
table config emission, the migration pipeline itself), also run the
live Docker suite:

```bash
LIVE_DOCKER_TESTS=1 pytest tests/docker -v
```

The live suite boots a real Druid + Pinot + Kafka stack via
`docker-compose` (~6 GB RAM); see `tests/docker/docker-compose.yml`.

### Style and structure

- **Pure functions stay pure.** The `migrator/` core (parsers,
  generators, planners) does not perform I/O. HTTP / file-system
  concerns live in dedicated client modules with injectable sessions.
  This is what keeps the unit-test suite under a second.
- **Tests for new behavior.** Every new public function or CLI flag
  needs at least one unit test asserting the contract. Live Docker tests
  are the catch-net for wire-protocol regressions across Druid / Pinot
  versions; add one when the change involves a new Druid endpoint, a
  new Pinot config field, or anything that depends on cluster behavior.
- **Hand-rolled mocks over `requests-mock`** in unit tests — the
  pattern in `tests/unit/test_overlord_client.py` is the reference.
- **One concept per PR.** Easier to review, easier to revert.

### Commit messages

We use a light-touch [Conventional Commits](https://www.conventionalcommits.org/)
flavour:

```
feat: short summary

Longer body explaining the why.
```

Common type prefixes used in this repo: `feat`, `fix`, `docs`, `test`,
`ci`, `refactor`, `chore`.

### CI

Two GitHub Actions workflows run on every pull request:

| Workflow | What it does |
|----------|--------------|
| `ci.yml` | Unit + integration tests on Python 3.11 / 3.12 + CLI smoke |
| `version-matrix.yml` (path-triggered) | Live Docker tests across a curated Druid × Pinot matrix |

Both must be green before a PR can be merged.

## Compatibility commitments

The version compatibility matrix in the [README](README.md) reflects
what we actively test. Changes that would require dropping support for
a previously-tested cell need an explicit note in the PR description.

## Code review

PRs are reviewed by the StarTree team listed in `CODEOWNERS`. We aim
for first-pass review within a few business days; if your PR has been
sitting longer, ping the issue or message the maintainers.
