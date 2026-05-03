from __future__ import annotations

import typer

from migrator.cli.commands import (
    backfill_batch,
    deploy,
    extract_offsets,
    extract_spec,
    generate,
    inspect,
    normalize,
    parity_check,
    plan_hybrid,
    validate,
)

app = typer.Typer(name="dpm", help="Druid to Pinot Migration Tool", no_args_is_help=True)

app.command("inspect")(inspect.command)
app.command("normalize")(normalize.command)
app.command("generate")(generate.command)
app.command("validate")(validate.command)
# Cluster-introspection commands
app.command("extract-spec")(extract_spec.command)
# Hybrid (REALTIME + OFFLINE) migration commands
app.command("extract-offsets")(extract_offsets.command)
app.command("plan-hybrid")(plan_hybrid.command)
app.command("backfill-batch")(backfill_batch.command)
# Pinot-side deployment
app.command("deploy")(deploy.command)
# Post-migration validation
app.command("parity-check")(parity_check.command)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
