from __future__ import annotations

import typer

from migrator.cli.commands import generate, inspect, normalize, validate

app = typer.Typer(name="dpm", help="Druid to Pinot Migration Tool", no_args_is_help=True)

app.command("inspect")(inspect.command)
app.command("normalize")(normalize.command)
app.command("generate")(generate.command)
app.command("validate")(validate.command)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
