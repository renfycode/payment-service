import json
from typing import Any

from rich.syntax import Syntax

from payments.cli.common import config_source, console, load_settings, new_typer

app = new_typer("Inspect the effective configuration.")


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return repr(value)
    # Строки JSON совместимы с базовыми строками TOML.
    return json.dumps(value, ensure_ascii=False)


def to_toml(data: dict[str, dict[str, Any]]) -> str:
    sections = []
    for section, values in data.items():
        lines = [f"[{section}]"]
        lines += [f"{key} = {_toml_value(value)}" for key, value in values.items()]
        sections.append("\n".join(lines))
    return "\n\n".join(sections) + "\n"


@app.command()
def show() -> None:
    """Print the effective configuration (TOML + environment + defaults), secrets masked."""
    settings = load_settings()
    # model_dump(mode="json") заменяет значения SecretStr на "**********".
    text = f"# {config_source()}\n\n" + to_toml(settings.model_dump(mode="json"))
    console.print(Syntax(text, "toml", theme="ansi_dark", background_color="default"))


@app.command()
def check() -> None:
    """Validate the configuration: exit code 0 if it is valid."""
    load_settings()
    console.print(f"[green]✓[/] Configuration is valid ({config_source()})")
