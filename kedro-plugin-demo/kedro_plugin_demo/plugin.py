from pathlib import Path
from pprint import pprint

import click
import jinja2
from click import secho
from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata
from slugify import slugify


@click.group(name="JSON")
def commands():
    pass


@commands.group(name="demo")
def demo_commands():
    pass


@demo_commands.command()
@click.pass_obj
def to_json(metadata):
    """Display the pipeline in JSON format"""
    pipeline = pipelines["__default__"]
    pprint(pipeline)


@demo_commands.command()
@click.pass_obj
def print_metadata(metadata):
    """Display the metadata"""
    pprint(metadata)


@demo_commands.command()
@click.option(
    "-j",
    "--jinja-file",
    type=click.Path(
        exists=True, readable=True, resolve_path=True, file_okay=True, dir_okay=False
    ),
    default=Path(__file__).parent / "demo_template.j2",
)
@click.pass_obj
def gen_tpl(metadata: ProjectMetadata, jinja_file):
    """render the jinja2 template"""
    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True, loader=loader, lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)

    package_name = metadata.package_name

    file_name = "mh_test_file.yaml"

    target_path = Path("./mh-yaml/")

    target_path = target_path / file_name

    target_path.parent.mkdir(parents=True, exist_ok=True)

    template.stream(package_name=package_name).dump(str(target_path))

    secho("")
    secho("A yaml file has been generated in:", fg="green")
    secho(str(target_path))
    secho("Now u can check it", fg="red")
