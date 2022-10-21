from pathlib import Path
from pprint import pprint
from re import M

import click
import jinja2
from click import secho
from kedro.config import ConfigLoader
from kedro.framework.project import pipelines, settings
from kedro.framework.startup import ProjectMetadata
from slugify import slugify

from .dag_tpl_render import gen_dag_by_pipeline
from .utils import Convertor


@click.group(name="Hackathon")
def commands():
    pass


@commands.group(name="hackathon")
def demo_commands():
    pass


@demo_commands.command()
@click.pass_obj
def to_json(metadata):
    """Display the pipeline in JSON format"""
    pipeline = pipelines["__default__"]
    pprint(pipeline)


@demo_commands.command()
@click.option(
    "-j",
    "--jinja-file",
    "jinja2_file",
    type=click.Path(exists=True,
                    readable=True,
                    resolve_path=True,
                    file_okay=True,
                    dir_okay=False),
    default=Path(__file__).parent / "templates/dag_tpl.j2",
)
@click.option(
    "-b",
    "--tpl-base-path",
    "tpl_base_path",
    type=click.Path(exists=True,
                    readable=True,
                    resolve_path=True,
                    file_okay=False,
                    dir_okay=True),
    default=Path(__file__).parent / "templates",
)
@click.option(
    "-p",
    "--pipeline",
    "pipeline_name",
    type=str,
    default="__default__",
    required=False,
    help="Pipeline name to pick.",
)
@click.pass_obj
def gen_dags(metadata: ProjectMetadata, jinja2_file, tpl_base_path,
             pipeline_name):
    """Display the metadata"""
    pprint("start generate dag file")
    pprint(f"jinja2_file:{jinja2_file}")
    pprint(f"pipeline_name:{pipeline_name}")
    gen_dag_by_pipeline(pipeline_name, jinja2_file, tpl_base_path)


@demo_commands.command()
@click.option(
    "-j",
    "--jinja-file",
    type=click.Path(exists=True,
                    readable=True,
                    resolve_path=True,
                    file_okay=True,
                    dir_okay=False),
    default=Path(__file__).parent / "spark_operator_tpl.j2",
)
@click.pass_obj
def gen_tpl(metadata: ProjectMetadata, jinja_file):
    """render the jinja2 template"""
    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True,
                                   loader=loader,
                                   lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)

    pprint(metadata)

    package_name = metadata.package_name

    file_name = "mh_test_file.yaml"

    target_path = Path("./mh-yaml/")

    target_path = target_path / file_name

    target_path.parent.mkdir(parents=True, exist_ok=True)

    local_var = generated_var()

    parameters = get_parameters(metadata)

    pprint(parameters)

    # render template
    template.stream(
        package_name=package_name,
        var=local_var,
    ).dump(str(target_path))

    secho("")
    secho("A yaml file has been generated in:", fg="green")
    secho(str(target_path))
    secho("Now u can check it", fg="red")


def get_parameters(metadata):
    project_path = metadata.project_path
    conf_path = str(project_path / settings.CONF_SOURCE)
    conf_loader = ConfigLoader(conf_source=conf_path, env="local")
    parameters = conf_loader.get("parameters*", "parameters*/**")
    return parameters


def generated_var():
    var = {}
    nodeSelector = {}
    nodeSelector['spark_task_node_selector'] = {
        "key": "{{ var.value.key }}",
        "value": '{{ var.value.value }}'
    }
    nodeSelector[
        'image'] = "harbor.docker.4pd.io/openaios/load_data.tar:v0.0.1"

    var = {"value": nodeSelector}

    return Convertor(var)
