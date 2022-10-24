from pathlib import Path

import click
from click import secho
from kedro.config import ConfigLoader
from kedro.framework.project import pipelines, settings
from kedro.framework.startup import ProjectMetadata

from .dag_tpl_render import gen_dag_by_pipeline


@click.group(name="Hackathon")
def commands():
    pass


@commands.group(name="hackathon")
def demo_commands():
    pass


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
@click.option(
    "-d",
    "--dag",
    "dag_name",
    type=str,
    default="hackathon-lego-team",
    required=False,
    help="dags name to pick.",
)
@click.option(
    "-t",
    "--type",
    "dag_type",
    type=str,
    default="airflow",
    required=False,
    help=
    "which style dag which you want to generate, airflow/argo-workflow/prefect/...",
)
@click.pass_obj
def gen_dags(metadata: ProjectMetadata, jinja2_file, tpl_base_path,
             pipeline_name, dag_name, dag_type):
    """transform pipeline to airflow/argo/.. dags"""
    secho("")
    secho("")
    secho("")
    secho("Start generating cool code for 2022 4paradigm hackathon",
          fg="green")
    secho("")
    secho("")
    secho("")
    secho("jinja2_file:")
    secho(str(jinja2_file), fg="green")
    secho("pipeline_name:")
    secho(pipeline_name, fg="green")

    secho("dag_name:")
    secho(dag_name, fg="green")
    secho("dag_type:")
    secho(dag_type, fg="green")

    gen_dag_by_pipeline(pipeline_name, jinja2_file, tpl_base_path, dag_name,
                        dag_type)

    secho("")
    secho("")
    secho("")
    secho("Your rendering task is complete", fg="green")
    secho("Good Job", fg="green")
    secho("")
    secho("")
    secho("")


def get_parameters(metadata):
    project_path = metadata.project_path
    conf_path = str(project_path / settings.CONF_SOURCE)
    conf_loader = ConfigLoader(conf_source=conf_path, env="local")
    parameters = conf_loader.get("parameters*", "parameters*/**")
    return parameters
