import json
import os
import re
from collections import defaultdict
from pprint import pprint

import click
from kedro.framework.context import KedroContext
from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata

from .context_helper import ContextHelper


@click.group(name="Debug")
def commands():
    """Kedro plugin Debug for kedro development debugging tool"""
    pass


@commands.group(name="debug")
def debug_commands():
    """kedro Debug development tool"""
    pass


@debug_commands.command()
@click.option("-p", "--pipeline", "pipeline_name", default="__default__")
@click.option(
    "-e",
    "--env",
    "env",
    type=str,
    default=lambda: os.environ.get("KEDRO_ENV", "local"),
    help="Environment to use.",
)
@click.pass_context
@click.pass_obj
def dump(metadata: ProjectMetadata, ctx: click.core.Context, env,
         pipeline_name):
    """print kedro build-in values"""
    #ctx.ensure_object(dict)
    #ctx.obj["context_helper"] = ContextHelper.init(metadata, env,
    #helper = ContextHelper.init(metadata, env, pipeline_name)
    pprint(pipeline_name)

    pipeline = pipelines.get(pipeline_name)

    pprint(pipeline)
    for n in pipeline.nodes:
        print(n.name)
        for tag in n.tags:
            if re.match('^airflow:operator', tag):
                operator = tag.replace('airflow:operator:', '')
                if operator == 'SparkKubernetesOperator':
                    pprint(n.outputs)

    pprint("--------------------new---line-----------")

    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)

    pprint("start")
    pprint(dependencies)
    pprint("end")

    #json_data = json.dumps(ctx)
    #print(json_data)
    #node_tags = {}
    #pipeline = pipelines.get(pipeline_name)
    #for n in pipeline.nodes:
    #    print(n.name)
    #    for tag in n.tags:
    #        if re.match('^airflow:operator', tag):
    #            node_tags[n.name] = tag.replace('airflow:operator:', '')

    #print(node_tags)
    #pipeline3 = pipelines[pipeline_name]
    #pprint(pipeline3)


@debug_commands.command()
@click.option("-p", "--pipeline", "pipeline_name", default="__default__")
@click.pass_obj
def print_pipeline(metadata: ProjectMetadata, pipeline_name):
    """print kedro build-in values"""
    #ctx.ensure_object(dict)
    pprint(pipeline_name)
    #pprint(helper.pipeline)

    pipeline = pipelines.get(pipeline_name)
    pprint(pipeline)
