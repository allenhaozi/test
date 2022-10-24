import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

import jinja2
from click import secho
from kedro.framework.project import pipelines
from slugify import slugify

from .const import Const


def gen_dag_by_pipeline(pipeline_name, jinja2_file, tpl_base_path, dag_name,
                        dag_type):
    gen_dag(pipeline_name, jinja2_file, dag_name)

    # generate node template
    pipeline_list = pipelines.get(pipeline_name)
    #node_inputs = parse_node_input(pipeline_list)
    for node in pipeline_list.nodes:
        gen_node_tpl(node, tpl_base_path, 'pod.template.yaml.j2')
    #sync_tpl(tpl_base_path, 'spark.operator.template.yaml.j2')
    

def gen_node_tpl(node, tpl_base_path, tpl_name):

    secho("start render node template")
    secho(node.name, fg="red")

    src_path = f"{tpl_base_path}/{tpl_name}"

    target_path = "./dags/airflow/"
    target_path = Path(target_path)
    target_path = target_path / f"{node.name}.template.yaml"

    target_path.parent.mkdir(parents=True, exist_ok=True)

    jinja_file = Path(src_path).resolve()

    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True,
                                   loader=loader,
                                   lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)
    # render template
    template.stream(node=node).dump(str(target_path))
    secho("finished render node template")
    secho("render file:")
    secho(target_path, fg="red")


def gen_dag(pipeline_name, jinja2_file, dag_name):

    dag_filename = f"{pipeline_name}_dag.py"

    secho("generate dag file name:")
    secho(dag_filename, fg="green")

    target_path = "./dags/airflow/"
    target_path = Path(target_path)
    target_path = target_path / dag_filename

    target_path.parent.mkdir(parents=True, exist_ok=True)

    jinja_file = Path(jinja2_file).resolve()

    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True,
                                   loader=loader,
                                   lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)

    pipeline_list = pipelines.get(pipeline_name)
    node_tags = parser_node_tags(pipeline_list)

    config = gen_config(node_tags, pipeline_list, pipeline_name)

    # dependencies nodes
    dependencies = parse_dependencies_nodes(pipeline_list)

    secho("start render dag:")
    secho(dag_name, fg="red")
    # render template
    template.stream(
        pipeline=pipeline_list,
        dependencies=dependencies,
        config=config,
        node_tags=node_tags,
        dag_name=dag_name,
    ).dump(str(target_path))
    secho("finished render dag:")
    secho(target_path, fg="red")


#TODO: how to patch spark sensor node
def patch_pipeline(pipeline_list, node_tags):
    pass


def parse_dependencies_nodes(pipeline):
    dependencies = defaultdict(list)
    for n, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(n)
    return dependencies


def parse_append_dags(node_tags):
    append_dags = {}
    for k, v in node_tags.items():
        if v == Const.SparkKubernetesOperator:
            append_dags[k] = f"{k}-sensor"

    return append_dags


def parser_node_tags(pipeline_list) -> Dict[str, str]:
    node_tags = dict()
    for n in pipeline_list.nodes:
        for tag in n.tags:
            if re.match('^airflow:operator', tag):
                node_tags[n.name] = tag.replace('airflow:operator:', '')

    return node_tags


def gen_config(node_tags, pipeline, pipeline_name) -> Dict:
    d = {}
    for v in node_tags.values():
        if v == Const.SparkKubernetesOperator:
            d[Const.SparkKubernetesOperator] = v
        if v == Const.KubernetesPodOperator:
            d[Const.KubernetesPodOperator] = v

    d['dag_name'] = pipeline_name

    return d
