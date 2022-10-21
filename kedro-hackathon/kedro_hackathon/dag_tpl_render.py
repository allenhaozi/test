import re
from collections import defaultdict
from pathlib import Path
from pprint import pprint
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

import jinja2
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline, node, pipeline
from slugify import slugify

from .const import Const


def gen_dag_by_pipeline(pipeline_name, jinja2_file, tpl_base_path):
    gen_dag(pipeline_name, jinja2_file)
    sync_tpl(tpl_base_path, 'pod.template.yaml.j2')
    sync_tpl(tpl_base_path, 'spark.operator.template.yaml.j2')


def sync_tpl(tpl_base_path, tpl_name):

    src_path = f"{tpl_base_path}/{tpl_name}"

    target_path = "./dags/airflow/"
    target_path = Path(target_path)
    target_path = target_path / tpl_name.replace(".j2", "")

    target_path.parent.mkdir(parents=True, exist_ok=True)

    with open(src_path) as src, open(target_path, "w") as dest:
        dest.write(src.read().strip())


def gen_dag(pipeline_name, jinja2_file):

    dag_filename = f"{pipeline_name}_dag.py"

    pprint(dag_filename)

    target_path = "./dags/airflow/"
    target_path = Path(target_path)
    target_path = target_path / dag_filename

    target_path.parent.mkdir(parents=True, exist_ok=True)

    pprint(target_path)

    jinja_file = Path(jinja2_file).resolve()
    pprint(jinja_file)
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True,
                                   loader=loader,
                                   lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)

    pipeline_list = pipelines.get(pipeline_name)
    node_tags = parser_node_tags(pipeline)

    config = gen_config(node_tags, pipeline_list, pipeline_name)

    # dependencies nodes
    dependencies = parse_dependencies_nodes(pipeline_list)

    # render template
    template.stream(
        pipeline=pipeline_list,
        dependencies=dependencies,
        config=config,
        node_tags=node_tags,
    ).dump(str(target_path))


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


def parser_node_tags(pipeline) -> Dict[str, str]:
    node_tags = dict()
    for n in pipeline.nodes:
        print(n.name)
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
    pprint(d)
    return d
