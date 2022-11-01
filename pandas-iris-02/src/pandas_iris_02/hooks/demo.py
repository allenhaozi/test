from typing import Any, Dict

from click import secho
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node


class KedroHookDemoHooks:
    @hook_impl
    def after_context_created(
        self,
        context: KedroContext,
        *args,
        **kwargs,
    ) -> None:
        """Hooks to be invoked after a `KedroContext` is created. This is the earliest
        hook triggered within a Kedro run. The `KedroContext` stores useful information
        such as `credentials`, `config_loader` and `env`.
        Args:
            context: The context that was created.
        """

        secho("after context created start", fg="green")
        print(args)
        print(kwargs)
        secho("after context created end", fg="green")

    @hook_impl
    def after_catalog_created(self, catalog: DataCatalog) -> None:
        secho("catalog created start", fg="green")
        for item in catalog.list():
            print(item)
        secho("catalog created end", fg="red")

    @hook_impl
    def before_node_run(self, node: Node, *args, **kwargs) -> None:
        secho("before_node_run start", fg="green")
        secho(node.name, fg="red")
        print(args)
        print(kwargs)
        # mock test data
        #data = {"before_node_run":"this is mock data from before_node_run"}
        secho("before_node_run", fg="green")
        #return data

    @hook_impl
    def after_node_run(self, node: Node, *args, **kwargs) -> None:
        secho("after_node_run start", fg="green")
        secho(node.name, fg="red")
        print(args)
        print(kwargs)
        secho("after_node_run end", fg="green")

    @hook_impl
    def on_node_error(self, node: Node, *args, **kwargs) -> None:
        secho("on_node_err start", fg="green")
        secho(node.name, fg="red")
        print(args)
        print(kwargs)
        secho("on_node_err end", fg="green")

    @hook_impl
    def before_pipeline_run(sefl, pipeline: Pipeline, *args, **kwargs) -> None:
        secho("before_pipeline_run start", fg="green")
        secho(pipeline.to_json(), fg="red")
        print(args)
        print(kwargs)
        secho("before_pipeline_run end", fg="green")


demo_hooks = KedroHookDemoHooks()
