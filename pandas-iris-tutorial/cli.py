import sys
from pathlib import Path
from typing import Dict

import pandas as pd
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from kedro.io import AbstractDataSet, DataCatalog, MemoryDataSet
from kedro.runner import SequentialRunner

#sys.path.append(cur_path)


#from kedro.framework.startup import _add_src_to_path


metadata = bootstrap_project(Path.cwd())


class CustomRunners(SequentialRunner):

    def __init__(self, is_async: bool = False):
        """Instantiates the runner classs.

        Args:
            is_async: If True, the node inputs and outputs are loaded and saved
                asynchronously with threads. Defaults to False.

        """
        super().__init__(is_async=is_async)

    def init_catalog(self):
        data = pd.DataFrame({'col1': [1, 2], 'col2': [4, 5],
                              'col3': [5, 6]})
        ds = MemoryDataSet(data=data)

        data_set_maps = dict()

        data_set_maps['test-catalog-01'] = ds

        self._catalog = DataCatalog(data_sets=data_set_maps)


    def create_default_data_set(self,ds_name: str) -> AbstractDataSet:

        return self._catalog._get_data_set(ds_name)

with KedroSession.create(metadata.package_name) as session:
    ctx = session.load_context()
    #ctx.
    #session.run(pipeline_name='__default__',
    #    node_names=['pre_process'],
    #    from_inputs=['example_dev_dataset']
    #)
    runner = CustomRunners()
    runner.init_catalog()

    session.run(runner=runner)
