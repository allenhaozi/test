import sys
from pathlib import Path

from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

#sys.path.append(cur_path)

#from kedro.framework.startup import _add_src_to_path


for item in sys.path:
    print(item)


metadata = bootstrap_project(Path.cwd())

print(metadata)

with KedroSession.create(metadata.package_name) as session:
    session.run(pipeline_name='__default__',node_names=['pre_process'],from_inputs=['example_dev_dataset'])
