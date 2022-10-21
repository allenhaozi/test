import re
from codecs import open
from os import path

from setuptools import setup

name = "kedro-plugin-debug"
here = path.abspath(path.dirname(__file__))

# get package version
package_name = name.replace("-", "_")
with open(path.join(here, package_name, "__init__.py"), encoding="utf-8") as f:
    version = re.search(r'__version__ = ["\']([^"\']+)', f.read()).group(1)

# get the dependencies and installs
with open("requirements.txt", "r", encoding="utf-8") as f:
    requires = [x.strip() for x in f if x.strip()]

# get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    readme = f.read()

setup(
    name=name,
    version=version,
    description="Kedro-Plugin-Debug debug all about Kedro",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="",
    author="allenhaozi",
    python_requires=">=3.7, <3.11",
    install_requires=requires,
    license="Apache Software License (Apache 2.0)",
    packages=["kedro_plugin_debug"],
    package_data={"tpl": ["kedro_plugin_debug/template.j2"]},
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "kedro.project_commands":
        ["debug = kedro_plugin_debug.plugin:commands"]
    },
)
