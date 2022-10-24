import logging
from pathlib import Path
from typing import Any, Dict, List

from click import secho
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.cli.hooks import cli_hook_impl
from kedro.framework.startup import ProjectMetadata
from kedro.io import DataCatalog

from .tools import _get_cli_structure, _mask_kedro_cli

logger = logging.getLogger(__name__)


class KedroHookDemoCLIHooks:
    @cli_hook_impl
    def before_command_run(self, project_metadata: ProjectMetadata,
                           command_args: List[str]):
        """Hook implementation to check hook strategy: before_command_run"""
        try:
            # get KedroCLI and its structure from actual project root
            cli = KedroCLI(project_path=Path.cwd())
            cli_struct = _get_cli_structure(cli_obj=cli, get_help=False)
            masked_command_args = _mask_kedro_cli(cli_struct=cli_struct,
                                                  command_args=command_args)
            main_command = masked_command_args[
                0] if masked_command_args else "kedro"
            if not project_metadata:  # in package mode
                return

            secho("this is hook")
            secho("before_command_run", fg="green")
            secho(main_command, fg="green")

        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Something went wrong in hook implementation to send command run data to Heap. "
                "Exception: %s",
                exc,
            )


cli_hooks = KedroHookDemoCLIHooks()
