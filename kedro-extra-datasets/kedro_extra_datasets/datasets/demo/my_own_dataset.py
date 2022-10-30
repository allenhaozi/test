from pathlib import Path, PurePosixPath
from typing import Any, Dict

import pandas as pd
from click import secho
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class MyOwnDataset(AbstractVersionedDataSet):
    def __init__(self,file_path:str,version:Version=None,param1:str=None,param2:bool=True,extra:Dict[str, Any]= None):
        secho("this is my own dataset init start",fg="red")
        super().__init__(PurePosixPath(file_path),version)
        self._param1 = param1
        self._param2 = param2
        print(extra)
        secho("this is my own dataset init end",fg="red")


    def _load(self) -> pd.DataFrame:
        load_path = self._get_load_path()
        secho("this is my own dataset and it's loaded",fg="red")
        return pd.read_csv(load_path)

    def _save(self,df: pd.DataFrame) -> None:
        save_path = self._get_save_path()
        df.to_csv(save_path)


    def _exists(self) -> bool:
        path = self._get_load_path()
        return Path(path.as_posix()).exists()

    def _describe(self):
        return dict(version=self._version,param1=self._param1,param2=self._param2)
