"""
This is a boilerplate pipeline
generated using Kedro 0.18.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from pandas_iris_02.deltalake import deltalake_task
from pandas_iris_02.iceberg import create_iceberg_table
from pandas_iris_02.nodes import (
    make_predictions,
    pre_process,
    report_accuracy,
    split_data,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=create_iceberg_table,
            inputs=['parameters'],
            outputs=None,
            name='create_iceberg_table'
        ),
        node(
            func=deltalake_task,
            inputs=['parameters'],
            outputs=None,
            name='deltalake_task',
        ),
        node(
            func=pre_process,
            inputs=["example_dev_dataset"],
            outputs=None,
            name="pre_process",
        ),
        node(
            func=split_data,
            inputs=["example_iris_data", "parameters"],
            outputs=["X_train", "X_test", "y_train", "y_test"],
            name="split",
        ),
        node(
            func=make_predictions,
            inputs=["X_train", "X_test", "y_train"],
            outputs="y_pred",
            name="make_predictions",
        ),
        node(
            func=report_accuracy,
            inputs=["y_pred", "y_test"],
            outputs=None,
            name="report_accuracy",
        ),
    ])
