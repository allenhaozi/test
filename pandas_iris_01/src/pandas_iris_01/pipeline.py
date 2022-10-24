"""
This is a boilerplate pipeline
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    make_predictions,
    report_accuracy,
    split_data,
)


def create_pipeline(**kwargs) -> Pipeline:

    n1 = node(
        func=split_data,
        inputs=["example_iris_data", "parameters"],
        outputs=["X_train", "X_test", "y_train", "y_test"],
        name="split-data",
        tags=["airflow:operator:SparkKubernetesOperator", "test_tags"],
    )

    n2 = node(
        func=make_predictions,
        inputs=["X_train", "X_test", "y_train"],
        outputs="y_pred",
        name="make_predictions",
        tags="airflow:operator:KubernetesPodOperator",
    )

    n3 = node(
        func=report_accuracy,
        inputs=["y_pred", "y_test"],
        outputs=None,
        name="report_accuracy",
        tags=["airflow:operator:SparkKubernetesOperator"],
    )

    return pipeline([n1, n2, n3])
