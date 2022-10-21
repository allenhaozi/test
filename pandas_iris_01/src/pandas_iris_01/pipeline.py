"""
This is a boilerplate pipeline
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    make_predictions,
    make_predictions_2,
    report_accuracy,
    simulate,
    split_data,
)


def create_pipeline(**kwargs) -> Pipeline:

    n1 = node(
        func=split_data,
        inputs=["example_iris_data", "parameters"],
        outputs=["X_train", "X_test", "y_train", "y_test"],
        name="split-data",
        tags=["airflow:operator:SparkKubernetesOperator"],
    )

    n3 = node(
        func=make_predictions,
        inputs=["X_train", "X_test", "y_train"],
        outputs="y_pred",
        name="make_predictions",
        tags="airflow:operator:KubernetesPodOperator",
    )

    return pipeline([n1, n3])


def create_pipeline_01(**kwargs) -> Pipeline:

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

    n22 = node(
        func=make_predictions_2,
        inputs=["X_train", "X_test"],
        outputs="z_pred",
        name="make_predictions_2",
        tags=["airflow:operator:KubernetesPodOperator"],
    )

    n3 = node(
        func=report_accuracy,
        inputs=["y_pred", "y_test", "z_pred"],
        outputs="",
        name="report_accuracy",
        tags=["airflow:operator:SparkKubernetesOperator"],
    )

    p1 = pipeline([n1])
    p2 = pipeline([n2, n22])

    return pipeline([p1, p2, n3])


def _create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=["example_iris_data", "parameters"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split-data",
                tags=["airflow:operator:KubernetesPodOperator"],
            ),
            node(
                func=make_predictions,
                inputs=["X_train", "X_test", "y_train"],
                outputs="y_pred",
                name="make_predictions",
                tags="airflow:operator:SparkKubernetesOperator",
            ),
            node(
                func=report_accuracy,
                inputs=["y_pred", "y_test"],
                outputs=None,
                name="report_accuracy",
                tags=["airflow:operator:SparkKubernetesOperator"],
            ),
        ],
        tags=["for hackathon"],
    )
