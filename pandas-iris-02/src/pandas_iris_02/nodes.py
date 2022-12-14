"""
This is a boilerplate pipeline
generated using Kedro 0.18.3
"""

import logging
from typing import Any, Dict, Tuple
import os

import numpy as np
import pandas as pd
from click import secho
from kedro.io import DataCatalog


def pre_process(data: pd.DataFrame,*args,**kwargs) -> None:
    secho("this is my test node start",fg="green")
    secho("this test node test my custom dataset",fg="green")
    print(data)
    print(args)
    print(kwargs)
    secho("this is my test node end",fg="green")

def split_data(
    data: pd.DataFrame, parameters: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """Splits data into features and target training and test sets.

    Args:
        data: Data containing features and target.
        parameters: Parameters defined in parameters.yml.
    Returns:
        Split data.
    """

    print(parameters)

    data_train = data.sample(frac=parameters["train_fraction"],
                             random_state=parameters["random_state"])
    data_test = data.drop(data_train.index)

    X_train = data_train.drop(columns=parameters["target_column"])
    X_test = data_test.drop(columns=parameters["target_column"])
    y_train = data_train[parameters["target_column"]]
    y_test = data_test[parameters["target_column"]]

    return X_train, X_test, y_train, y_test


def make_predictions(X_train: pd.DataFrame, X_test: pd.DataFrame,
                     y_train: pd.Series) -> pd.Series:
    """Uses 1-nearest neighbour classifier to create predictions.

    Args:
        X_train: Training data of features.
        y_train: Training data for target.
        X_test: Test data for features.

    Returns:
        y_pred: Prediction of the target variable.
    """

    X_train_numpy = X_train.to_numpy()
    X_test_numpy = X_test.to_numpy()

    squared_distances = np.sum(
        (X_train_numpy[:, None, :] - X_test_numpy[None, :, :])**2, axis=-1)
    nearest_neighbour = squared_distances.argmin(axis=0)
    y_pred = y_train.iloc[nearest_neighbour]
    y_pred.index = X_test.index

    return y_pred


def report_accuracy(y_pred: pd.Series, y_test: pd.Series):
    """Calculates and logs the accuracy.

    Args:
        y_pred: Predicted target.
        y_test: True target.
    """
    accuracy = (y_pred == y_test).sum() / len(y_test)
    logger = logging.getLogger(__name__)
    logger.info("Model has accuracy of %.3f on test data.", accuracy)

    principal = os.getenv("PRINCIPAL")
    
    secho("get env principal:",fg="green")
    secho(principal,fg="red")