"""
Module: Spark MLlib Training Pipeline
Machine Learning: Distributed GBT Classifier Training

Key Responsibilities:
1. Load Gold feature store data from Snowflake or Parquet artifacts.
2. Build a Spark ML Pipeline: VectorAssembler -> StandardScaler -> GBTClassifier.
3. Split data temporally (Oct 2019 - Jan 2020 train; Feb - Apr 2020 OOT test).
4. Fit the pipeline and persist the model artifact.
"""

import argparse
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame, SparkSession


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the ML training job."""
    parser = argparse.ArgumentParser(description="Spark MLlib GBT Training")
    parser.add_argument(
        "--input-path",
        type=str,
        required=True,
        help="Path to Gold feature Parquet (or Snowflake table name).",
    )
    parser.add_argument(
        "--model-output",
        type=str,
        required=True,
        help="Directory to persist the fitted PipelineModel.",
    )
    parser.add_argument(
        "--val-ratio",
        type=float,
        default=0.2,
        help="Validation split ratio (default 0.2).",
    )
    return parser.parse_args()


def build_pipeline() -> Pipeline:
    """
    Construct the Spark ML Pipeline.

    TODO [ML Pipeline]:
    1. Import VectorAssembler, StandardScaler, GBTClassifier from pyspark.ml.
    2. Define FEATURE_COLS (from ml_config.yaml or hardcoded list).
    3. VectorAssembler: inputCols=FEATURE_COLS, outputCol='raw_features'.
    4. StandardScaler: inputCol='raw_features', outputCol='features', withMean=True.
    5. GBTClassifier: featuresCol='features', labelCol='is_purchased',
                      maxIter=20, maxDepth=5.
    6. Return Pipeline(stages=[assembler, scaler, gbt]).
    """
    raise NotImplementedError("Implement build_pipeline following the docstring.")


def split_data(df: DataFrame, val_ratio: float = 0.2, seed: int = 42) -> tuple[DataFrame, DataFrame]:
    """
    Perform an 80/20 random train/validation split.

    TODO [ML Pipeline]:
    Use df.randomSplit([1 - val_ratio, val_ratio], seed=seed).
    Return (train_df, val_df).
    """
    raise NotImplementedError("Implement split_data following the docstring.")


def main() -> None:
    args = parse_args()
    # TODO: Initialize SparkSession via src.common.spark_session.get_spark_session()
    # TODO: Load feature data from args.input_path
    # TODO: Split into train/val via split_data()
    # TODO: Build pipeline via build_pipeline()
    # TODO: Fit pipeline on train_df -> fitted_model
    # TODO: Evaluate on val_df; log ROC-AUC
    # TODO: Save fitted_model to args.model_output
    pass


if __name__ == "__main__":
    main()
