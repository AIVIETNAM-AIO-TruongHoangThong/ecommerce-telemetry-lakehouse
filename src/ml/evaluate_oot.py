"""
Module: Out-of-Time (OOT) Evaluation Engine
Machine Learning: Temporal Validation & Concept Drift Detection

Key Responsibilities:
1. Load the persisted PipelineModel.
2. Apply the same feature extraction to the OOT test set (Feb - Apr 2020).
3. Run distributed batch inference.
4. Compute ROC-AUC, PR-AUC, and feature importances.
5. Export results as JSON for reporting.
"""

import argparse
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the OOT evaluation job."""
    parser = argparse.ArgumentParser(description="Out-of-Time ML Evaluation")
    parser.add_argument(
        "--model-path",
        type=str,
        required=True,
        help="Path to persisted PipelineModel directory.",
    )
    parser.add_argument(
        "--test-input",
        type=str,
        required=True,
        help="Path to OOT test feature Parquet or raw CSV.",
    )
    parser.add_argument(
        "--metrics-output",
        type=str,
        required=True,
        help="Output path for JSON metrics report.",
    )
    return parser.parse_args()


def evaluate(predictions_df: DataFrame) -> dict:
    """
    Compute ROC-AUC and PR-AUC on the predictions DataFrame.

    TODO [ML Pipeline]:
    1. Use BinaryClassificationEvaluator with metricName='areaUnderROC'.
    2. Use BinaryClassificationEvaluator with metricName='areaUnderPR'.
    3. Return {'roc_auc': ..., 'pr_auc': ...}.
    """
    raise NotImplementedError("Implement evaluate following the docstring.")


def extract_feature_importance(model: PipelineModel, feature_cols: list[str]) -> list[tuple[str, float]]:
    """
    Extract and rank feature importances from the GBT model stage.

    TODO [ML Pipeline]:
    1. Access the GBT stage: model.stages[-1].featureImportances.toArray().
    2. Zip importances with feature_cols.
    3. Return sorted list of (feature_name, importance) tuples descending by importance.
    """
    raise NotImplementedError("Implement extract_feature_importance following the docstring.")


def write_metrics(metrics: dict, feature_importance: list[tuple[str, float]], output_path: str) -> None:
    """
    Write evaluation metrics and feature importances to a JSON file.

    TODO [ML Pipeline]:
    1. Combine metrics dict and feature_importance list into a single dict.
    2. Write to output_path using json.dump().
    """
    raise NotImplementedError("Implement write_metrics following the docstring.")


def main() -> None:
    args = parse_args()
    # TODO: Initialize SparkSession via src.common.spark_session.get_spark_session()
    # TODO: Load OOT test features from args.test_input
    # TODO: Load PipelineModel.load(args.model_path)
    # TODO: Run model.transform(test_df) -> predictions_df
    # TODO: Call evaluate(predictions_df) and extract_feature_importance()
    # TODO: Call write_metrics() to save JSON report to args.metrics_output
    pass


if __name__ == "__main__":
    main()
