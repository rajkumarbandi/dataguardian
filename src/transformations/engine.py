"""
Transformation engine — executes YAML-configured transformations sequentially.

``TransformationEngine`` is the runtime orchestrator for M6.  It reads the
``transformations:`` list from a ``SourceConfig``, resolves each step through
``TransformationRegistry``, applies them in order, and returns a
``TransformationRunResult`` containing the transformed DataFrame and per-step
metrics.

Error handling modes
--------------------
Each transformation step declares an ``on_error`` policy that overrides the
source-level ``transformation_policy.on_error`` when set:

``fail_fast`` (default)
    Raise a ``TransformationException`` immediately.  The engine returns with
    ``success=False`` and the output DataFrame is the last successful state.

``continue``
    Log the error as WARNING, keep the DataFrame from the previous successful
    step, and continue with the next transformation.

``skip``
    Identical to ``continue`` but logged at DEBUG.  Used when a transformation
    is expected to fail on certain batches.

Row-count metrics
-----------------
Calling ``df.count()`` is expensive.  The engine counts rows only once at the
start (or accepts ``input_row_count`` from the notebook where the count was
already computed).  For column-only transformations (``modifies_row_count=False``)
the engine reuses the previous count.  Only transformations that can add or
remove rows (filter, remove_duplicates) trigger a fresh ``count()`` call.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from src.common.exceptions import TransformationException
from src.common.logger import DataGuardianLogger, get_logger
from src.transformations.registry import TransformationRegistry
from src.transformations.results import TransformationMetric, TransformationRunResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from src.common.models import SourceConfig

_ON_ERROR_VALUES = frozenset({"fail_fast", "continue", "skip"})


class TransformationEngine:
    """
    Sequentially executes the transformation steps declared in a source YAML.

    Parameters
    ----------
    spark:
        Active SparkSession.
    logger:
        Optional pre-bound logger.
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._spark = spark
        self._log = logger or get_logger("dataguardian.transformation.engine")

    def run(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        run_id: str,
        input_row_count: int | None = None,
    ) -> TransformationRunResult:
        """
        Execute all enabled transformations defined in ``source_config``.

        Parameters
        ----------
        df:
            Input DataFrame (typically the Bronze batch after schema validation).
        source_config:
            Parsed source YAML — provides ``transformations:`` and
            ``transformation_policy:``.
        run_id:
            Pipeline run ID for correlation with audit records.
        input_row_count:
            Pre-computed row count to avoid a redundant ``df.count()`` call.
            When ``None`` the engine counts the input DataFrame once on start.

        Returns
        -------
        TransformationRunResult
            Contains the transformed DataFrame, per-step metrics, and an
            overall success flag.
        """
        # Import transforms so they are registered before the first get()
        import src.transformations.transforms  # noqa: F401

        enabled_steps = [t for t in source_config.transformations if t.enabled]

        if not enabled_steps:
            self._log.debug(
                "No enabled transformations — passing DataFrame through",
                source_name=source_config.name,
            )
            return TransformationRunResult(
                source_name=source_config.name,
                run_id=run_id,
                input_df=df,
                output_df=df,
                metrics=[],
                success=True,
            )

        current_row_count: int = (
            input_row_count if input_row_count is not None else df.count()
        )
        current_df = df
        metrics: list[TransformationMetric] = []
        overall_success = True
        overall_error = ""
        total_time = 0.0
        global_policy = source_config.transformation_policy.on_error

        for idx, step in enumerate(enabled_steps):
            transform_type = step.type
            on_error = step.on_error or global_policy
            cols_before = list(current_df.columns)

            self._log.debug(
                "Applying transformation",
                source_name=source_config.name,
                step=idx + 1,
                transformation_type=transform_type,
                on_error=on_error,
            )

            try:
                transform = TransformationRegistry.get(transform_type)
                t0 = time.perf_counter()
                result_df = transform.apply(current_df, step.params)
                exec_time = time.perf_counter() - t0
                total_time += exec_time

                cols_after = list(result_df.columns)
                rows_after = (
                    result_df.count()
                    if transform.modifies_row_count
                    else current_row_count
                )

                metric = TransformationMetric(
                    transformation_type=transform_type,
                    execution_order=idx + 1,
                    execution_time_seconds=exec_time,
                    rows_before=current_row_count,
                    rows_after=rows_after,
                    columns_before=len(cols_before),
                    columns_after=len(cols_after),
                    columns_added=[c for c in cols_after if c not in cols_before],
                    columns_removed=[c for c in cols_before if c not in cols_after],
                    status="SUCCESS",
                    description=transform.describe(step.params),
                )
                metrics.append(metric)

                current_df = result_df
                current_row_count = rows_after

                self._log.info(
                    "Transformation applied",
                    source_name=source_config.name,
                    step=idx + 1,
                    transformation_type=transform_type,
                    execution_time_seconds=round(exec_time, 4),
                    rows_before=metric.rows_before,
                    rows_after=rows_after,
                    columns_before=metric.columns_before,
                    columns_after=metric.columns_after,
                )

            except Exception as exc:  # noqa: BLE001
                exec_time = time.perf_counter() - (t0 if "t0" in dir() else time.perf_counter())
                error_msg = str(exc)

                if on_error == "skip":
                    status = "SKIPPED"
                    self._log.debug(
                        "Transformation skipped on error",
                        source_name=source_config.name,
                        step=idx + 1,
                        transformation_type=transform_type,
                        error=error_msg,
                    )
                elif on_error == "continue":
                    status = "FAILED"
                    self._log.warning(
                        "Transformation failed — continuing with previous DataFrame",
                        source_name=source_config.name,
                        step=idx + 1,
                        transformation_type=transform_type,
                        error=error_msg,
                    )
                else:  # fail_fast
                    status = "FAILED"
                    self._log.error(
                        "Transformation failed — aborting engine run",
                        source_name=source_config.name,
                        step=idx + 1,
                        transformation_type=transform_type,
                        error=error_msg,
                    )

                metric = TransformationMetric(
                    transformation_type=transform_type,
                    execution_order=idx + 1,
                    execution_time_seconds=exec_time,
                    rows_before=current_row_count,
                    rows_after=current_row_count,
                    columns_before=len(cols_before),
                    columns_after=len(cols_before),
                    status=status,
                    error_message=error_msg,
                )
                metrics.append(metric)

                if on_error == "fail_fast":
                    overall_success = False
                    overall_error = (
                        f"Transformation '{transform_type}' (step {idx + 1}) failed: {error_msg}"
                    )
                    break
                # continue and skip: current_df stays unchanged

        return TransformationRunResult(
            source_name=source_config.name,
            run_id=run_id,
            input_df=df,
            output_df=current_df,
            metrics=metrics,
            success=overall_success,
            error_message=overall_error,
            total_execution_time_seconds=total_time,
        )
