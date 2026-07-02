"""
DataGuardian Transformation Framework — Milestone 6.

Provides a YAML-driven transformation engine that applies reusable business
transformations to Bronze DataFrames before DQ validation and Silver writing.

The notebook only needs to import and call ``TransformationEngine``::

    from src.transformations import TransformationEngine

    engine = TransformationEngine(spark=spark)
    result = engine.run(
        df=bronze_df,
        source_config=source_config,
        run_id=run.run_id,
        input_row_count=bronze_count,
    )
    if not result.success:
        raise PipelineExecutionException(result.error_message)
    bronze_df = result.output_df

Extending the framework
-----------------------
Register a custom transformation before calling ``engine.run()``::

    from src.transformations.registry import TransformationRegistry
    from mypackage import MyTransformation

    TransformationRegistry.register("my_transform", MyTransformation)
"""

from src.transformations.base_transformation import BaseTransformation
from src.transformations.engine import TransformationEngine
from src.transformations.registry import TransformationRegistry
from src.transformations.results import TransformationMetric, TransformationRunResult

__all__ = [
    "BaseTransformation",
    "TransformationEngine",
    "TransformationMetric",
    "TransformationRegistry",
    "TransformationRunResult",
]
