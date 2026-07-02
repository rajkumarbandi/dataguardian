"""
Abstract base class for all DataGuardian transformations.

Every transformation must subclass ``BaseTransformation`` and implement
``transformation_type`` and ``apply()``.  The ``TransformationEngine``
discovers transformations through ``TransformationRegistry`` and calls
``apply()`` on each step in declaration order.

Design contract
---------------
* ``apply()`` receives the DataFrame and a params dict from the YAML
  ``params:`` block.  It must return a new DataFrame — never mutate in place.
* Transformations must be **side-effect free** (no writes, no external calls,
  no shared mutable state between calls).
* ``modifies_row_count``: override to ``True`` for transformations that add
  or remove rows (filter, remove_duplicates).  The engine uses this flag to
  decide whether to re-count rows for accurate metrics.

Extension point
---------------
To add a custom transformation, subclass ``BaseTransformation``, implement
the abstract members, and call
``TransformationRegistry.register("my_transform", MyTransformation)``
at import time.  The engine discovers it transparently.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class BaseTransformation(ABC):
    """
    Abstract interface for a single DataFrame transformation.

    Attributes
    ----------
    modifies_row_count:
        Class-level flag.  ``False`` by default — the engine assumes column-
        only operations preserve the row count.  Override to ``True`` for
        transformations that filter or deduplicate rows so the engine counts
        rows accurately for metrics.
    """

    modifies_row_count: bool = False

    @property
    @abstractmethod
    def transformation_type(self) -> str:
        """
        Snake_case identifier for this transformation.

        Must match the key used to register it in ``TransformationRegistry``
        and the ``type:`` field in source YAML.

        Examples: ``"rename_column"``, ``"filter_rows"``, ``"cast_column"``
        """

    @abstractmethod
    def apply(self, df: DataFrame, params: dict[str, Any]) -> DataFrame:
        """
        Apply the transformation and return the resulting DataFrame.

        Parameters
        ----------
        df:
            The input DataFrame.  Never mutate in place.
        params:
            Transformation-specific parameters from the YAML ``params:`` block.

        Returns
        -------
        DataFrame
            A new DataFrame with the transformation applied.
        """

    def describe(self, params: dict[str, Any]) -> str:
        """
        Return a brief human-readable description of this transformation call.

        Used in log messages and audit records.  Override to include key
        parameter values (e.g. ``"rename_column: old_name → new_name"``).
        """
        return self.transformation_type
