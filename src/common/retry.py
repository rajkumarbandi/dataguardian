"""
Retry helper with exponential backoff for the DataGuardian platform.

``RetryHelper`` wraps any callable and re-executes it on failure up to a
configurable maximum number of attempts.  Delay between retries grows
exponentially with a configurable multiplier and ceiling.

Designed for transient failures such as:
- Azure Data Lake Storage throttling (HTTP 429)
- Spark task failures due to executor loss
- Temporary Delta table lock contention

Usage
-----
::

    from src.common.models import RetryPolicyConfig
    from src.common.retry import RetryHelper

    policy = RetryPolicyConfig(max_attempts=3, initial_delay_seconds=2.0)
    helper = RetryHelper(policy=policy)

    result = helper.execute(engine.run, bronze_df, source_config, batch_id)

YAML configuration
------------------
::

    pipeline:
      retry_policy:
        max_attempts: 3
        initial_delay_seconds: 1.0
        backoff_multiplier: 2.0
        max_delay_seconds: 60.0
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from src.common.exceptions import PipelineExecutionException
from src.common.logger import DataGuardianLogger, get_logger

if TYPE_CHECKING:
    from src.common.models import RetryPolicyConfig

_T = TypeVar("_T")


class RetryHelper:
    """
    Executes a callable with automatic retries and exponential backoff.

    Parameters
    ----------
    policy:
        ``RetryPolicyConfig`` parsed from the environment YAML.
    logger:
        Optional pre-bound logger; defaults to a module-level logger.

    Backoff formula
    ---------------
    ``delay = min(initial_delay × backoff_multiplier^(attempt-1), max_delay)``

    Example delays with default policy (initial=1s, multiplier=2, max=60s):
    - Attempt 1 → immediate (execute)
    - Attempt 2 → sleep 1.0s
    - Attempt 3 → sleep 2.0s
    - Attempt 4 → sleep 4.0s
    """

    def __init__(
        self,
        policy: RetryPolicyConfig,
        logger: DataGuardianLogger | None = None,
    ) -> None:
        self._policy = policy
        self._log = logger or get_logger("dataguardian.retry")

    def execute(
        self,
        func: Callable[..., _T],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """
        Call ``func(*args, **kwargs)`` and retry on failure.

        Parameters
        ----------
        func:
            The callable to execute.
        *args, **kwargs:
            Arguments forwarded to ``func`` on every attempt.

        Returns
        -------
        _T
            The return value of ``func`` on success.

        Raises
        ------
        PipelineExecutionException
            Wraps the last exception after all retry attempts are exhausted.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self._policy.max_attempts + 1):
            try:
                return func(*args, **kwargs)

            except Exception as exc:
                last_exc = exc
                func_name = getattr(func, "__name__", repr(func))

                if attempt == self._policy.max_attempts:
                    self._log.error(
                        "All retry attempts exhausted",
                        func=func_name,
                        attempts=attempt,
                        error=str(exc),
                    )
                    raise PipelineExecutionException(
                        f"'{func_name}' failed after {attempt} attempt(s): {exc}"
                    ) from exc

                delay = self._compute_delay(attempt)
                self._log.warning(
                    "Attempt failed — retrying with backoff",
                    func=func_name,
                    attempt=attempt,
                    max_attempts=self._policy.max_attempts,
                    retry_in_seconds=round(delay, 2),
                    error=str(exc),
                )
                time.sleep(delay)

        # Unreachable — loop always returns or raises, but satisfies mypy
        raise PipelineExecutionException(
            f"Retry loop exited unexpectedly: {last_exc}"
        ) from last_exc

    def _compute_delay(self, attempt: int) -> float:
        """Exponential backoff delay capped at max_delay_seconds."""
        delay = self._policy.initial_delay_seconds * (
            self._policy.backoff_multiplier ** (attempt - 1)
        )
        return min(delay, self._policy.max_delay_seconds)
