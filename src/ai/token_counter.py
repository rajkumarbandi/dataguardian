"""
Token tracking and cost estimation for the AI Intelligence Layer.

TokenCounter accumulates usage across all AI feature calls and provides
a cost estimate based on the configured pricing table.
The counter is session-scoped — created once per Streamlit session.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock


@dataclass
class TokenUsage:
    """Single-call token usage snapshot."""
    feature: str
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    estimated_cost_usd: float
    latency_ms: float

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass
class _Totals:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    estimated_cost_usd: float = 0.0
    call_count: int = 0


class TokenCounter:
    """
    Thread-safe accumulator for LLM token usage.

    Usage:
        counter = TokenCounter(pricing=config.pricing)
        usage = counter.record(feature="dq_assistant", response=llm_response)
        stats = counter.stats()
    """

    def __init__(
        self,
        prompt_cost_per_1k: float = 0.0,
        completion_cost_per_1k: float = 0.0,
    ) -> None:
        self._prompt_rate = prompt_cost_per_1k / 1000.0
        self._completion_rate = completion_cost_per_1k / 1000.0
        self._lock = Lock()
        self._totals = _Totals()
        self._history: list[TokenUsage] = []

    def record(
        self,
        *,
        feature: str,
        provider: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        latency_ms: float,
    ) -> TokenUsage:
        """Record one LLM call and return the TokenUsage snapshot."""
        cost = (
            prompt_tokens * self._prompt_rate
            + completion_tokens * self._completion_rate
        )
        usage = TokenUsage(
            feature=feature,
            provider=provider,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            estimated_cost_usd=cost,
            latency_ms=latency_ms,
        )
        with self._lock:
            self._totals.prompt_tokens += prompt_tokens
            self._totals.completion_tokens += completion_tokens
            self._totals.estimated_cost_usd += cost
            self._totals.call_count += 1
            self._history.append(usage)
        return usage

    def record_from_response(self, feature: str, response: object) -> TokenUsage:
        """Convenience wrapper — accepts an LLMResponse directly."""
        return self.record(
            feature=feature,
            provider=getattr(response, "provider", "unknown"),
            model=getattr(response, "model", "unknown"),
            prompt_tokens=getattr(response, "prompt_tokens", 0),
            completion_tokens=getattr(response, "completion_tokens", 0),
            latency_ms=getattr(response, "latency_ms", 0.0),
        )

    def stats(self) -> dict[str, object]:
        """Return a serialisable summary of all accumulated usage."""
        with self._lock:
            by_feature: dict[str, dict[str, int | float]] = {}
            for u in self._history:
                if u.feature not in by_feature:
                    by_feature[u.feature] = {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "call_count": 0,
                        "estimated_cost_usd": 0.0,
                    }
                by_feature[u.feature]["prompt_tokens"] += u.prompt_tokens
                by_feature[u.feature]["completion_tokens"] += u.completion_tokens
                by_feature[u.feature]["call_count"] += 1
                by_feature[u.feature]["estimated_cost_usd"] += u.estimated_cost_usd

            return {
                "total_prompt_tokens": self._totals.prompt_tokens,
                "total_completion_tokens": self._totals.completion_tokens,
                "total_tokens": self._totals.prompt_tokens + self._totals.completion_tokens,
                "total_calls": self._totals.call_count,
                "estimated_cost_usd": round(self._totals.estimated_cost_usd, 6),
                "by_feature": by_feature,
            }

    def reset(self) -> None:
        with self._lock:
            self._totals = _Totals()
            self._history.clear()
