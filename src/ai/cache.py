"""
Hash-based TTL cache for LLM responses.

Prevents redundant API calls for identical prompts within the TTL window.
Cache keys are built from the SHA-256 hash of the serialised message list,
so the same logical request from any code path produces the same cache key.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.ai.provider import LLMMessage, LLMResponse


@dataclass
class _Entry:
    response: "LLMResponse"
    stored_at: float


class PromptCache:
    """
    In-memory cache for LLMResponse objects.

    Thread-safe. TTL is applied lazily on read (stale entries are evicted
    on the next access to the same key; periodic full eviction is available
    via evict_stale()).
    """

    def __init__(self, ttl_seconds: int = 3600, max_size: int = 512) -> None:
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._store: dict[str, _Entry] = {}
        self._lock = Lock()
        self._hits = 0
        self._misses = 0

    # ── Public API ─────────────────────────────────────────────────────────────

    def get(self, messages: "list[LLMMessage]") -> "LLMResponse | None":
        key = self._make_key(messages)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            if self._is_stale(entry):
                del self._store[key]
                self._misses += 1
                return None
            self._hits += 1
            return entry.response

    def put(self, messages: "list[LLMMessage]", response: "LLMResponse") -> None:
        key = self._make_key(messages)
        with self._lock:
            if len(self._store) >= self._max_size:
                self._evict_oldest()
            self._store[key] = _Entry(response=response, stored_at=time.monotonic())

    def stats(self) -> dict[str, int | float]:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(hit_rate, 4),
                "size": len(self._store),
                "max_size": self._max_size,
                "ttl_seconds": self._ttl,
            }

    def evict_stale(self) -> int:
        """Remove all stale entries; returns count evicted."""
        with self._lock:
            stale = [k for k, v in self._store.items() if self._is_stale(v)]
            for k in stale:
                del self._store[k]
            return len(stale)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            self._hits = 0
            self._misses = 0

    # ── Internal ───────────────────────────────────────────────────────────────

    @staticmethod
    def _make_key(messages: "list[LLMMessage]") -> str:
        payload = json.dumps(
            [{"role": m.role, "content": m.content} for m in messages],
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def _is_stale(self, entry: _Entry) -> bool:
        # Use >= so TTL=0 makes every entry immediately stale (no elapsed time needed)
        return (time.monotonic() - entry.stored_at) >= self._ttl

    def _evict_oldest(self) -> None:
        """Remove the oldest entry (called under lock)."""
        if not self._store:
            return
        oldest_key = min(self._store, key=lambda k: self._store[k].stored_at)
        del self._store[oldest_key]
