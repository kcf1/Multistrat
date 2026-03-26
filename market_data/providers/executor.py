"""Provider-scoped worker-pool helper for dataset jobs."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class ProviderExecutorConfig:
    """Thread-pool sizing for provider-scoped task execution."""

    max_workers: int = 1

    def normalized_workers(self) -> int:
        """Return a valid positive worker count (``>= 1``)."""
        return max(1, int(self.max_workers))


class ProviderExecutor(Generic[T]):
    """
    Thin wrapper around :class:`~concurrent.futures.ThreadPoolExecutor`.

    Dataset jobs can share one executor per provider type and submit symbol-scoped tasks.
    """

    def __init__(self, config: ProviderExecutorConfig) -> None:
        self._config = config
        self._pool = ThreadPoolExecutor(max_workers=config.normalized_workers())

    @property
    def max_workers(self) -> int:
        return self._config.normalized_workers()

    def submit(self, fn: Callable[..., T], *args, **kwargs) -> Future[T]:
        return self._pool.submit(fn, *args, **kwargs)

    def gather(self, futures: Iterable[Future[T]]) -> list[T]:
        """Wait for completion and return results in completion order."""
        out: list[T] = []
        for f in as_completed(list(futures)):
            out.append(f.result())
        return out

    def shutdown(self, *, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    def __enter__(self) -> "ProviderExecutor[T]":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown(wait=True)

