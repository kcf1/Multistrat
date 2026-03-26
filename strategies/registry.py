"""Strategy discovery/registry."""

from __future__ import annotations

from collections.abc import Callable

BuildPipelineFn = Callable[[], object]


STRATEGY_BUILDERS: dict[str, BuildPipelineFn] = {
}


def get_enabled_pipelines(strategy_ids: tuple[str, ...]) -> list[object]:
    """Return pipeline instances for enabled strategy ids."""
    pipelines: list[object] = []
    for strategy_id in strategy_ids:
        builder = STRATEGY_BUILDERS.get(strategy_id)
        if builder is None:
            continue
        pipelines.append(builder())
    return pipelines

