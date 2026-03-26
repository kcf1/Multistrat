from __future__ import annotations

from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig


def test_provider_executor_config_normalizes_non_positive() -> None:
    assert ProviderExecutorConfig(max_workers=0).normalized_workers() == 1
    assert ProviderExecutorConfig(max_workers=-5).normalized_workers() == 1
    assert ProviderExecutorConfig(max_workers=3).normalized_workers() == 3


def test_provider_executor_submit_and_gather() -> None:
    with ProviderExecutor[int](ProviderExecutorConfig(max_workers=2)) as ex:
        futures = [ex.submit(lambda x: x * 2, i) for i in range(3)]
        out = sorted(ex.gather(futures))
    assert out == [0, 2, 4]

