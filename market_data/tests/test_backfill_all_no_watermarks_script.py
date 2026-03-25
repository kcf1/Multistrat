import importlib.util
from pathlib import Path
import sys


def _load_wrapper_module():
    # Import the script as a module without executing it.
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "backfill_all_no_watermarks.py"
    # When running from repo root, the above path is incorrect (market_data/tests/... -> market_data/scripts).
    # Fix relative path by locating repo root:
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "backfill_all_no_watermarks.py"

    spec = importlib.util.spec_from_file_location("backfill_all_no_watermarks", str(script_path))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # dataclasses + postponed annotations can look up sys.modules[cls.__module__]
    # during decoration, so register the module before exec_module().
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_build_backfill_plan_order_and_flags():
    mod = _load_wrapper_module()

    inv = mod.build_backfill_plan(only=None, skip_existing=False)
    assert [i.dataset for i in inv] == list(mod.DATASET_ORDER)
    assert all("--no-watermark" in i.cmd for i in inv)
    assert all("--skip-existing" not in i.cmd for i in inv)

    inv2 = mod.build_backfill_plan(only="ohlcv,basis_rate", skip_existing=True)
    assert [i.dataset for i in inv2] == ["ohlcv", "basis_rate"]
    assert all("--no-watermark" in i.cmd for i in inv2)
    assert all("--skip-existing" in i.cmd for i in inv2)


def test_build_backfill_plan_unknown_dataset_raises():
    mod = _load_wrapper_module()
    try:
        mod.build_backfill_plan(only="nope", skip_existing=False)
        assert False, "Expected ValueError"
    except ValueError as e:
        assert "Unknown dataset key" in str(e)

