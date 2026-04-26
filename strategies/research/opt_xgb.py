"""
Optuna search for XGBoost regression on vol_weighted_return (Sharpe-style workflow:
predict signal → rank within date → long/short deciles).

Sklearn XGBRegressor kwargs vs native `xgb.train`:

  learning_rate → eta          reg_alpha → alpha          reg_lambda → "lambda"
  n_estimators → num_boost_round (train() argument)

Environment:
  XGBOOST_DEVICE   cuda | cpu (default cuda)
  OPTUNA_METRIC    rmse | spearman | cs_spearman (default rmse)
                   cs_spearman = mean Spearman(pred, y) within each ts (aligns with deciles).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
import sklearn.metrics
import xgboost as xgb

# Allow `python strategies/research/opt_xgb.py` or cwd in research/
_research_dir = Path(__file__).resolve().parent
if str(_research_dir) not in sys.path:
    sys.path.insert(0, str(_research_dir))

from create_data import load_data  # noqa: E402


def _rmse(y_true, y_pred) -> float:
    try:
        return float(sklearn.metrics.root_mean_squared_error(y_true, y_pred))
    except AttributeError:
        return float(sklearn.metrics.mean_squared_error(y_true, y_pred, squared=False))


def _pooled_spearman(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    c = pd.Series(y_pred).corr(pd.Series(y_true), method="spearman")
    return float(c) if pd.notna(c) else -1.0


def _mean_cs_spearman(ts: np.ndarray, y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean date-wise Spearman(pred, y); matches cross-sectional ranking use case."""
    df = pd.DataFrame({"ts": ts, "y": y_true, "p": y_pred})
    corrs: list[float] = []
    for _, g in df.groupby("ts", sort=False):
        if len(g) < 3:
            continue
        c = g["p"].corr(g["y"], method="spearman")
        if pd.notna(c):
            corrs.append(float(c))
    return float(np.mean(corrs)) if corrs else -1.0


# --- Notebook base (explicit in XGBRegressor(...)) ---
_BASE_EXPLICIT = {
    "n_estimators": 500,
    "learning_rate": 0.05,
    "max_depth": 3,
    "reg_alpha": 0.3,
    "max_bin": 128,
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "n_jobs": -1,
    "device": "cuda",
}

# --- sklearn XGBRegressor defaults when kwargs omitted (for tuning context) ---
_SKLEARN_DEFAULTS = {
    "booster": "gbtree",
    "reg_lambda": 1.0,
    "gamma": 0.0,
    "min_child_weight": 1,
    "subsample": 1.0,
    "colsample_bytree": 1.0,
    "colsample_bylevel": 1.0,
    "colsample_bynode": 1.0,
    "max_delta_step": 0,
}


def _suggest_params(trial: optuna.Trial) -> tuple[dict, int]:
    """Broad search: regression objective, tree build, booster, regularization."""
    n_estimators = trial.suggest_int("n_estimators", 250, 900, step=50)

    reg_objective = trial.suggest_categorical(
        "reg_objective",
        ["reg:squarederror", "reg:absoluteerror", "reg:pseudohubererror"],
    )
    tree_method = trial.suggest_categorical("tree_method", ["hist", "approx"])
    booster = trial.suggest_categorical("booster", ["gbtree", "dart"])
    grow_policy = trial.suggest_categorical("grow_policy", ["depthwise", "lossguide"])
    eval_metric = trial.suggest_categorical("eval_metric", ["rmse", "mae"])

    param: dict = {
        "verbosity": 0,
        "objective": reg_objective,
        "tree_method": tree_method,
        "device": os.getenv("XGBOOST_DEVICE", _BASE_EXPLICIT["device"]),
        "n_jobs": _BASE_EXPLICIT["n_jobs"],
        "booster": booster,
        "grow_policy": grow_policy,
        "eval_metric": eval_metric,
        "eta": trial.suggest_float("learning_rate", 0.02, 0.12, log=True),
        "alpha": trial.suggest_float("reg_alpha", 1e-3, 2.0, log=True),
        "lambda": trial.suggest_float("reg_lambda", 0.05, 10.0, log=True),
        "gamma": trial.suggest_float("gamma", 0.0, 2.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
        "subsample": trial.suggest_float("subsample", 0.65, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.65, 1.0),
        "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.65, 1.0),
        "max_bin": trial.suggest_categorical("max_bin", [64, 128, 256, 384, 512]),
    }

    if reg_objective == "reg:pseudohubererror":
        param["huber_slope"] = trial.suggest_float("huber_slope", 0.2, 5.0, log=True)

    if grow_policy == "lossguide":
        param["max_leaves"] = trial.suggest_int("max_leaves", 16, 256)
    else:
        param["max_depth"] = trial.suggest_int("max_depth", 2, 8)

    if booster == "dart":
        param["sample_type"] = trial.suggest_categorical("dart_sample_type", ["uniform", "weighted"])
        param["normalize_type"] = trial.suggest_categorical("dart_normalize_type", ["tree", "forest"])
        param["rate_drop"] = trial.suggest_float("rate_drop", 0.0, 0.35)
        param["skip_drop"] = trial.suggest_float("skip_drop", 0.0, 0.85)

    return param, n_estimators


def _rebuild_xgb_params(best: dict) -> tuple[dict, int]:
    n_estimators = int(best["n_estimators"])
    reg_objective = best["reg_objective"]
    booster = best["booster"]
    grow_policy = best["grow_policy"]

    param: dict = {
        "verbosity": 0,
        "objective": reg_objective,
        "tree_method": best["tree_method"],
        "device": os.getenv("XGBOOST_DEVICE", _BASE_EXPLICIT["device"]),
        "n_jobs": _BASE_EXPLICIT["n_jobs"],
        "booster": booster,
        "grow_policy": grow_policy,
        "eval_metric": best["eval_metric"],
        "eta": best["learning_rate"],
        "alpha": best["reg_alpha"],
        "lambda": best["reg_lambda"],
        "gamma": best["gamma"],
        "min_child_weight": int(best["min_child_weight"]),
        "subsample": best["subsample"],
        "colsample_bytree": best["colsample_bytree"],
        "colsample_bylevel": best["colsample_bylevel"],
        "max_bin": best["max_bin"],
    }
    if reg_objective == "reg:pseudohubererror":
        param["huber_slope"] = best["huber_slope"]
    if grow_policy == "lossguide":
        param["max_leaves"] = int(best["max_leaves"])
    else:
        param["max_depth"] = int(best["max_depth"])
    if booster == "dart":
        param["sample_type"] = best["dart_sample_type"]
        param["normalize_type"] = best["dart_normalize_type"]
        param["rate_drop"] = best["rate_drop"]
        param["skip_drop"] = best["skip_drop"]
    return param, n_estimators


def _optuna_validation_score(
    metric: str,
    y_va: np.ndarray,
    pred: np.ndarray,
    ts_va: np.ndarray | None,
) -> float:
    if metric == "rmse":
        return _rmse(y_va, pred)
    if metric == "spearman":
        return _pooled_spearman(y_va, pred)
    if metric == "cs_spearman":
        if ts_va is None:
            raise ValueError("cs_spearman requires valid timestamps")
        return _mean_cs_spearman(ts_va, y_va, pred)
    raise ValueError(f"Unknown OPTUNA_METRIC={metric!r} (use rmse, spearman, cs_spearman)")


def make_objective(
    train_x,
    train_y,
    valid_x,
    valid_y,
    *,
    early_stopping_rounds: int = 50,
    optuna_metric: str = "rmse",
):
    y_tr = train_y["vol_weighted_return"].to_numpy()
    y_va = valid_y["vol_weighted_return"].to_numpy()
    ts_va = valid_y["ts"].to_numpy() if optuna_metric == "cs_spearman" else None
    dtrain = xgb.DMatrix(train_x, label=y_tr, feature_names=list(train_x.columns))
    dvalid = xgb.DMatrix(valid_x, label=y_va, feature_names=list(valid_x.columns))

    def objective(trial: optuna.Trial) -> float:
        param, n_estimators = _suggest_params(trial)
        bst = xgb.train(
            param,
            dtrain,
            num_boost_round=n_estimators,
            evals=[(dvalid, "valid")],
            early_stopping_rounds=early_stopping_rounds,
            verbose_eval=False,
        )
        pred = bst.predict(dvalid, iteration_range=(0, bst.best_iteration + 1))
        return _optuna_validation_score(optuna_metric, y_va, pred, ts_va)

    return objective


def main() -> None:
    optuna_metric = os.getenv("OPTUNA_METRIC", "rmse").strip().lower()
    if optuna_metric not in ("rmse", "spearman", "cs_spearman"):
        raise SystemExit(f"Invalid OPTUNA_METRIC={optuna_metric!r}")

    direction = "minimize" if optuna_metric == "rmse" else "maximize"
    print("Base (notebook):", _BASE_EXPLICIT)
    print("Sklearn defaults (context):", _SKLEARN_DEFAULTS)
    print(f"OPTUNA_METRIC={optuna_metric} direction={direction}")
    print("XGB search includes: reg_objective, tree_method, booster, grow_policy, eval_metric, …")

    train_x, train_y, valid_x, valid_y, test_x, test_y = load_data()
    objective = make_objective(
        train_x, train_y, valid_x, valid_y, optuna_metric=optuna_metric
    )
    study = optuna.create_study(direction=direction, study_name="xgb_vol_wt_return")
    study.optimize(objective, n_trials=100, timeout=600, show_progress_bar=True)

    print("Number of finished trials:", len(study.trials))
    print(f"Best trial value ({optuna_metric}):", study.best_value)
    print("Best params:")
    for key, value in study.best_trial.params.items():
        print(f"  {key}: {value}")

    param, n_estimators = _rebuild_xgb_params(study.best_trial.params)
    x_fit = pd.concat([train_x, valid_x], axis=0)
    y_fit = pd.concat([train_y["vol_weighted_return"], valid_y["vol_weighted_return"]], axis=0)
    dfit = xgb.DMatrix(x_fit, label=y_fit, feature_names=list(train_x.columns))
    dtest = xgb.DMatrix(test_x, label=test_y["vol_weighted_return"], feature_names=list(test_x.columns))
    bst = xgb.train(param, dfit, num_boost_round=n_estimators)
    pred_test = bst.predict(dtest)
    y_te = test_y["vol_weighted_return"].to_numpy()
    ts_te = test_y["ts"].to_numpy()
    test_rmse = _rmse(y_te, pred_test)
    test_pool_sp = _pooled_spearman(y_te, pred_test)
    test_cs_sp = _mean_cs_spearman(ts_te, y_te, pred_test)
    print(f"Test RMSE (train+valid refit, full rounds): {test_rmse:.6f}")
    print(f"Test pooled Spearman(pred, y): {test_pool_sp:.6f}")
    print(f"Test mean cross-sectional Spearman by ts: {test_cs_sp:.6f}")


if __name__ == "__main__":
    main()
