"""
Optuna search for XGBoost regression on vol_weighted_return (Sharpe-style workflow:
predict signal → rank within date → long/short deciles).

Sklearn XGBRegressor kwargs vs native `xgb.train`:

  learning_rate → eta          reg_alpha → alpha          reg_lambda → "lambda"
  n_estimators → num_boost_round (train() argument)

Run configuration is set in-code below.

The reported "best" model is the best trial **within your search budget** (trial count
and/or timeout), not a global optimum. Optuna's default **TPESampler** uses past
trials to propose new points (not i.i.d. uniform draws). For stronger coverage,
raise ``OPTUNA_N_TRIALS`` / timeout, run multiple studies with different seeds, or
narrow spaces after an exploratory pass.
"""

from __future__ import annotations

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

# --- Run configuration (edit here) ---
# Optuna objective: rmse | spearman | cs_spearman | decile_spread | decile_sharpe
OPTUNA_METRIC = "cs_spearman"
OPTUNA_DECILE_FRAC = 0.1
# For metric decile_sharpe: multiply mean/std by sqrt(252) when True
OPTUNA_DECILE_ANNUALIZE = True
XGBOOST_DEVICE = "cuda"

# Search budget (best trial = argmax/min over this budget only)
OPTUNA_N_TRIALS = 500
OPTUNA_TIMEOUT_SEC = 3600
OPTUNA_SEED = 42  # TPESampler seed for reproducibility


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


def _decile_ls_spread_series(
    ts: np.ndarray,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    frac: float,
) -> np.ndarray:
    """
    Per-ts spread: mean(y | highest frac preds) - mean(y | lowest frac preds).
    Matches long top decile / short bottom decile on realized y.
    """
    df = pd.DataFrame({"ts": ts, "y": y_true, "p": y_pred})
    spreads: list[float] = []
    for _, g in df.groupby("ts", sort=False):
        n = len(g)
        k = max(1, int(round(n * frac)))
        if n < 2 * k:
            continue
        g = g.sort_values("p", kind="mergesort")
        bottom_mean = float(g.iloc[:k]["y"].mean())
        top_mean = float(g.iloc[-k:]["y"].mean())
        spreads.append(top_mean - bottom_mean)
    return np.asarray(spreads, dtype=float)


def _mean_decile_spread(
    ts: np.ndarray,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    frac: float,
) -> float:
    s = _decile_ls_spread_series(ts, y_true, y_pred, frac=frac)
    if len(s) == 0:
        return -1e18
    return float(np.mean(s))


def _sharpe_decile_spread(
    ts: np.ndarray,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    frac: float,
    annualize: bool = True,
) -> float:
    s = _decile_ls_spread_series(ts, y_true, y_pred, frac=frac)
    if len(s) < 3:
        return -1e18
    std = float(np.std(s, ddof=1))
    if std < 1e-12:
        return -1e18
    ir = float(np.mean(s) / std)
    if annualize:
        ir *= float(np.sqrt(252.0))
    return ir


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
        "device": XGBOOST_DEVICE,
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
        "device": XGBOOST_DEVICE,
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
    *,
    decile_frac: float,
    decile_annualize: bool,
) -> float:
    if metric == "rmse":
        return _rmse(y_va, pred)
    if metric == "spearman":
        return _pooled_spearman(y_va, pred)
    if metric == "cs_spearman":
        if ts_va is None:
            raise ValueError("cs_spearman requires valid timestamps")
        return _mean_cs_spearman(ts_va, y_va, pred)
    if metric == "decile_spread":
        if ts_va is None:
            raise ValueError("decile_spread requires valid timestamps")
        return _mean_decile_spread(ts_va, y_va, pred, frac=decile_frac)
    if metric == "decile_sharpe":
        if ts_va is None:
            raise ValueError("decile_sharpe requires valid timestamps")
        return _sharpe_decile_spread(
            ts_va, y_va, pred, frac=decile_frac, annualize=decile_annualize
        )
    raise ValueError(
        f"Unknown OPTUNA_METRIC={metric!r} "
        "(use rmse, spearman, cs_spearman, decile_spread, decile_sharpe)"
    )


_METRICS_NEED_TS = frozenset({"cs_spearman", "decile_spread", "decile_sharpe"})


def make_objective(
    train_x,
    train_y,
    valid_x,
    valid_y,
    *,
    early_stopping_rounds: int = 50,
    optuna_metric: str = OPTUNA_METRIC,
    decile_frac: float = OPTUNA_DECILE_FRAC,
    decile_annualize: bool = OPTUNA_DECILE_ANNUALIZE,
):
    y_tr = train_y["vol_weighted_return"].to_numpy()
    y_va = valid_y["vol_weighted_return"].to_numpy()
    ts_va = valid_y["ts"].to_numpy() if optuna_metric in _METRICS_NEED_TS else None
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
        return _optuna_validation_score(
            optuna_metric,
            y_va,
            pred,
            ts_va,
            decile_frac=decile_frac,
            decile_annualize=decile_annualize,
        )

    return objective


def main() -> None:
    optuna_metric = OPTUNA_METRIC.strip().lower()
    allowed = ("rmse", "spearman", "cs_spearman", "decile_spread", "decile_sharpe")
    if optuna_metric not in allowed:
        raise SystemExit(f"Invalid OPTUNA_METRIC={optuna_metric!r}; use one of {allowed}")

    decile_frac = float(OPTUNA_DECILE_FRAC)
    if not (0.0 < decile_frac <= 0.5):
        raise SystemExit("OPTUNA_DECILE_FRAC must be in (0, 0.5]")

    direction = "minimize" if optuna_metric == "rmse" else "maximize"
    print("Base (notebook):", _BASE_EXPLICIT)
    print("Sklearn defaults (context):", _SKLEARN_DEFAULTS)
    print(f"OPTUNA_METRIC={optuna_metric} direction={direction}")
    if optuna_metric in ("decile_spread", "decile_sharpe"):
        print(f"OPTUNA_DECILE_FRAC={decile_frac} OPTUNA_DECILE_ANNUALIZE={OPTUNA_DECILE_ANNUALIZE}")
    print("XGB search includes: reg_objective, tree_method, booster, grow_policy, eval_metric, …")
    print(
        f"Optuna: TPESampler(seed={OPTUNA_SEED}), "
        f"n_trials<={OPTUNA_N_TRIALS}, timeout<={OPTUNA_TIMEOUT_SEC}s"
    )

    train_x, train_y, valid_x, valid_y, test_x, test_y = load_data()
    objective = make_objective(
        train_x,
        train_y,
        valid_x,
        valid_y,
        optuna_metric=optuna_metric,
        decile_frac=decile_frac,
        decile_annualize=OPTUNA_DECILE_ANNUALIZE,
    )
    study = optuna.create_study(
        direction=direction,
        study_name="xgb_vol_wt_return",
        sampler=optuna.samplers.TPESampler(seed=OPTUNA_SEED),
    )
    study.optimize(
        objective,
        n_trials=OPTUNA_N_TRIALS,
        timeout=OPTUNA_TIMEOUT_SEC,
        show_progress_bar=True,
    )

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
    test_dec_spread = _mean_decile_spread(ts_te, y_te, pred_test, frac=decile_frac)
    test_dec_sharpe = _sharpe_decile_spread(
        ts_te,
        y_te,
        pred_test,
        frac=decile_frac,
        annualize=OPTUNA_DECILE_ANNUALIZE,
    )
    print(f"Test RMSE (train+valid refit, full rounds): {test_rmse:.6f}")
    print(f"Test pooled Spearman(pred, y): {test_pool_sp:.6f}")
    print(f"Test mean cross-sectional Spearman by ts: {test_cs_sp:.6f}")
    print(
        f"Test mean decile L/S spread by ts (frac={decile_frac}): {test_dec_spread:.6f}"
    )
    print(
        f"Test decile-spread Sharpe (annualized={OPTUNA_DECILE_ANNUALIZE}): {test_dec_sharpe:.6f}"
    )


if __name__ == "__main__":
    main()
