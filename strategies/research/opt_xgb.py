"""
Optuna search for XGBoost regression on vol_weighted_return.

Base model (double_sort.ipynb) uses sklearn XGBRegressor kwargs; native
`xgb.train` names differ:

  XGBRegressor (sklearn)     xgb.train param dict
  --------------------     ---------------------
  learning_rate              eta
  reg_alpha                  alpha
  reg_lambda                 lambda   (reserved word → string key "lambda")
  n_estimators               num_boost_round (train() argument, not in dict)

Fixed to match your notebook: objective=reg:squarederror, tree_method=hist.
Optional: device from env XGBOOST_DEVICE (default cuda) for CPU-only machines.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import optuna
import pandas as pd
import sklearn.metrics
import xgboost as xgb


def _rmse(y_true, y_pred) -> float:
    try:
        return float(sklearn.metrics.root_mean_squared_error(y_true, y_pred))
    except AttributeError:
        return float(sklearn.metrics.mean_squared_error(y_true, y_pred, squared=False))

# Allow `python strategies/research/opt_xgb.py` or cwd in research/
_research_dir = Path(__file__).resolve().parent
if str(_research_dir) not in sys.path:
    sys.path.insert(0, str(_research_dir))

from create_data import load_data  # noqa: E402

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
    """Search spaces centered on notebook base; extras follow sklearn defaults."""
    n_estimators = trial.suggest_int("n_estimators", 250, 900, step=50)
    param = {
        "verbosity": 0,
        "objective": "reg:squarederror",
        "tree_method": "hist",
        "device": os.getenv("XGBOOST_DEVICE", _BASE_EXPLICIT["device"]),
        "n_jobs": _BASE_EXPLICIT["n_jobs"],
        "eta": trial.suggest_float("learning_rate", 0.02, 0.12, log=True),
        "max_depth": trial.suggest_int("max_depth", 2, 8),
        "alpha": trial.suggest_float("reg_alpha", 1e-3, 2.0, log=True),
        "lambda": trial.suggest_float("reg_lambda", 0.05, 10.0, log=True),
        "gamma": trial.suggest_float("gamma", 0.0, 2.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
        "subsample": trial.suggest_float("subsample", 0.65, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.65, 1.0),
        "max_bin": trial.suggest_categorical("max_bin", [64, 128, 256, 384, 512]),
    }
    return param, n_estimators


def make_objective(
    train_x,
    train_y,
    valid_x,
    valid_y,
    *,
    early_stopping_rounds: int = 50,
):
    y_tr = train_y["vol_weighted_return"].to_numpy()
    y_va = valid_y["vol_weighted_return"].to_numpy()
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
        return _rmse(y_va, pred)

    return objective


def main() -> None:
    print("Base (notebook):", _BASE_EXPLICIT)
    print("Sklearn defaults (context):", _SKLEARN_DEFAULTS)
    train_x, train_y, valid_x, valid_y, test_x, test_y = load_data()
    objective = make_objective(train_x, train_y, valid_x, valid_y)
    study = optuna.create_study(direction="minimize", study_name="xgb_vol_wt_return")
    study.optimize(objective, n_trials=100, timeout=600, show_progress_bar=True)

    print("Number of finished trials:", len(study.trials))
    print("Best trial RMSE:", study.best_value)
    print("Best params:")
    for key, value in study.best_trial.params.items():
        print(f"  {key}: {value}")

    # Optional: refit best on train+valid and score test (not part of Optuna objective)
    best = study.best_trial.params.copy()
    max_bin = best.pop("max_bin")
    n_estimators = best.pop("n_estimators")
    param = {
        "verbosity": 0,
        "objective": "reg:squarederror",
        "tree_method": "hist",
        "device": os.getenv("XGBOOST_DEVICE", _BASE_EXPLICIT["device"]),
        "n_jobs": _BASE_EXPLICIT["n_jobs"],
        "eta": best["learning_rate"],
        "max_depth": best["max_depth"],
        "alpha": best["reg_alpha"],
        "lambda": best["reg_lambda"],
        "gamma": best["gamma"],
        "min_child_weight": best["min_child_weight"],
        "subsample": best["subsample"],
        "colsample_bytree": best["colsample_bytree"],
        "max_bin": max_bin,
    }
    x_fit = pd.concat([train_x, valid_x], axis=0)
    y_fit = pd.concat([train_y["vol_weighted_return"], valid_y["vol_weighted_return"]], axis=0)
    dfit = xgb.DMatrix(x_fit, label=y_fit, feature_names=list(train_x.columns))
    dtest = xgb.DMatrix(test_x, label=test_y["vol_weighted_return"], feature_names=list(test_x.columns))
    bst = xgb.train(param, dfit, num_boost_round=n_estimators)
    pred_test = bst.predict(dtest)
    test_rmse = _rmse(test_y["vol_weighted_return"].to_numpy(), pred_test)
    print(f"Test RMSE (full num_round, no early stopping on merge): {test_rmse:.6f}")


if __name__ == "__main__":
    main()
