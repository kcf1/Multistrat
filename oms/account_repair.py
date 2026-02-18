"""
OMS account repairs (task 12.2.8).

Dummy process for the loop: no account/balance repairs implemented yet.
Call run_all_account_repairs(pg_connect) from the OMS main loop alongside
account sync; returns 0 until real repairs (e.g. fix NULL/zero from payload) are added.
"""

from typing import Any, Callable, Union

from oms.log import logger


def run_all_account_repairs(pg_connect: Union[str, Callable[[], Any]]) -> int:
    """
    Run all account/balance repairs. Currently a no-op (nothing to repair).
    Call from OMS loop alongside sync_accounts_to_postgres.
    Returns total number of rows updated (0 for now).
    """
    logger.debug("run_all_account_repairs: no repairs configured (dummy)")
    return 0
