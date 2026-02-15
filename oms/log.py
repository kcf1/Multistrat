"""
OMS logging (loguru).

Use: from oms.log import logger

Default level: INFO (DEBUG hidden). Set LOG_LEVEL=DEBUG to show debug logs.
"""

import os
import sys

from loguru import logger

_level = (os.environ.get("LOG_LEVEL") or "INFO").strip().upper()
logger.remove()
logger.add(sys.stderr, level=_level)

__all__ = ["logger"]
